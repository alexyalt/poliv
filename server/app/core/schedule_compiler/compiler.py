"""Компилятор машинограмм (Этап 4; контракт — Артефакт 0.5 §3).

Превращает программы (programs/program_zones) конкретного контроллера в
исполнительную машинограмму: список запусков (runs) с последовательными
шагами (steps).

Правила ТЗ Этапа 4, п.1:
- программа → список запусков (weekdays_mask или interval от anchor-даты);
- зоны → последовательность шагов по seq; parallel_group (ADR-12) объединяет
  зоны одного контроллера в один шаг (mode=parallel_group);
- длительности → base_duration × watering_adjustment_percent, либо
  duration_override_minutes программы;
- cycle&soak → отдельные фазы water/soak (§3.6, §3.9: soak_after_last —
  финальное впитывание без повторов);
- блокировки зон (zone_locks) и отключённые зоны → шаг исключается из
  машинограммы, в журнал — 'skipped' с reason_code (ТЗ Этапа 2 п.8: зона под
  блокировкой не включается в машинограмму в это время);
- дождевая задержка (adjustment.rain_delay_hours + weather_rules.latest_rain_ts)
  → пропуск запусков с reason_code rain_delay;
- конфликты пересечения запусков одной машины → ScheduleConflict (API 422).

Хеш (§3.10): sha256 канонического JSON содержимого (без generated_ts и
schedule_hash) — одинаковое содержимое даёт одинаковый хеш.
Версия: монотонная на контроллер (UNIQUE(controller_id, schedule_version)).
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from ...infra.logging import get_logger
from ..weather_rules import latest_rain_ts

log = get_logger("poliv.compiler")

PROTOCOL_VERSION = "1.0"
DEFAULT_DAYS_AHEAD = 7          # период действия машинограммы (valid_to_date)
MINUTE_IN_SEC = 60


class ScheduleConflict(RuntimeError):
    """Конфликт расписания (пересечение запусков) — API отдаёт 422."""

    def __init__(self, message: str, details: Optional[list] = None):
        super().__init__(message)
        self.details = details or []


@dataclass
class CompiledSchedule:
    box_id: str
    controller_id: int
    payload: dict[str, Any]                 # машинограмма (контракт §3.2)
    schedule_hash: str
    valid_from: date
    valid_to: date
    warnings: list[dict] = field(default_factory=list)
    skipped_runs: list[dict] = field(default_factory=list)   # rain_delay и т.п.
    skipped_steps: list[dict] = field(default_factory=list)  # zone_locked/disabled
    skipped_notes: list[dict] = field(default_factory=list)  # soak_in_group_ignored и т.п.

    @property
    def runs_count(self) -> int:
        return len(self.payload["runs"])


# --------------------------------------------------------------------- helpers
def _setting_value(conn: sqlite3.Connection, key: str, default: Any) -> Any:
    row = conn.execute("SELECT value_json FROM settings WHERE key=?",
                       (key,)).fetchone()
    if row is None or row["value_json"] is None:
        return default
    try:
        return json.loads(row["value_json"])
    except (TypeError, ValueError):
        return default


def local_timezone_offset_min(when: Optional[datetime] = None) -> int:
    """Смещение локального часового пояса сервера в минутах (поле §3.2)."""
    dt = (when or datetime.now().astimezone())
    off = dt.utcoffset()
    return int(off.total_seconds() // 60) if off else 0


def _canonical_hash(payload: dict) -> str:
    """sha256 канонического содержимого: без generated_ts/hash (§3.10).

    run_id каждого запуска тоже исключается из хеша: uuid4 генерируется на
    каждую компиляцию, а идентичность машинограммы определяется её
    содержательным смыслом (даты, времена, шаги). Стабильный хеш нужен для
    сверки «нужна ли перекомпиляция» (ScheduleService.on_hello) и для теста
    §3.10. Отправленный контроллеру payload сохраняет конкретные run_id —
    по ним сервер связывает события с плановыми записями watering_runs.
    """
    content = {k: v for k, v in payload.items()
               if k not in ("generated_ts", "schedule_hash")}
    runs = [{k: v for k, v in r.items() if k != "run_id"}
            for r in content.get("runs", [])]
    content = {**content, "runs": runs}
    blob = json.dumps(content, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _stable_run_id(box_id: str, program_id: Any, day_iso: str,
                   start_minute: Any) -> str:
    """Детерминированный run_id: uuid5 от пространства машинограммы бокса.

    Один и тот же запуск при повторной компиляции получает тот же run_id —
    это делает хеш стабильным (§3.10) и позволяет сопоставлять план/факт
    между версиями. Уникальность внутри payload гарантирована ключом
    (box_id, program_id, date, start_minute) — дубликаты программ не
    допускаются схемой.
    """
    ns = uuid.uuid5(uuid.NAMESPACE_URL, f"poliv://schedule/{box_id}")
    key = f"{program_id}|{day_iso}|{start_minute}"
    return str(uuid.uuid5(ns, key))


def _run_start_minute(start_time: str) -> int:
    hh, mm = (start_time or "06:00").split(":")[:2]
    return int(hh) * 60 + int(mm)


def _occurs_on(program: dict, d: date, anchor: date) -> bool:
    """Активен ли день d для программы (weekdays_mask пн..вс / interval)."""
    if program["schedule_type"] == "interval":
        interval = max(1, int(program["interval_days"] or 1))
        return (d - anchor).days % interval == 0
    mask = program["weekdays_mask"] or "0000000"
    return mask[d.weekday()] == "1"


def _step_duration_sec(zone: dict, pz: dict, adjustment_pct: float) -> tuple[int, int]:
    """Длительности (water_sec, soak_sec) с учётом регулировок и cycle&soak.

    Возвращает воду и впитывание ОДНОГО цикла; число циклов считает вызывающий.
    """
    override = pz.get("duration_override_minutes")
    base_min = override if override is not None else zone["base_duration_minutes"]
    water_min = max(0.0, float(base_min) * adjustment_pct / 100.0)
    soak_min = 0
    if zone["cycle_soak_enabled"]:
        soak_min = int(zone["soak_minutes"] or 0)
    return max(1, round(water_min * MINUTE_IN_SEC)), soak_min * MINUTE_IN_SEC


def _zone_locked(conn: sqlite3.Connection, zone_id: int, day_iso: str) -> bool:
    """Зона под активной блокировкой на день компиляции.

    unlock_at сравнивается с КОНЦОМ дня today (23:59:59), а не с полуночью:
    машинограмма покрывает сутки целиком — блокировка, снимаемая в течение
    этого дня, всё ещё активна на момент поливов этого дня (например, дождь
    или обслуживание утром). Блокировки, истёкшие до начала дня (unlock_at <=
    конец yesterday), считаются неактивными. Сравнение идёт по КЛЕНДАРНОЙ
    дате (substr(...,1,10)), т.к. day_iso — полная ISO-строка дня; строковое
    сравнение '2026-10-05' >= '2026-10-05T00:00:00' было бы ложно-отрицательным.
    """
    row = conn.execute(
        """SELECT 1 FROM zone_locks
           WHERE zone_id=? AND active=1
             AND substr(COALESCE(unlock_at,'9999-12-31'),1,10) >= substr(?,1,10)
           LIMIT 1""", (zone_id, day_iso)).fetchone()
    return row is not None


def _merge_skipped(skipped_steps: list[dict]) -> tuple[list[dict], list[dict]]:
    """Разделяет пропуски зон и заметки, убирая дубликаты.

    Одна и та же зона может быть пропущена в нескольких группах/чанках —
    в отчёт попадает один пропускатель на (program, zone, reason).
    Записи-заметки (skipped_note=True, напр. soak_in_group_ignored) уходят
    в отдельный список skipped_notes: у них нет zone_number, они носят
    справочный характер и не должны «съедать» реальные пропуски зон."""
    steps_out: list[dict] = []
    notes_out: list[dict] = []
    seen: set[tuple] = set()
    notes_seen: set[tuple] = set()
    for s in skipped_steps:
        if s.get("skipped_note"):
            nkey = (s.get("program_id"), tuple(s.get("zones") or ()),
                    s.get("reason_code"))
            if nkey in notes_seen:
                continue
            notes_seen.add(nkey)
            notes_out.append(s)
            continue
        key = (s.get("program_id"), s.get("zone_id"), s.get("reason_code"))
        if key in seen:
            continue
        seen.add(key)
        steps_out.append(s)
    return steps_out, notes_out


# ------------------------------------------------------------------ compile
def compile_schedule(conn: sqlite3.Connection, controller: dict, *,
                     days_ahead: int = DEFAULT_DAYS_AHEAD,
                     source: str = "auto",
                     today: Optional[date] = None,
                     timezone_offset_min: Optional[int] = None,
                     programs: Optional[list[dict]] = None) -> CompiledSchedule:
    """Компилирует машинограмму одного контроллера (см. модуль docstring)."""
    cid = int(controller["id"])
    box_id = controller["box_id"]
    today = today or date.today()
    tz_off = (local_timezone_offset_min() if timezone_offset_min is None
              else int(timezone_offset_min))
    valid_to = today + timedelta(days=max(1, int(days_ahead)))

    adjustment_pct = float(_setting_value(conn, "adjustment.watering_percent", 100))
    min_dur = int(_setting_value(conn, "schedule.min_duration_minutes", 1))
    max_dur = int(_setting_value(conn, "schedule.max_duration_minutes", 240))
    parallel_enabled = bool(controller.get("parallel_enabled") or 0)
    # «сейчас» для блокировок/погоды — от откомпилированного дня (today), а не
    # от часов системы: компиляция детерминирована и тестируема.
    now_iso = datetime(today.year, today.month, today.day).isoformat(
        timespec="seconds")

    # --- программы с зонами этого контроллера -------------------------------
    if programs is None:
        programs = _load_programs(conn, cid)

    warnings: list[dict] = []
    skipped_steps: list[dict] = []

    # candidate runs: (date, start_minute, program, steps)
    candidates: list[dict] = []
    for prog in programs:
        if not prog["enabled"]:
            continue
        zones_spec = prog["zones"]
        if not zones_spec:
            warnings.append({"code": "program_without_zones",
                             "program_id": prog["id"],
                             "message": f"Программа «{prog['name']}» без зон — "
                                        f"запуски не формируются"})
            continue
        anchor = today - timedelta(days=today.toordinal() % max(1,
                                     int(prog["interval_days"] or 1)))
        start_min = _run_start_minute(prog["start_time"])
        # Интервал якорится на дату создания программы (стабильный график:
        # запуск в день created_at + N*interval). Если программа создана
        # позже начала окна — первый запуск сдвигается в окно назад по
        # шагу интервала. Иначе каждая компиляция сдвигала бы дни запуска
        # на «сегодня» (ТЗ Этапа 4 п.1 требует стабильного графика).
        if prog["schedule_type"] == "interval":
            interval = max(1, int(prog["interval_days"] or 1))
            created_anchor: Optional[date] = None
            created = prog.get("created_at")
            if created:
                try:
                    created_anchor = datetime.fromisoformat(created).date()
                except ValueError:
                    created_anchor = None
            if created_anchor is not None and created_anchor <= today:
                anchor = created_anchor
            elif created_anchor is not None:  # future creation -> first run there
                anchor = created_anchor
            else:
                anchor = today - timedelta(days=today.toordinal() % interval)

        steps = _build_steps(conn, cid, zones_spec, adjustment_pct,
                             min_dur, max_dur, parallel_enabled,
                             now_iso, skipped_steps, prog["id"])
        if not steps:
            warnings.append({"code": "all_zones_skipped",
                             "program_id": prog["id"],
                             "message": f"Все зоны программы «{prog['name']}» "
                                        f"пропущены (блокировки/отключение)"})
            continue

        d = today
        while d <= valid_to:
            if _occurs_on(prog, d, anchor):
                candidates.append({
                    "date": d, "start_minute": start_min,
                    "program_id": prog["id"], "program_name": prog["name"],
                    "steps": steps,
                })
            d += timedelta(days=1)

    # --- дождевая задержка (rain_delay) --------------------------------------
    skipped_runs: list[dict] = []
    rain_delay_h = int(_setting_value(conn, "adjustment.rain_delay_hours", 0) or 0)
    if rain_delay_h > 0:
        rain_ts = latest_rain_ts(conn)
        if rain_ts is not None:
            delay_until = rain_ts + rain_delay_h * 3600
            today_start_ts = datetime(today.year, today.month, today.day,
                                      tzinfo=timezone(timedelta(minutes=tz_off))
                                      ).timestamp()
            kept = []
            for c in candidates:
                start_ts = _local_dt(c["date"], c["start_minute"], tz_off).timestamp()
                if start_ts < delay_until:
                    # «пропущен» — только реально ожидаемый запуск в окне;
                    # плановые даты до начала компиляции не сообщаем (шум).
                    if start_ts >= today_start_ts:
                        skipped_runs.append({
                            "date": c["date"].isoformat(),
                            "start_minute_local": c["start_minute"],
                            "program_id": c["program_id"],
                            "reason_code": "rain_delay",
                        })
                else:
                    kept.append(c)
            candidates = kept

    # --- конфликты пересечения запусков --------------------------------------
    _check_conflicts(candidates, box_id)

    # --- сборка payload -------------------------------------------------------
    runs_payload = []
    for c in sorted(candidates, key=lambda x: (x["date"], x["start_minute"])):
        run_id = _stable_run_id(box_id, c["program_id"],
                                c["date"].isoformat(), c["start_minute"])
        mode = ("sequential"
                if all(len(s["active_zones"]) == 1 for s in c["steps"])
                else "parallel_group")
        runs_payload.append({
            "run_id": run_id,
            "program_id": c["program_id"],
            "date": c["date"].isoformat(),
            "start_minute_local": c["start_minute"],
            "enabled": True,
            "mode": mode,
            "steps": c["steps"],
        })

    payload: dict[str, Any] = {
        "protocol_version": PROTOCOL_VERSION,
        "box_id": box_id,
        "schedule_version": 0,   # проставляет ScheduleService (монотонная версия)
        "schedule_hash": "",     # —"--
        "valid_from_date": today.isoformat(),
        "valid_to_date": valid_to.isoformat(),
        "timezone_offset_min": tz_off,
        "generated_ts": int(datetime.now().timestamp()),
        "source": source,
        "options": {
            "max_catchup_sec": 600,
            "max_start_delay_sec": 120,
            "parallel_enabled": parallel_enabled,
            "soak_display": "display_zones",
        },
        "runs": runs_payload,
    }
    payload["schedule_hash"] = _canonical_hash(payload)

    merged_steps, merged_notes = _merge_skipped(skipped_steps)
    return CompiledSchedule(box_id=box_id, controller_id=cid, payload=payload,
                            schedule_hash=payload["schedule_hash"],
                            valid_from=today, valid_to=valid_to,
                            warnings=warnings, skipped_runs=skipped_runs,
                            skipped_steps=merged_steps,
                            skipped_notes=merged_notes)


def _local_dt(d: date, minute: int, tz_off_min: int) -> datetime:
    """Локальные сутки контроллера -> unix-ts (через смещение §3.2)."""
    naive = datetime(d.year, d.month, d.day, minute // 60, minute % 60)
    aware = naive.replace(tzinfo=timezone(timedelta(minutes=tz_off_min)))
    return aware


def _load_programs(conn: sqlite3.Connection, controller_id: int) -> list[dict]:
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM programs WHERE enabled=1 ORDER BY id").fetchall()]
    for prog in rows:
        prog["zones"] = [dict(r) for r in conn.execute(
            """SELECT pz.seq, pz.duration_override_minutes, pz.parallel_group,
                      z.id AS zone_id, z.zone_number, z.name, z.enabled,
                      z.base_duration_minutes, z.cycle_soak_enabled,
                      z.soak_minutes, z.soak_after_last, z.expected_flow_lpm
               FROM program_zones pz
               JOIN zones z ON z.id = pz.zone_id
               WHERE pz.program_id=? AND z.controller_id=? AND z.deleted_at IS NULL
               ORDER BY pz.seq""", (prog["id"], controller_id)).fetchall()]
    return rows


def _build_steps(conn: sqlite3.Connection, controller_id: int,
                 zones_spec: list[dict], adjustment_pct: float,
                 min_dur: int, max_dur: int, parallel_enabled: bool,
                 now_iso: str, skipped_steps: list[dict],
                 program_id: int) -> list[dict]:
    """Шаги запуска: группы параллели + cycle&soak-фазы (§3.6–3.8).

    Группировка ADR-12: соседние строки program_zones с одинаковым
    parallel_group != NULL образуют одну параллельную группу; остальные зоны —
    последовательные шаги. Зоны чужих контроллеров здесь уже отфильтрованы.
    """
    groups: list[list[dict]] = []
    for spec in zones_spec:
        pg = spec.get("parallel_group")
        if pg and groups and groups[-1][0].get("parallel_group") == pg:
            groups[-1].append(spec)
        else:
            groups.append([spec])

    steps: list[dict] = []
    seq = 0
    for group in groups:
        live_specs = []
        for spec in group:
            zone = conn.execute(
                "SELECT * FROM zones WHERE id=?", (spec["zone_id"],)).fetchone()
            if zone is None or zone["deleted_at"] or not zone["enabled"]:
                skipped_steps.append({
                    "program_id": program_id, "zone_id": spec["zone_id"],
                    "zone_number": spec["zone_number"],
                    "reason_code": "disabled",
                })
                continue
            if _zone_locked(conn, spec["zone_id"], now_iso):
                skipped_steps.append({
                    "program_id": program_id, "zone_id": spec["zone_id"],
                    "zone_number": spec["zone_number"],
                    "reason_code": "zone_locked",
                })
                continue
            live_specs.append((spec, dict(zone)))
        if not live_specs:
            continue
        if len(live_specs) > 1 and not parallel_enabled:
            # ADR-12/§3.8: параллель запрещена настройкой контроллера —
            # разбиваем группу на последовательные шаги (не ошибка компиляции).
            log.info("Контроллер %s: параллельная группа разбита на "
                     "последовательные шаги (parallel_enabled=false), "
                     "программа %s", controller_id, program_id)
            split = True
        else:
            split = False

        chunk_list = [[item] for item in live_specs] if split else [live_specs]
        for chunk in chunk_list:
            step_zones = [z for _, z in chunk]
            numbers = sorted(z["zone_number"] for z in step_zones)
            expected_flow = None
            if all(z["expected_flow_lpm"] is not None for z in step_zones):
                expected_flow = round(sum(float(z["expected_flow_lpm"])
                                          for z in step_zones), 2)
            # вода/впитывание: для группы — суммарная вода считается по каждой
            # зоне отдельно только в одозонном шаге; в группе используется
            # наибольшая скорректированная длительность (зоны закрываются
            # одновременно в конце шага).
            waters = []
            soaks = []
            cycles = []
            for spec, zone in chunk:
                w, s = _step_duration_sec(zone, spec, adjustment_pct)
                w = min(max(w, min_dur * 60), max_dur * 60)
                waters.append(w)
                soaks.append(s)
                cyc = 1
                if zone["cycle_soak_enabled"] and s > 0:
                    cycle_sec = int((zone["cycle_minutes"] or 10) * MINUTE_IN_SEC)
                    cyc = max(1, math.ceil(w / max(60, cycle_sec)))
                cycles.append(cyc)

            if len(chunk) == 1 and cycles[0] > 1:
                # cycle & soak: N циклов water+soak (+ финальный soak, если задан)
                spec, zone = chunk[0]
                w = waters[0] // cycles[0] if cycles[0] else waters[0]
                # последний короче: делим остаток
                per = [w] * cycles[0]
                per[-1] = waters[0] - w * (cycles[0] - 1)
                for i, wdur in enumerate(per):
                    seq += 1
                    steps.append(_mk_step(seq, "water", numbers, wdur,
                                          expected_flow, parallel=False))
                    last = (i == cycles[0] - 1)
                    soak_dur = soaks[0]
                    if last and not zone["soak_after_last"]:
                        soak_dur = 0
                    if soak_dur > 0:
                        seq += 1
                        steps.append(_mk_step(seq, "soak", numbers, soak_dur,
                                              None, parallel=False))
            else:
                total_water = max(waters) if len(chunk) > 1 else waters[0]
                seq += 1
                steps.append(_mk_step(seq, "water", numbers, total_water,
                                      expected_flow,
                                      parallel=len(chunk) > 1))
                # в группах cycle&soak не применяется (упрощение Этапа 4 —
                # фиксируется в skipped_notes как заметка, НЕ пропуск зоны;
                # поле zone_number у заметок отсутствует, поэтому они не
                # попадают в «причины пропусков зон» на уровне API/тестов);
                # одиночные шаги с soak_after_last обрабатываются ниже
                if len(chunk) > 1 and any(soaks):
                    skipped_steps.append({"skipped_note": True,
                                          "program_id": program_id,
                                          "zones": numbers,
                                          "reason_code":
                                              "soak_in_group_ignored"})
                final_soak = soaks[0] if len(chunk) == 1 else 0
                if len(chunk) == 1 and final_soak > 0 and cycles[0] == 1 \
                        and chunk[0][1]["soak_after_last"]:
                    seq += 1
                    steps.append(_mk_step(seq, "soak", numbers, final_soak,
                                          None, parallel=False))
    return steps


def _mk_step(seq: int, phase: str, zone_numbers: list[int], duration_sec: int,
             expected_flow: Optional[float], parallel: bool) -> dict:
    step: dict[str, Any] = {
        "seq": seq,
        "duration_sec": int(duration_sec),
        "phase": phase,
        "active_zones": list(zone_numbers) if phase == "water" else [],
    }
    if phase == "soak":
        # §3.9: реле выключены, активные зоны пусты, отображаемые — те же
        step["display_zones"] = list(zone_numbers)
    if expected_flow is not None and phase == "water":
        step["expected_flow_lpm"] = expected_flow
    if parallel:
        step["parallel_allowed"] = True
    return step


def _check_conflicts(candidates: list[dict], box_id: str) -> None:
    """Пересечение запусков во времени — ошибка компиляции (§3.7 п.1: шаги
    исполняются последовательно, два одновременных запуска невозможны)."""
    ordered = sorted(candidates, key=lambda c: (c["date"], c["start_minute"]))
    for a, b in zip(ordered, ordered[1:]):
        if a["date"] != b["date"]:
            continue
        a_end = a["start_minute"] + sum(s["duration_sec"] for s in a["steps"]) // 60
        if b["start_minute"] < a_end:
            raise ScheduleConflict(
                f"Контроллер {box_id}: запуски программ {a['program_id']} и "
                f"{b['program_id']} пересекаются {a['date']} "
                f"(старт {b['start_minute']} мин < окончание {a_end} мин)",
                details=[{"date": a["date"].isoformat(),
                          "program_a": a["program_id"],
                          "program_b": b["program_id"],
                          "start_a": a["start_minute"],
                          "end_a": a_end, "start_b": b["start_minute"]}])


# ---------------------------------------------------------------- persistence
def persist_schedule(conn: sqlite3.Connection, compiled: CompiledSchedule,
                    *, data_dir: Path, status: str,
                    created_by: Optional[str] = None,
                    next_version: Optional[int] = None) -> tuple[int, Path]:
    """Записывает метаданные в controller_schedules + JSON в файл.

    Возвращает (schedule_id, путь к файлу). Содержимое машинограммы хранится
    ФАЙЛОМ (data/schedules/{box_id}/v<N>.json), в БД — только метаданные.
    next_version: явная версия (для preview без записи версий); по умолчанию —
    максимум существующих версий контроллера + 1.
    """
    cid = compiled.controller_id
    if next_version is None:
        row = conn.execute(
            "SELECT MAX(schedule_version) FROM controller_schedules "
            "WHERE controller_id=?", (cid,)).fetchone()
        next_version = (row[0] or 0) + 1
    version = int(next_version)
    compiled.payload["schedule_version"] = version
    compiled.payload["schedule_hash"] = compiled.schedule_hash

    sched_dir = Path(data_dir) / "schedules" / compiled.box_id
    sched_dir.mkdir(parents=True, exist_ok=True)
    path = sched_dir / f"v{version}.json"
    path.write_text(json.dumps(compiled.payload, ensure_ascii=False, indent=2),
                    encoding="utf-8")

    now = datetime.now().isoformat(timespec="seconds")
    cur = conn.execute(
        """INSERT INTO controller_schedules(
               controller_id, schedule_version, valid_from_date, valid_to_date,
               timezone_offset_min, source, status, schedule_hash, payload_path,
               runs_count, warnings_json, errors_json, created_by, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (cid, version, compiled.valid_from.isoformat(),
         compiled.valid_to.isoformat(),
         compiled.payload["timezone_offset_min"], compiled.payload["source"],
         status, compiled.schedule_hash, str(path), compiled.runs_count,
         json.dumps({"warnings": compiled.warnings,
                     "skipped_runs": compiled.skipped_runs,
                     "skipped_steps": compiled.skipped_steps,
                     "skipped_notes": compiled.skipped_notes},
                    ensure_ascii=False),
         None, created_by, now),
    )
    return int(cur.lastrowid), path


def load_schedule_file(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))
