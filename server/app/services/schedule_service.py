"""Сервис машинограмм (Этап 4): компиляция → хранение → публикация → ack.

Связывает компилятор (core/schedule_compiler) с MQTT-клиентом и БД:
- compile_and_store(): компилирует машинограмму контроллера, присваивает
  монотонную версию, сохраняет JSON-файл (data/schedules/{box_id}/v<N>.json)
  и метаданные в controller_schedules (статус compiled);
- send_schedule(): публикует сохранённую (или свежескомпилированную)
  машинограмму в poliv/{box_id}/schedule (QoS 1), статус sent;
- on_hello / on_schedule_request(): автоответ контроллеру — вместо заглушки
  Этапа 3 реально компилируется и отправляется актуальная версия (ТЗ Этап 4,
  п.2: «программа стартует» начинается с доставки машинограммы);
- apply_policy (настройка schedule.apply_policy, ТЗ п.4):
    * next_run (по умолчанию, безопасная) — новая версия НЕ перекрывает
      активный прогон: машинограмма сохраняется и отправляется, эмулятор
      применит её от следующего запуска; при active-прогоне в БД публикация
      выполняется только если версии ещё нет в полёте;
    * immediate — новая версия применяется сразу (прерывание шага — на
      стороне контроллера);
- mark_ack(): по schedule_ack переводит sent → acknowledged/failed и
  обновляет controllers.schedule_version/hash;
- watering_runs: planned-записи создаются при доставке машинограммы
  (ack applied), фактические runs пишет event-обработчик (source=schedule).

Потокобезопасность: сервис вызывается из потока paho (hello/schedule_request/
schedule_ack/event) и из потоков FastAPI — все операции с БД идут через
собственное соединение с блокировкой _db_lock (короткие транзакции).
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from ..core.schedule_compiler.compiler import (
    CompiledSchedule,
    ScheduleConflict,
    compile_schedule,
    load_schedule_file,
    persist_schedule,
)
from ..infra.logging import get_logger

log = get_logger("poliv.schedules")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ScheduleService:
    """Компиляция + рассылка машинограмм; точка подключения к MQTT-клиенту."""

    def __init__(self, cfg, conn: sqlite3.Connection, mqtt=None,
                 db_path: Optional[str] = None):
        self.cfg = cfg
        self._api_conn = conn                  # соединение FastAPI (для чтения)
        self._db_path = db_path or str(cfg.db_path)
        self._mqtt = mqtt                      # MqttServerClient или None
        self._lock = threading.Lock()          # сериализация записи в БД
        self._conn: Optional[sqlite3.Connection] = None  # своё соединение потока

    # ------------------------------------------------------------------ infra
    def attach(self, mqtt_client) -> None:
        """Lifespan: привязать запущенный MQTT-клиент сервера."""
        self._mqtt = mqtt_client

    @property
    def mqtt(self):
        return self._mqtt

    @property
    def conn(self) -> sqlite3.Connection:
        """Своё соединение для фоновых потоков (как у MqttServerClient)."""
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode = WAL")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _wconn(self) -> sqlite3.Connection:
        """Соединение для ЗАПИСИ из любого потока + блокировка."""
        return self.conn

    def write_log(self, action: str, box_id: str, details: dict,
                  username: Optional[str] = None, source: str = "service") -> None:
        try:
            with self._lock, self.conn:
                self.conn.execute(
                    "INSERT INTO logs(ts, username, action, object_type, object_id,"
                    " details_json, source) VALUES (?, ?, ?, 'controller', ?, ?, ?)",
                    (utcnow_iso(), username, action, box_id,
                     json.dumps(details, ensure_ascii=False), source),
                )
        except Exception:
            log.exception("SCHEDULE: не удалось записать журнал (%s)", action)

    def _setting(self, key: str, default: Any) -> Any:
        row = self.conn.execute(
            "SELECT value_json FROM settings WHERE key=?", (key,)).fetchone()
        if row is None or row["value_json"] is None:
            return default
        try:
            return json.loads(row["value_json"])
        except (TypeError, ValueError):
            return default

    def apply_policy(self) -> str:
        policy = str(self._setting("schedule.apply_policy", "next_run"))
        return policy if policy in ("next_run", "immediate") else "next_run"

    # ------------------------------------------------------------- compile API
    def compile_and_store(self, controller_id: int, *, source: str = "manual",
                          created_by: Optional[str] = None,
                          days_ahead: Optional[int] = None,
                          programs: Optional[list[dict]] = None) -> dict:
        """Компилирует и СОХРАНЯЕТ новую версию машинограммы (статус compiled).

        Возбуждает ScheduleConflict при конфликте запусков (API → 422),
        ValueError если контроллер не найден.
        """
        with self._lock:
            ctrl = self.conn.execute(
                "SELECT * FROM controllers WHERE id=? AND deleted_at IS NULL",
                (controller_id,)).fetchone()
            if ctrl is None:
                raise ValueError("Контроллер не найден")
            kwargs: dict[str, Any] = {"source": source}
            if days_ahead:
                kwargs["days_ahead"] = int(days_ahead)
            if programs is not None:
                kwargs["programs"] = programs
            compiled = compile_schedule(self.conn, dict(ctrl), **kwargs)
            sched_id, path = persist_schedule(
                self.conn, compiled, data_dir=self.cfg.data_dir,
                status="compiled", created_by=created_by)
        log.info("SCHEDULE: %s v%s скомпилирована (id=%s, runs=%d, hash=%s…)",
                 compiled.box_id, compiled.payload["schedule_version"],
                 sched_id, compiled.runs_count, compiled.schedule_hash[:8])
        self.write_log("schedule.compiled", compiled.box_id,
                       {"schedule_id": sched_id,
                        "version": compiled.payload["schedule_version"],
                        "runs": compiled.runs_count,
                        "hash": compiled.schedule_hash,
                        "warnings": len(compiled.warnings)},
                       username=created_by)
        return {
            "id": sched_id,
            "box_id": compiled.box_id,
            "controller_id": compiled.controller_id,
            "schedule_version": compiled.payload["schedule_version"],
            "schedule_hash": compiled.schedule_hash,
            "status": "compiled",
            "payload_path": str(path),
            "runs_count": compiled.runs_count,
            "valid_from_date": compiled.valid_from.isoformat(),
            "valid_to_date": compiled.valid_to.isoformat(),
            "warnings": compiled.warnings,
            "skipped_runs": compiled.skipped_runs,
            "skipped_steps": compiled.skipped_steps,
            "skipped_notes": compiled.skipped_notes,
            "payload": compiled.payload,
        }

    def preview(self, controller_id: int, *,
                programs: Optional[list[dict]] = None) -> dict:
        """Компиляция БЕЗ сохранения (просмотр оператором, версия не тратится)."""
        with self._lock:
            ctrl = self.conn.execute(
                "SELECT * FROM controllers WHERE id=? AND deleted_at IS NULL",
                (controller_id,)).fetchone()
            if ctrl is None:
                raise ValueError("Контроллер не найден")
            kwargs: dict[str, Any] = {"source": "auto"}
            if programs is not None:
                kwargs["programs"] = programs
            compiled = compile_schedule(self.conn, dict(ctrl), **kwargs)
        return {"box_id": compiled.box_id, "status": "preview",
                "payload": compiled.payload,
                "warnings": compiled.warnings,
                "skipped_runs": compiled.skipped_runs,
                "skipped_steps": compiled.skipped_steps,
                "skipped_notes": compiled.skipped_notes}

    # ----------------------------------------------------------------- queries
    def list_for_controller(self, controller_id: int, limit: int = 20) -> list[dict]:
        rows = self.conn.execute(
            """SELECT cs.*, c.box_id FROM controller_schedules cs
               JOIN controllers c ON c.id = cs.controller_id
               WHERE cs.controller_id=? ORDER BY cs.schedule_version DESC
               LIMIT ?""", (controller_id, int(limit))).fetchall()
        return [self._row_view(r) for r in rows]

    def get(self, schedule_id: int) -> Optional[dict]:
        row = self.conn.execute(
            """SELECT cs.*, c.box_id FROM controller_schedules cs
               JOIN controllers c ON c.id = cs.controller_id
               WHERE cs.id=?""", (schedule_id,)).fetchone()
        if row is None:
            return None
        view = self._row_view(row)
        if row["payload_path"] and Path(row["payload_path"]).exists():
            view["payload"] = load_schedule_file(row["payload_path"])
        return view

    def current_for_controller(self, controller_id: int) -> Optional[dict]:
        """Последняя сохранённая версия (любого статуса >= compiled)."""
        row = self.conn.execute(
            """SELECT cs.*, c.box_id FROM controller_schedules cs
               JOIN controllers c ON c.id = cs.controller_id
               WHERE cs.controller_id=? AND cs.status != 'draft'
               ORDER BY cs.schedule_version DESC LIMIT 1""",
            (controller_id,)).fetchone()
        if row is None:
            return None
        view = self._row_view(row)
        if row["payload_path"] and Path(row["payload_path"]).exists():
            view["payload"] = load_schedule_file(row["payload_path"])
        return view

    def _row_view(self, row: sqlite3.Row) -> dict:
        d = dict(row)
        warn = {}
        try:
            warn = json.loads(d.get("warnings_json") or "{}")
        except (TypeError, ValueError):
            pass
        return {
            "id": d["id"],
            "controller_id": d["controller_id"],
            "box_id": d["box_id"],
            "schedule_version": d["schedule_version"],
            "schedule_hash": d["schedule_hash"],
            "status": d["status"],
            "source": d["source"],
            "valid_from_date": d["valid_from_date"],
            "valid_to_date": d["valid_to_date"],
            "runs_count": d["runs_count"],
            "payload_path": d.get("payload_path"),
            "created_by": d.get("created_by"),
            "created_at": d.get("created_at"),
            "sent_at": d.get("sent_at"),
            "acknowledged_at": d.get("acknowledged_at"),
            "error_message": d.get("error_message"),
            "warnings": warn.get("warnings", []),
            "skipped_runs": warn.get("skipped_runs", []),
            "skipped_steps": warn.get("skipped_steps", []),
            "skipped_notes": warn.get("skipped_notes", []),
        }

    # --------------------------------------------------------------- publish
    def send_schedule(self, schedule_id: int) -> dict:
        """Публикует сохранённую машинограмму в poliv/{box_id}/schedule (QoS 1).

        Политика apply_policy=next_run: если у контроллера есть активный
        прогон (watering_runs status=active), публикация новой версии
        ОТКЛАДЫВАется до завершения прогона — машинограмма остаётся compiled,
        в журнал пишется schedule.send_deferred (ТЗ Этап 4, п.4: безопасное
        применение — со следующего запуска).
        """
        view = self.get(schedule_id)
        if view is None:
            raise ValueError("Машинограмма не найдена")
        payload = view.get("payload")
        if not payload:
            raise ValueError("Файл машинограммы отсутствует на диске")
        box_id = view["box_id"]
        if self._mqtt is None:
            raise RuntimeError("MQTT недоступен")

        policy = self.apply_policy()
        with self._lock:
            active = self.conn.execute(
                """SELECT 1 FROM watering_runs
                   WHERE controller_id=? AND status='active' LIMIT 1""",
                (view["controller_id"],)).fetchone()
        if policy == "next_run" and active is not None:
            # Этап 4 final (P1-2): отложенная доставка — в БД (pending_schedule),
            # состояние переживает перезапуск сервера; хук on_event отправит
            # queued-версию при завершении активного прогона.
            log.info("SCHEDULE: %s v%s — публикация отложена (идёт прогон, "
                     "apply_policy=next_run)", box_id, view["schedule_version"])
            now = utcnow_iso()
            with self._lock, self.conn:
                self.conn.execute(
                    "UPDATE pending_schedule SET status='cancelled', updated_at=?"
                    " WHERE controller_id=? AND status='queued'",
                    (now, view["controller_id"]))
                self.conn.execute(
                    """INSERT INTO pending_schedule(
                           controller_id, schedule_id, status, reason,
                           created_at, updated_at)
                       VALUES (?,?, 'queued', ?, ?, ?)""",
                    (view["controller_id"], schedule_id,
                     "active_run_next_run_policy", now, now))
            self.write_log("schedule.send_deferred", box_id,
                           {"schedule_id": schedule_id,
                            "version": view["schedule_version"],
                            "reason": "active_run_next_run_policy"})
            return {**view, "status": view["status"], "sent": False,
                    "deferred": True,
                    "message": "Идёт прогон — новая версия применится после "
                               "его завершения (apply_policy=next_run)"}

        self._mqtt.publish(f"poliv/{box_id}/schedule", payload, qos=1)
        now = utcnow_iso()
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE controller_schedules SET status='sent', sent_at=? "
                "WHERE id=?", (now, schedule_id))
            # P1-2: явная отправка снимает queued-статус этой же версии
            self.conn.execute(
                "UPDATE pending_schedule SET status='sent', updated_at=? "
                "WHERE schedule_id=? AND status IN ('queued','failed')",
                (now, schedule_id))
        self.write_log("schedule.sent", box_id,
                       {"schedule_id": schedule_id,
                        "version": view["schedule_version"],
                        "hash": view["schedule_hash"],
                        "policy": policy})
        log.info("SCHEDULE: %s v%s опубликована (policy=%s)", box_id,
                 view["schedule_version"], policy)
        return {**view, "status": "sent", "sent": True, "deferred": False}

    def compile_and_send(self, controller_id: int, *, source: str = "manual",
                         created_by: Optional[str] = None) -> dict:
        """Операторский сценарий: скомпилировать новую версию и отправить."""
        stored = self.compile_and_store(controller_id, source=source,
                                        created_by=created_by)
        return self.send_schedule(stored["id"])

    # -------------------------------------------- события MQTT (фоны paho/api)
    def on_hello(self, box_id: str) -> None:
        """Контроллер представился — отправить ему актуальную машинограмму.

        Если сохранённой версии нет (или она устарела относительно программ) —
        компилируется заново (source=auto). Ошибки компиляции (конфликт) —
        warning, сервер не падает: контроллер продолжит работать по последней
        принятой версии.
        """
        try:
            ctrl = self.conn.execute(
                "SELECT * FROM controllers WHERE box_id=? AND deleted_at IS NULL",
                (box_id,)).fetchone()
            if ctrl is None:
                return
            cid = int(ctrl["id"])
            current = self.current_for_controller(cid)
            if current is None:
                stored = self.compile_and_store(cid, source="auto")
                self.send_schedule(stored["id"])
                return
            # Есть ли смысл перекомпилировать? Пока программы/зоны не менялись,
            # повторная компиляция дала бы тот же хеш — отправляем существующую.
            fresh = compile_schedule(self.conn, dict(ctrl), source="auto")
            if fresh.schedule_hash == current["schedule_hash"]:
                self.send_schedule(current["id"])
            else:
                sched_id, _ = persist_schedule(self.conn, fresh,
                                               data_dir=self.cfg.data_dir,
                                               status="compiled")
                self.send_schedule(sched_id)
        except ScheduleConflict as exc:
            log.warning("SCHEDULE: %s — конфликт расписания, машинограмма не "
                        "обновлена: %s", box_id, exc)
        except Exception:
            log.exception("SCHEDULE: не удалось подготовить машинограмму для %s",
                          box_id)

    def on_schedule_ack(self, box_id: str, p: dict) -> None:
        """schedule_ack: sent→acknowledged/failed + controllers.schedule_*.

        Этап 4 final (P1-5): валидация ack — не доверяем контроллеру:
        1. версия должна существовать в controller_schedules этого контроллера;
        2. status=applied — хеш сверяется с сохранённым (несовпадение → failed);
        3. status=rejected — запись → failed с причиной (schedule_rejections),
           controllers.schedule_version/hash НЕ меняются.
        """
        sv = p.get("schedule_version")
        status = p.get("status")
        if not isinstance(sv, int) or isinstance(sv, bool):
            return
        ok = status in ("applied", "ok")
        with self._lock:
            ctrl = self.conn.execute(
                "SELECT id FROM controllers WHERE box_id=?", (box_id,)).fetchone()
            row = self.conn.execute(
                """SELECT cs.id, cs.status, cs.schedule_hash FROM controller_schedules cs
                   WHERE cs.controller_id=? AND cs.schedule_version=?""",
                ((int(ctrl["id"]) if ctrl else -1), sv)).fetchone()
            # P1-5, шаг 1: ack по несуществующей версии — игнорируем полностью
            if ctrl is None or row is None:
                log.warning("SCHEDULE: ack %s v%s — версия не найдена, "
                            "игнорируется (P1-5)", box_id, sv)
                return
            hash_mismatch = False
            if ok and status == "applied":
                # P1-5, шаг 2: сверка хеша с сохранённым (не доверяем клиенту)
                ack_hash = p.get("schedule_hash")
                if ack_hash and ack_hash != row["schedule_hash"]:
                    log.warning("SCHEDULE: ack %s v%s applied, но хеш не "
                                "совпадает (%s… != %s…) — failed",
                                box_id, sv, str(ack_hash)[:8],
                                row["schedule_hash"][:8])
                    ok, hash_mismatch = False, True
            now = utcnow_iso()
            new_status = "acknowledged" if ok else "failed"
            self.conn.execute(
                "UPDATE controller_schedules SET status=?, acknowledged_at=?"
                " WHERE id=?", (new_status, now, row["id"]))
            if ok:
                self.conn.execute(
                    """UPDATE controllers SET schedule_version=?, schedule_hash=?,
                                              updated_at=?
                       WHERE box_id=?""",
                    (sv, row["schedule_hash"], now, box_id))
            elif status == "rejected" or hash_mismatch:
                # P1-5, шаг 3: rejected — история отказов; версия контроллера
                # остаётся прежней (controllers не трогаем)
                reason = p.get("reason") or p.get("message") or (
                    "hash_mismatch" if hash_mismatch else "rejected_by_controller")
                self.conn.execute(
                    """INSERT INTO schedule_rejections(controller_id, version,
                                                       reason, ts)
                       VALUES (?,?,?,?)""",
                    (int(ctrl["id"]), sv, reason, now))
            if ok:
                # Плановые прогоны из принятой машинограммы — в watering_runs
                self._record_planned_runs(box_id, sv, row["id"])
        self.write_log("schedule.ack", box_id,
                       {"version": sv, "status": status,
                        "result": new_status,
                        **({"reason": p.get("reason")} if p.get("reason") else {})})
        log.info("SCHEDULE: ack %s v%s -> %s", box_id, sv, new_status)

    def _record_planned_runs(self, box_id: str, version: int,
                             schedule_id: Optional[int]) -> None:
        """Planned-записи запусков принятой машинограммы (журнал плана)."""
        try:
            ctrl = self.conn.execute(
                "SELECT id FROM controllers WHERE box_id=?", (box_id,)).fetchone()
            cur = self.current_for_payload(version, ctrl["id"]) if ctrl else None
            if not cur:
                return
            from datetime import date as _date
            tz_off = int(cur.get("timezone_offset_min") or 0)
            today = _date.today()
            now = utcnow_iso()
            # NB: вызывается из on_schedule_ack уже под self._lock —
            # повторный Lock.acquire() мёртв (не reentrant) → зависание.
            # Транзакционное обёртывание оставляем без захвата блокировки.
            with self.conn:
                for run in cur.get("runs", []):
                    try:
                        rdate = datetime.strptime(run["date"], "%Y-%m-%d").date()
                    except (KeyError, ValueError):
                        continue
                    if rdate < today:
                        continue
                    minute = int(run.get("start_minute_local") or 0)
                    planned_ts = int(datetime(
                        rdate.year, rdate.month, rdate.day,
                        minute // 60, minute % 60,
                        tzinfo=timezone.utc).timestamp()) - tz_off * 60
                    zones = sorted({z for s in run.get("steps", [])
                                    for z in (s.get("active_zones") or [])})
                    self.conn.execute(
                        """INSERT OR IGNORE INTO watering_runs(
                               controller_id, box_id, run_id, program_id,
                               schedule_id, schedule_version, source, status,
                               planned_start_ts, zones_json, created_at, updated_at)
                           VALUES (?,?,?,?,?,?, 'schedule', 'planned', ?, ?, ?, ?)""",
                        (ctrl["id"], box_id, run.get("run_id"),
                         run.get("program_id"), schedule_id, version,
                         planned_ts, json.dumps(zones), now, now))
        except Exception:
            log.exception("SCHEDULE: не удалось записать план прогонов %s v%s",
                          box_id, version)

    def current_for_payload(self, version: int, controller_id: int) -> Optional[dict]:
        row = self.conn.execute(
            """SELECT payload_path FROM controller_schedules
               WHERE controller_id=? AND schedule_version=?""",
            (controller_id, version)).fetchone()
        if row and row["payload_path"] and Path(row["payload_path"]).exists():
            return load_schedule_file(row["payload_path"])
        return None

    def on_event(self, box_id: str, p: dict) -> None:
        """События полива: started/finished → watering_runs (план→факт).

        Источник событий — контроллер (эмулятор), поле source уже размечено
        ('schedule' | 'manual'). Записи журнала дадут проверяемое «программа
        стартует, зона открывается» без обращения к брокеру.
        """
        try:
            status = p.get("status")
            if status not in ("started", "finished", "stopped"):
                return
            # Этап 4 final (P1-1): раздельная валидация обязательных полей
            # схем события (ТЗ §3.11). started: event_uid, source, start_ts,
            # active_zones, buffered; finished/stopped: + end_ts, water_sec,
            # volume_liters, aborted. Эмулятор публикует обе схемы целиком.
            if not p.get("event_uid"):
                log.warning("SCHEDULE: event %s без event_uid — отброшен", box_id)
                return
            for field in ("start_ts", "active_zones", "buffered"):
                if field not in p:
                    log.warning("SCHEDULE: event %s (%s) без обязательного "
                                "поля %s — отброшен", box_id, status, field)
                    return
            if status in ("finished", "stopped"):
                for field in ("end_ts", "water_sec", "volume_liters", "aborted"):
                    if field not in p:
                        log.warning("SCHEDULE: event %s (%s) без обязательного "
                                    "поля %s — отброшен", box_id, status, field)
                        return
            src = p.get("source")
            if src not in ("schedule", "manual", "service", "test"):
                return
            # Этап 4 final (P1-1): schedule-прогон обязан нести run_id —
            # только так событие связывается с плановой записью watering_runs
            # (план→факт). manual-прогон run_id не имеет: идентификатор
            # прогона на стороне сервера — event_uid события started
            # (finished/stopped ссылается на него через started_event_uid).
            if src == "schedule" and not p.get("run_id"):
                log.warning("SCHEDULE: schedule-event %s без run_id — отброшен",
                            box_id)
                return
            ctrl = self.conn.execute(
                "SELECT id FROM controllers WHERE box_id=?", (box_id,)).fetchone()
            if ctrl is None:
                return
            cid = int(ctrl["id"])
            zones = p.get("active_zones") or []
            if src == "schedule":
                run_id = str(p["run_id"])
            elif status == "started":
                run_id = str(p.get("event_uid"))
            else:
                # finished/stopped manual-прогона: привязка к started-событию
                run_id = str(p.get("started_event_uid") or p.get("event_uid"))
            if not run_id:
                return
            now = utcnow_iso()
            # Этап 4 final (P2-8): старт прогона — все остальные planned-записи
            # этого контроллера устарели (новая машинограмма принята / план
            # пересобран): status='cancelled', reason='replaced_by_newer_schedule'.
            # Отмена выполняется ВНУТРИ основной транзакции ниже — иначе
            # незакрытая транзакция блокирует чтение conn в _flush_pending.
            cancel_stale = status == "started"
            with self._lock, self.conn:
                if cancel_stale:
                    self.conn.execute(
                        """UPDATE watering_runs
                           SET status='cancelled', reason_code=?, updated_at=?
                           WHERE controller_id=? AND status='planned'
                             AND run_id != ?""",
                        ("replaced_by_newer_schedule", now, cid, str(run_id)))
                if status == "started":
                    # §3.9/Этап 4: событие несёт зоны, которые реально
                    # поливались в прогоне (ev_zones), а не текущие
                    # active_zones — после soak-фазы реле закрыты и
                    # active_zones пусты; пустые зоны не перезаписывают
                    # известные из «started»
                    if zones:
                        self.conn.execute(
                            """UPDATE watering_runs SET zones_json=?
                               WHERE run_id=?""",
                            (json.dumps(zones), str(run_id)))
                    self.conn.execute(
                        """INSERT OR IGNORE INTO watering_runs(
                               controller_id, box_id, run_id, source, status,
                               actual_start_ts, water_sec, zones_json,
                               details_json, created_at, updated_at)
                           VALUES (?,?,?,?, 'active', ?, 0, ?, ?, ?, ?)""",
                        (cid, box_id, str(run_id), src,
                         int(p.get("start_ts") or datetime.now().timestamp()),
                         json.dumps(zones),
                         json.dumps({"event_uid": p.get("event_uid")},
                                    ensure_ascii=False), now, now))
                    # плановая запись того же запуска (если была) -> active
                    self.conn.execute(
                        """UPDATE watering_runs SET status='active',
                               actual_start_ts=?, updated_at=?
                           WHERE run_id=? AND status='planned'""",
                        (int(p.get("start_ts") or 0), now, str(run_id)))
                else:
                    end_ts = int(p.get("end_ts") or datetime.now().timestamp())
                    new_status = "completed" if status == "finished" else "aborted"
                    cur = self.conn.execute(
                        "SELECT id FROM watering_runs WHERE run_id=?",
                        (str(run_id),)).fetchone()
                    if cur is not None:
                        self.conn.execute(
                            """UPDATE watering_runs SET status=?, end_ts=?,
                                       water_sec=?, updated_at=?
                               WHERE id=?""",
                            (new_status, end_ts,
                             int(p.get("water_sec") or 0), now, cur["id"]))
                    else:
                        self.conn.execute(
                            """INSERT OR IGNORE INTO watering_runs(
                                   controller_id, box_id, run_id, source, status,
                                   actual_start_ts, end_ts, water_sec, zones_json,
                                   created_at, updated_at)
                               VALUES (?,?,?,?,?,?,?, ?,?,?,?)""",
                            (cid, box_id, str(run_id), src, new_status,
                             int(p.get("start_ts") or 0), end_ts,
                             int(p.get("water_sec") or 0), json.dumps(zones),
                             now, now))
            # Этап 4 final (P1-2): прогон завершён — освобождается очередь
            # отложенных машинограмм этого контроллера.
            if status in ("finished", "stopped"):
                self._flush_pending(box_id, cid)
        except Exception:
            log.exception("SCHEDULE: обработка event %s не удалась", box_id)

    def _flush_pending(self, box_id: str, controller_id: int) -> None:
        """P1-2: после завершения прогона отправить последнюю queued-версию.

        Очередь живёт в БД (pending_schedule) — переживает перезапуск сервера;
        повторные события (duplicates) безопасны: статус меняется атомарно.
        """
        try:
            with self._lock:
                row = self.conn.execute(
                    """SELECT ps.id, ps.schedule_id FROM pending_schedule ps
                       JOIN controller_schedules cs ON cs.id = ps.schedule_id
                       WHERE ps.controller_id=? AND ps.status='queued'
                       ORDER BY cs.schedule_version DESC LIMIT 1""",
                    (controller_id,)).fetchone()
                if row is None:
                    return
                # помечаем отправленной ДО публикации — защита от реентерабельной
                # отправки при повторном событии/сбое MQTT
                self.conn.execute(
                    "UPDATE pending_schedule SET status='sent', updated_at=? "
                    "WHERE id=?", (utcnow_iso(), row["id"]))
            log.info("SCHEDULE: %s — прогон завершён, отправляю отложенную "
                     "машинограмму (schedule_id=%s)", box_id, row["schedule_id"])
            self.send_schedule(row["schedule_id"])
            self.write_log("schedule.pending_flushed", box_id,
                           {"schedule_id": row["schedule_id"]})
        except RuntimeError as exc:      # MQTT недоступен → failed, ждём hello
            with self._lock, self.conn:
                self.conn.execute(
                    "UPDATE pending_schedule SET status='failed', updated_at=? "
                    "WHERE id=?", (utcnow_iso(), row["id"]))
            log.warning("SCHEDULE: отложенная доставка %s не удалась: %s",
                        box_id, exc)
        except Exception:
            log.exception("SCHEDULE: flush pending_schedule для %s упал", box_id)

    # ------------------------------------------------- автокомпиляция (изменения)
    def controllers_affected_by_program(self, program_id: int) -> list[int]:
        """Контроллеры, чьи машинограммы зависят от программы.

        Как компилятор (_load_programs): программа применяется ко всем
        контроллерам, у которых есть живые зоны из program_zones этой
        программы.
        """
        rows = self.conn.execute(
            """SELECT DISTINCT z.controller_id AS cid
               FROM program_zones pz
               JOIN zones z ON z.id = pz.zone_id
               WHERE pz.program_id=? AND z.deleted_at IS NULL""",
            (program_id,)).fetchall()
        return [int(r["cid"]) for r in rows]

    def record_compile_error(self, controller_id: int, reason: str,
                             error: dict) -> None:
        """P1-3: ошибка автокомпиляции — в БД (UI различает «настройки
        сохранены» и «расписание применено»)."""
        try:
            with self._lock, self.conn:
                self.conn.execute(
                    """INSERT INTO schedule_compile_errors(
                           controller_id, reason, error_json, ts)
                       VALUES (?,?,?,?)""",
                    (controller_id, reason,
                     json.dumps(error, ensure_ascii=False), utcnow_iso()))
        except Exception:
            log.exception("SCHEDULE: не удалось сохранить ошибку компиляции")

    def last_compile_error(self, controller_id: int) -> Optional[dict]:
        row = self.conn.execute(
            """SELECT * FROM schedule_compile_errors
               WHERE controller_id=? ORDER BY id DESC LIMIT 1""",
            (controller_id,)).fetchone()
        if row is None:
            return None
        try:
            err = json.loads(row["error_json"])
        except (TypeError, ValueError):
            err = {"message": row["error_json"]}
        return {"id": row["id"], "reason": row["reason"], "ts": row["ts"],
                "error": err}

    def clear_compile_error(self, controller_id: int) -> None:
        """Успешная компиляция снимает статус ошибки (для UI)."""
        with self._lock, self.conn:
            self.conn.execute(
                "DELETE FROM schedule_compile_errors WHERE controller_id=?",
                (controller_id,))

    def invalidate_and_recompile(self, controller_id: int, reason: str,
                                 actor: Optional[str] = None) -> None:
        """Вызывается после правок программ/зон/блокировок: компилирует новую
        версию (source=auto) и применяет политику: online-контроллер получает
        публикацию немедленно (с учётом next_run), офлайн — ждёт hello.

        P1-3: ошибка компиляции сохраняется в schedule_compile_errors
        (CRUD-запрос при этом завершается успешно — настройки применены).
        """
        try:
            stored = self.compile_and_store(controller_id, source="auto",
                                            created_by=actor)
        except ScheduleConflict as exc:
            log.warning("SCHEDULE: автокомпиляция %s (%s) — конфликт: %s",
                        controller_id, reason, exc)
            self.record_compile_error(controller_id, reason, {
                "code": "schedule_conflict",
                "message": str(exc),
                "conflicts": getattr(exc, "conflicts", [])})
            return
        except ValueError:
            return
        self.clear_compile_error(controller_id)
        ctrl = self.conn.execute(
            "SELECT connection_status FROM controllers WHERE id=?",
            (controller_id,)).fetchone()
        if ctrl is not None and ctrl["connection_status"] == "online" \
                and self._mqtt is not None:
            self.send_schedule(stored["id"])
        else:
            log.info("SCHEDULE: новая версия %s для контроллера %s (%s) — "
                     "будет отправлена при подключении", stored["id"],
                     controller_id, reason)

    def recompile_for_program(self, program_id: int, reason: str,
                              actor: Optional[str] = None) -> None:
        """P1-3: хук CRUD программ — перекомпиляция всех затронутых контроллеров."""
        for cid in self.controllers_affected_by_program(program_id):
            self.invalidate_and_recompile(cid, reason, actor)

    def recompile_for_zone(self, zone_id: int, reason: str,
                           actor: Optional[str] = None) -> None:
        """P1-3: хук CRUD зон — перекомпиляция контроллера-владельца зоны."""
        row = self.conn.execute(
            "SELECT controller_id FROM zones WHERE id=?", (zone_id,)).fetchone()
        if row is not None:
            self.invalidate_and_recompile(int(row["controller_id"]), reason, actor)


_instance: Optional[ScheduleService] = None
_instance_lock = threading.Lock()


def get_schedule_service() -> Optional[ScheduleService]:
    return _instance


def set_schedule_service(svc: Optional[ScheduleService]) -> None:
    global _instance
    with _instance_lock:
        _instance = svc
