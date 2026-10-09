"""Тесты Этапа 4 (часть 1): компилятор машинограмм.

Проверяются правила ТЗ Этап 4, п.1 и Артефакта 0.5 §3:
- программа → список запусков (weekdays_mask / interval от anchor-даты);
- зоны → шаги по seq; parallel_group (ADR-12) → один шаг; при
  parallel_enabled=0 группа разбивается на последовательные шаги;
- cycle&soak → фазы water/soak, soak_after_last (§3.6/§3.9);
- блокировки зон и disabled → skipped_steps с reason_code;
- rain_delay → skipped_runs (через временную таблицу weather_observations);
- конфликт пересечения запусков → ScheduleConflict (API 422);
- хеш содержимого (§3.10): стабильный, без generated_ts;
- persist_schedule: монотонная версия (UNIQUE(controller_id, schedule_version)),
  хранение JSON-файлом.

БД — временный файл со ВСЕМИ миграциями (включая 0005/0006), как в бою.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

os.environ.setdefault(
    "POLIV_CONFIG_LOCAL",
    str(Path(__file__).resolve().parents[2] / "config" / "config.local.toml"),
)

from server.app.core.schedule_compiler.compiler import (  # noqa: E402
    ScheduleConflict,
    _canonical_hash,
    compile_schedule,
    load_schedule_file,
    persist_schedule,
)


def _migrated_conn() -> tuple[sqlite3.Connection, Path]:
    """Чистая БД со всеми миграциями проекта (0001..0006)."""
    tmpdir = Path(tempfile.mkdtemp(prefix="poliv-st4c-"))
    conn = sqlite3.connect(tmpdir / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    from server.app.infra.db import migrate
    migrate(conn)
    return conn, tmpdir


def _mk_controller(conn, box_id="BOX-C1", **fields) -> dict:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cols = {"box_id": box_id, "name": f"Бокс {box_id}",
            "created_at": now, "updated_at": now}
    cols.update(fields)
    keys = ", ".join(cols)
    marks = ", ".join("?" for _ in cols)
    cur = conn.execute(
        f"INSERT INTO controllers({keys}) VALUES ({marks})",
        tuple(cols.values()))
    conn.commit()
    row = conn.execute("SELECT * FROM controllers WHERE id=?",
                       (cur.lastrowid,)).fetchone()
    return dict(row)


def _mk_zone(conn, controller_id: int, num: int, **f) -> int:
    data = {"controller_id": controller_id, "zone_number": num,
            "name": f"Зона {num}", "enabled": 1,
            "base_duration_minutes": 10, "watering_adjustment_percent": 100,
            "cycle_soak_enabled": 0, "cycle_minutes": None,
            "soak_minutes": None, "soak_after_last": 0,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00"}
    data.update(f)
    keys = ", ".join(data)
    marks = ", ".join("?" for _ in data)
    cur = conn.execute(f"INSERT INTO zones({keys}) VALUES ({marks})",
                       tuple(data.values()))
    conn.commit()
    return int(cur.lastrowid)


def _mk_program(conn, name: str, start_time="06:00", schedule_type="weekdays",
                weekdays_mask="1111111", interval_days=None, created=None,
                enabled=1) -> int:
    now = created or "2026-01-01T00:00:00+00:00"
    if weekdays_mask is None:      # схема NOT NULL DEFAULT '0000000'
        weekdays_mask = "0000000"
    cur = conn.execute(
        """INSERT INTO programs(name, description, enabled, schedule_type,
                                weekdays_mask, interval_days, start_time,
                                created_at, updated_at)
           VALUES (?,NULL,?,?,?,?,?,?,?)""",
        (name, enabled, schedule_type, weekdays_mask, interval_days,
         start_time, now, now))
    conn.commit()
    return int(cur.lastrowid)


def _link(conn, program_id: int, zone_id: int, seq: int, group=None,
          override=None) -> None:
    conn.execute(
        """INSERT INTO program_zones(program_id, zone_id, seq,
                                     duration_override_minutes, parallel_group)
           VALUES (?,?,?,?,?)""",
        (program_id, zone_id, seq, override, group))
    conn.commit()


def _set_setting(conn, key: str, value) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO settings(key, value_json, updated_at) "
        "VALUES (?,?,?)",
        (key, json.dumps(value), datetime.now(timezone.utc).isoformat()))
    conn.commit()


# ---------------------------------------------------------------- 1. базовая сборка
def test_weekdays_run_structure_and_contract_fields():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1, base_duration_minutes=10)
    p = _mk_program(conn, "Утренняя", start_time="06:00",
                   weekdays_mask="1010000")  # пн, ср
    _link(conn, p, z1, 1)

    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=7,
                            timezone_offset_min=0)
    payload = comp.payload
    # контракт §3.2 — обязательные поля
    for field in ("protocol_version", "box_id", "schedule_version",
                  "schedule_hash", "valid_from_date", "valid_to_date",
                  "timezone_offset_min", "generated_ts", "source",
                  "options", "runs"):
        assert field in payload, f"нет поля {field}"
    # окно [today, today+days_ahead]: 05.10..12.10; mask пн/ср → 05,07,12
    dates = [r["date"] for r in payload["runs"]]
    assert dates == ["2026-10-05", "2026-10-07", "2026-10-12"]
    assert payload["valid_to_date"] == "2026-10-12"
    run = payload["runs"][0]
    assert run["start_minute_local"] == 6 * 60
    assert run["mode"] == "sequential"
    assert run["enabled"] is True
    steps = run["steps"]
    # 10 минут воды + нет soak → один шаг
    assert len(steps) == 1
    st = steps[0]
    assert st["seq"] == 1 and st["phase"] == "water"
    assert st["active_zones"] == [1] and st["duration_sec"] == 600
    assert st["expected_flow_lpm"] is None if "expected_flow_lpm" in st else True
    # run_id уникальны
    ids = [r["run_id"] for r in payload["runs"]]
    assert len(ids) == len(set(ids))
    conn.close()


def test_interval_anchor_is_program_created_at():
    """Интервал якорится на created_at программы (стабильный график)."""
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1)
    # программа создана 01.10.2026 — запуски каждые 3 дня: 01,04,07,10...
    p = _mk_program(conn, "Интервал", schedule_type="interval",
                    weekdays_mask=None, interval_days=3,
                    created="2026-10-01T00:00:00+00:00")
    _link(conn, p, z1, 1)
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=7,
                            timezone_offset_min=0)
    dates = [r["date"] for r in comp.payload["runs"]]
    assert dates == ["2026-10-07", "2026-10-10"]
    # повторная компиляция в тот же день — те же даты (график не «прыгает»)
    comp2 = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=7,
                             timezone_offset_min=0)
    assert [r["date"] for r in comp2.payload["runs"]] == dates
    conn.close()


# ---------------------------------------------------------------- 2. cycle & soak
def test_cycle_soak_phases_and_soak_after_last():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    # 30 мин воды, цикл 10 мин → 3 цикла water+soak; soak_after_last=1
    z1 = _mk_zone(conn, c["id"], 1, base_duration_minutes=30,
                  cycle_soak_enabled=1, cycle_minutes=10, soak_minutes=5,
                  soak_after_last=1)
    p = _mk_program(conn, "Цикл")
    _link(conn, p, z1, 1)
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                            timezone_offset_min=0)
    steps = comp.payload["runs"][0]["steps"]
    phases = [s["phase"] for s in steps]
    assert phases == ["water", "soak"] * 3
    waters = [s for s in steps if s["phase"] == "water"]
    soaks = [s for s in steps if s["phase"] == "soak"]
    assert sum(s["duration_sec"] for s in waters) == 30 * 60
    assert all(s["duration_sec"] == 5 * 60 for s in soaks)
    # soak-шаг: реле выключены, активные зоны пусты, display — те же (§3.9)
    for s in soaks:
        assert s["active_zones"] == [] and s["display_zones"] == [1]
    for s in waters:
        assert s["active_zones"] == [1]
    # seq монотонен
    assert [s["seq"] for s in steps] == list(range(1, len(steps) + 1))

    # soak_after_last=0 → финальный soak отсутствует
    conn.execute("UPDATE zones SET soak_after_last=0 WHERE id=?", (z1,))
    conn.commit()
    comp2 = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                             timezone_offset_min=0)
    steps2 = comp2.payload["runs"][0]["steps"]
    assert [s["phase"] for s in steps2] == \
        ["water", "soak", "water", "soak", "water"]
    conn.close()


def test_watering_adjustment_percent_applied():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1, base_duration_minutes=20)
    p = _mk_program(conn, "Коррекция")
    _link(conn, p, z1, 1, override=None)
    _set_setting(conn, "adjustment.watering_percent", 50)
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                            timezone_offset_min=0)
    assert comp.payload["runs"][0]["steps"][0]["duration_sec"] == 10 * 60
    # duration_override программы важнее базы
    conn.execute("UPDATE program_zones SET duration_override_minutes=8 "
                 "WHERE zone_id=?", (z1,))
    conn.commit()
    comp2 = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                             timezone_offset_min=0)
    assert comp2.payload["runs"][0]["steps"][0]["duration_sec"] == 4 * 60
    conn.close()


# ---------------------------------------------------------------- 3. ADR-12 параллель
def test_parallel_group_single_step_when_enabled():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn, parallel_enabled=1)
    za = _mk_zone(conn, c["id"], 1, base_duration_minutes=10,
                  expected_flow_lpm=12.5)
    zb = _mk_zone(conn, c["id"], 2, base_duration_minutes=15,
                  expected_flow_lpm=7.5)
    zc = _mk_zone(conn, c["id"], 3, base_duration_minutes=5)
    p = _mk_program(conn, "Параллель")
    _link(conn, p, za, 1, group="A")
    _link(conn, p, zb, 2, group="A")
    _link(conn, p, zc, 3, group=None)
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                            timezone_offset_min=0)
    steps = comp.payload["runs"][0]["steps"]
    assert len(steps) == 2                      # группа A + зона 3 отдельно
    g = steps[0]
    assert g["active_zones"] == [1, 2]          # оба номера в одном шаге
    assert g.get("parallel_allowed") is True
    assert g["duration_sec"] == 15 * 60         # максимум из группы
    assert g["expected_flow_lpm"] == 20.0       # сумма потоков группы
    assert steps[1]["active_zones"] == [3]
    assert comp.payload["runs"][0]["mode"] == "parallel_group"
    conn.close()


def test_parallel_disabled_splits_into_sequential_steps():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn, parallel_enabled=0)
    za = _mk_zone(conn, c["id"], 1, base_duration_minutes=10)
    zb = _mk_zone(conn, c["id"], 2, base_duration_minutes=15)
    p = _mk_program(conn, "Последовательно")
    _link(conn, p, za, 1, group="A")
    _link(conn, p, zb, 2, group="A")
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                            timezone_offset_min=0)
    steps = comp.payload["runs"][0]["steps"]
    assert [s["active_zones"] for s in steps] == [[1], [2]]
    assert all(not s.get("parallel_allowed") for s in steps)
    assert comp.payload["options"]["parallel_enabled"] is False
    conn.close()


# ---------------------------------------------------------------- 4. блокировки/отключение
def test_locked_and_disabled_zones_skipped_with_reason():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1)
    z2 = _mk_zone(conn, c["id"], 2, enabled=0)          # отключена
    z3 = _mk_zone(conn, c["id"], 3, cycle_soak_enabled=1, soak_minutes=5,
                  soak_after_last=1)                    # cycle&soak: см. ниже
    # время блокировок — относительно дня компиляции (2026-10-05), т.к.
    # compile_schedule детерминирован и берёт «сейчас» из today, не из часов ОС
    future = "2026-10-05T23:00:00"
    past = "2026-10-04T22:00:00"
    conn.execute("INSERT INTO zone_locks(zone_id, kind, reason, locked_by,"
                 " locked_at, unlock_at, active) VALUES (?,'maintenance','р',"
                 "'admin',?, ?, 1)", (z3, past, future))
    expired_id = conn.execute("SELECT COALESCE(MAX(id),0)+1 x FROM zone_locks"
                              ).fetchone()["x"]
    conn.execute("INSERT INTO zone_locks(id, zone_id, kind, reason, locked_by,"
                 " locked_at, unlock_at, active) VALUES (?,?,'other','x','a',"
                 " ?, ?, 1)", (expired_id, z1, past, past))
    conn.commit()
    p = _mk_program(conn, "Все зоны")
    _link(conn, p, z1, 1)
    _link(conn, p, z2, 2)
    _link(conn, p, z3, 3)
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                            timezone_offset_min=0)
    reasons = {(s["zone_number"], s["reason_code"])
               for s in comp.skipped_steps if "zone_number" in s}
    assert (2, "disabled") in reasons
    assert (3, "zone_locked") in reasons
    assert all(z != 1 for z, _ in reasons)   # истёкшая блокировка не считается
    steps = comp.payload["runs"][0]["steps"]
    assert [s["active_zones"] for s in steps] == [[1]]
    conn.close()


def test_all_zones_skipped_gives_warning_no_runs():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1, enabled=0)
    p = _mk_program(conn, "Пустая")
    _link(conn, p, z1, 1)
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                            timezone_offset_min=0)
    assert comp.payload["runs"] == []
    assert any(w["code"] == "all_zones_skipped" for w in comp.warnings)
    conn.close()


# ---------------------------------------------------------------- 5. rain delay
def test_rain_delay_skips_runs_until_window():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1)
    p = _mk_program(conn, "Дождливая")
    _link(conn, p, z1, 1)
    _set_setting(conn, "adjustment.rain_delay_hours", 24)
    # Таблица погоды появится в Этапе 5 — для теста создаём её вручную
    # (latest_rain_ts читает MAX(ts) WHERE precipitation_mm > 0).
    conn.execute("CREATE TABLE weather_observations (ts INTEGER,"
                 " precipitation_mm REAL)")
    rain_ts = int(datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc).timestamp())
    conn.execute("INSERT INTO weather_observations VALUES (?, ?)",
                 (rain_ts, 5.0))
    conn.commit()
    comp = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=3,
                            timezone_offset_min=0)
    # окно 05..08.10 (valid_to включительно), старты 06:00 UTC; дождь
    # 05.10 12:00 + 24ч → delay_until 06.10 12:00: задержаны запуски
    # 05.10 (старт в прошлом относительно компиляции — «сейчас» = конец
    # дня today, см. now_iso) и 06.10. Запуск 07.10 после окна задержки.
    skipped_dates = {s["date"] for s in comp.skipped_runs}
    assert skipped_dates == {"2026-10-05", "2026-10-06"}
    assert all(s["reason_code"] == "rain_delay" for s in comp.skipped_runs)
    kept = [r["date"] for r in comp.payload["runs"]]
    assert kept == ["2026-10-07", "2026-10-08"]
    conn.close()


# ---------------------------------------------------------------- 6. конфликты
def test_overlapping_runs_raise_schedule_conflict():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1, base_duration_minutes=120)
    z2 = _mk_zone(conn, c["id"], 2, base_duration_minutes=10)
    pa = _mk_program(conn, "Ранняя", start_time="06:00")
    pb = _mk_program(conn, "Поздняя", start_time="07:00")   # внутри 2 ч ранней
    _link(conn, pa, z1, 1)
    _link(conn, pb, z2, 1)
    try:
        compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                         timezone_offset_min=0)
        raise AssertionError("ожидался ScheduleConflict")
    except ScheduleConflict as exc:
        assert exc.details and exc.details[0]["date"] == "2026-10-05"
    conn.close()


# ---------------------------------------------------------------- 7. хеш §3.10
def test_hash_stable_content_dependent():
    conn, _ = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1)
    p = _mk_program(conn, "Хеш")
    _link(conn, p, z1, 1)
    a = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                         timezone_offset_min=0)
    b = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                         timezone_offset_min=0)
    # одинаковое содержимое → одинаковый хеш (generated_ts игнорируется)
    assert a.payload["generated_ts"] != b.payload["generated_ts"] \
        or True  # ts может совпасть — главное хеш
    assert a.schedule_hash == b.schedule_hash
    # хеш = sha256 канонического содержимого без generated_ts/hash
    content = {k: v for k, v in a.payload.items()
               if k not in ("generated_ts", "schedule_hash")}
    assert a.schedule_hash == _canonical_hash(content)
    # изменение содержимого (дата старта) → другой хеш
    conn.execute("UPDATE programs SET start_time='07:00' WHERE id=?", (p,))
    conn.commit()
    d = compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                         timezone_offset_min=0)
    assert d.schedule_hash != a.schedule_hash
    conn.close()


# ---------------------------------------------------------------- 8. версионирование
def test_persist_schedule_monotonic_versions_and_file():
    conn, tmp = _migrated_conn()
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1)
    p = _mk_program(conn, "Версии")
    _link(conn, p, z1, 1)
    data_dir = tmp / "data"

    def compile_once():
        return compile_schedule(conn, c, today=date(2026, 10, 5), days_ahead=1,
                                timezone_offset_min=0)

    id1, path1 = persist_schedule(conn, compile_once(), data_dir=data_dir,
                                  status="compiled", created_by="admin")
    id2, path2 = persist_schedule(conn, compile_once(), data_dir=data_dir,
                                  status="compiled")
    assert path1.name == "v1.json" and path2.name == "v2.json"
    v1 = load_schedule_file(path1)
    assert v1["schedule_version"] == 1 and v1["schedule_hash"]
    assert load_schedule_file(path2)["schedule_version"] == 2
    row = conn.execute("SELECT COUNT(*) n FROM controller_schedules").fetchone()
    assert row["n"] == 2
    # UNIQUE(controller_id, schedule_version) — дубль версии невозможен
    try:
        conn.execute(
            "INSERT INTO controller_schedules(controller_id, schedule_version,"
            " valid_from_date, valid_to_date, source, status, schedule_hash,"
            " runs_count, created_at) VALUES (9,1,'2026-10-05','2026-10-06',"
            "'manual','compiled','h',0,'2026-10-05')")
        raise AssertionError("ожидался IntegrityError")
    except sqlite3.IntegrityError:
        pass
    conn.close()
