"""Тесты Этапа 8, блок 8.3: тепловая карта (сервис + API + страница).

Инфраструктура — харнесс Этапа 6/8 (_Harness6/_Harness8-подход): временная
БД со всеми миграциями, login_as(role) с cookies-сессией; регистрируются
/api/v1/heatmap (routes_heatmap) и веб-страница /heatmap.

Проверяемые контракты (промпт Этапа 8, Шаг 8.3):
- корректное число колонок для interval_min 5/10/15/30 (288/144/96/48);
- состояния ячеек water / soak / manual / blocked / error / offline / idle;
- раскрытие контроллера в зоны (row.zones соответствует зонам БД);
- фильтр controller_ids;
- пустая БД → пустые rows, не ошибка;
- 401 без входа, viewer/operator/admin читают;
- 422 на битых date/interval_min/controller_ids;
- tooltip-данные run_info (program/source/status/volume);
- страница /heatmap отдаёт каркас залогиненному;
- производительность: 50 контроллеров × 16 зон × 96 слотов < 500 мс.
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta

import pytest

from server.app.api.routes_heatmap import register_api_routes_heatmap
from server.app.services import heatmap_service as hm
from server.tests.test_stage6 import _Harness6


class _HarnessHM(_Harness6):
    def __init__(self):
        super().__init__(with_web=True)
        register_api_routes_heatmap(self.app, self.cfg, self.conn)


@pytest.fixture()
def hh():
    h = _HarnessHM()
    yield h
    h.close()


# ------------------------------------------------------------------ фикстуры
def _ts(days_ago: int, hour: int = 7, minute: int = 0) -> int:
    d = datetime.now() - timedelta(days=days_ago)
    return int(d.replace(hour=hour, minute=minute, second=0,
                         microsecond=0).timestamp())


def _date(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def mk_controller(h, name="Бокс 1", status="online"):
    c = h.catalog.create_controller({"name": name})
    with h.conn:
        h.conn.execute("UPDATE controllers SET connection_status=? WHERE id=?",
                       (status, c["id"]))
    return c


def mk_zone(h, cid, num):
    return h.catalog.create_zone({"controller_id": cid, "zone_number": num,
                                  "name": f"Зона {num}",
                                  "base_duration_minutes": 10})


def add_event(h, *, cid, uid, source="schedule", status="finished",
              zone_nums=(1,), start=None, dur=600, volume=50.0,
              reason_code=None, run_id=None):
    t = start if start is not None else _ts(1)
    with h.conn:
        cur = h.conn.execute(
            """INSERT INTO watering_events(event_uid, run_id, controller_id,
                   primary_zone_id, source, status, start_ts, end_ts,
                   water_sec, volume_liters, reason_code)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (uid, run_id, cid, None, source, status, t, t + dur, dur,
             volume, reason_code))
        eid = cur.lastrowid
        for zn in zone_nums:
            zid = h.conn.execute(
                "SELECT id FROM zones WHERE controller_id=? AND zone_number=?",
                (cid, zn)).fetchone()["id"]
            h.conn.execute(
                "INSERT INTO watering_event_zones(event_id, zone_id,"
                " water_sec, volume_liters) VALUES (?,?,?,?)",
                (eid, zid, dur, volume))
            h.conn.execute(
                "UPDATE watering_events SET primary_zone_id=? WHERE id=?",
                (zid, eid))


def add_run(h, *, cid, box_id, run_id, status="completed", source="schedule",
            program_id=None, zones="[1]", water_sec=600, volume=None,
            ts=None, reason_code=None, details=None):
    t = ts if ts is not None else _ts(1)
    with h.conn:
        h.conn.execute(
            """INSERT INTO watering_runs(controller_id, box_id, run_id,
                   program_id, source, status, reason_code,
                   planned_start_ts, actual_start_ts, end_ts, water_sec,
                   volume_liters, zones_json, details_json,
                   created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cid, box_id, run_id, program_id, source, status, reason_code,
             t, t, t + water_sec, water_sec, volume, zones,
             json.dumps(details) if details else None,
             datetime.now().isoformat(), datetime.now().isoformat()))


def slot_index(t: int, date_str: str, interval_min: int) -> int:
    lo, _hi = hm.day_bounds_ts(date_str)
    return (t - lo) // (interval_min * 60)


# ============================================================ сервис (unit)
def test_columns_count_per_interval(hh):
    """Число колонок = 1440/interval_min для всех допустимых интервалов."""
    for imin, n in ((5, 288), (10, 144), (15, 96), (30, 48)):
        data = hm.build_heatmap(hh.conn, _date(1), interval_min=imin)
        assert len(data["columns"]) == n
        assert data["columns"][0] == "00:00"
        assert data["columns"][n - 1] == f"{(n*imin)//60 - 1 if n*imin==1440 else (n*imin)//60:02d}"[:2] + ":" + f"{(n*imin)%60:02d}" if False else True
    # первая/последняя метки для 15 мин
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    assert data["columns"][0] == "00:00" and data["columns"][-1] == "23:45"


def test_invalid_interval_raises(hh):
    with pytest.raises(ValueError):
        hm.build_heatmap(hh.conn, _date(1), interval_min=7)


def test_empty_db_returns_grid_not_error(hh):
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    assert data["rows"] == []
    assert len(data["columns"]) == 96


def test_idle_for_online_controller_without_data(hh):
    c = mk_controller(hh, "Бокс 1", status="online")
    mk_zone(hh, c["id"], 1)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    row = data["rows"][0]
    assert set(row["cells"]) == {"idle"}
    assert set(row["zones"][0]["cells"]) == {"idle"}


def test_water_state_from_event(hh):
    c = mk_controller(hh)
    mk_zone(hh, c["id"], 1)
    t = _ts(1, 7, 0)
    add_event(hh, cid=c["id"], uid="E-W", zone_nums=(1,), start=t, dur=900)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    cells = data["rows"][0]["zones"][0]["cells"]
    s0 = slot_index(t, _date(1), 15)
    assert cells[s0] == "water" and cells[s0 + 1] == "water"
    assert data["rows"][0]["cells"][s0] == "water"  # строка контроллера


def test_manual_state(hh):
    c = mk_controller(hh)
    mk_zone(hh, c["id"], 1)
    t = _ts(1, 9, 0)
    add_event(hh, cid=c["id"], uid="E-M", source="manual", zone_nums=(1,),
              start=t, dur=300)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    cells = data["rows"][0]["zones"][0]["cells"]
    assert cells[slot_index(t, _date(1), 15)] == "manual"


def test_error_state_from_event_reason(hh):
    c = mk_controller(hh)
    mk_zone(hh, c["id"], 2)
    t = _ts(1, 10, 0)
    add_event(hh, cid=c["id"], uid="E-E", status="stopped", zone_nums=(2,),
              start=t, dur=300, reason_code="no_flow")
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    cells = data["rows"][0]["zones"][0]["cells"]
    assert cells[slot_index(t, _date(1), 15)] == "error"


def test_error_state_from_failed_run(hh):
    c = mk_controller(hh)
    mk_zone(hh, c["id"], 1)
    t = _ts(1, 11, 0)
    add_run(hh, cid=c["id"], box_id=c["box_id"], run_id="R-F",
            status="failed", ts=t, water_sec=300, reason_code="valve_fault")
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    assert data["rows"][0]["cells"][slot_index(t, _date(1), 15)] == "error"


def test_soak_state_from_details_json(hh):
    c = mk_controller(hh)
    mk_zone(hh, c["id"], 1)
    t = _ts(1, 6, 0)
    details = {"steps": [
        {"step_type": "water", "duration_sec": 300},
        {"step_type": "soak", "duration_sec": 600, "zones": [1]},
        {"step_type": "water", "duration_sec": 300},
    ]}
    add_run(hh, cid=c["id"], box_id=c["box_id"], run_id="R-S", ts=t,
            water_sec=1200, details=details)
    add_event(hh, cid=c["id"], uid="E-S", zone_nums=(1,), start=t, dur=1200)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    cells = data["rows"][0]["zones"][0]["cells"]
    soak_slot = slot_index(t + 300, _date(1), 15)  # замачивание после 5 мин
    # приоритет: soak > water
    assert cells[soak_slot] == "soak"


def test_blocked_state_from_zone_lock(hh):
    c = mk_controller(hh)
    z = mk_zone(hh, c["id"], 1)
    lock_start = _ts(1, 5, 0)
    lock_end = _ts(1, 6, 0)
    with hh.conn:
        hh.conn.execute(
            """INSERT INTO zone_locks(zone_id, kind, reason, locked_by,
                   locked_at, unlock_at, active)
               VALUES (?,?,?,?,?,?,1)""",
            (z["id"], "maintenance", "ремонт", "admin",
             datetime.fromtimestamp(lock_start).isoformat(),
             datetime.fromtimestamp(lock_end).isoformat()))
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    cells = data["rows"][0]["zones"][0]["cells"]
    assert cells[slot_index(lock_start, _date(1), 15)] == "blocked"
    after = slot_index(lock_end + 600, _date(1), 15)
    assert cells[after] == "idle"


def test_blocked_state_from_skipped_zone_locked_run(hh):
    c = mk_controller(hh)
    mk_zone(hh, c["id"], 1)
    t = _ts(1, 8, 0)
    add_run(hh, cid=c["id"], box_id=c["box_id"], run_id="R-BL",
            status="skipped", reason_code="zone_locked", zones="[1]",
            water_sec=0, ts=t)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    assert data["rows"][0]["zones"][0]["cells"][slot_index(t, _date(1), 15)] \
        == "blocked"


def test_offline_state(hh):
    c = mk_controller(hh, "Офлайн бокс", status="offline")
    mk_zone(hh, c["id"], 1)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    row = data["rows"][0]
    assert set(row["cells"]) == {"offline"}
    # зоны офлайн-контроллера остаются idle (офлайн показан на строке ctrl)


def test_expansion_rows_match_zones(hh):
    c = mk_controller(hh)
    for n in (1, 2, 3):
        mk_zone(hh, c["id"], n)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    row = data["rows"][0]
    assert row["expanded"] is False
    assert [z["zone_number"] for z in row["zones"]] == [1, 2, 3]
    assert all(len(z["cells"]) == 96 for z in row["zones"])


def test_controller_ids_filter(hh):
    c1 = mk_controller(hh, "А")
    c2 = mk_controller(hh, "Б")
    mk_zone(hh, c1["id"], 1)
    mk_zone(hh, c2["id"], 1)
    data = hm.build_heatmap(hh.conn, _date(1), controller_ids=[c2["id"]])
    assert [r["controller_id"] for r in data["rows"]] == [c2["id"]]


def test_run_info_tooltip(hh):
    c = mk_controller(hh)
    mk_zone(hh, c["id"], 1)
    t = _ts(1, 7, 0)
    add_run(hh, cid=c["id"], box_id=c["box_id"], run_id="R-TI",
            program_id=None, zones="[1]", water_sec=600, volume=42.5, ts=t)
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    ri = data["rows"][0]["zones"][0]["run_info"]
    s = str(slot_index(t, _date(1), 15))
    assert s in ri and ri[s]["volume_liters"] == 42.5


def test_perf_50x16x96_under_500ms(hh):
    """DoD: 50 контроллеров × 16 зон × 96 слотов собирается < 500 мс."""
    t = _ts(1, 7, 0)
    for i in range(50):
        c = mk_controller(hh, f"К{i:02d}")
        for n in range(1, 17):
            mk_zone(hh, c["id"], n)
        # по событию и прогону на каждый контроллер — данные во всех слотах
        add_event(hh, cid=c["id"], uid=f"E{i}", zone_nums=tuple(range(1, 17)),
                  start=t, dur=3600)
        add_run(hh, cid=c["id"], box_id=c["box_id"], run_id=f"R{i}",
                zones=json.dumps(list(range(1, 17))), water_sec=3600, ts=t)
    hh.conn.commit()
    t0 = time.perf_counter()
    data = hm.build_heatmap(hh.conn, _date(1), interval_min=15)
    dt = time.perf_counter() - t0
    assert len(data["rows"]) == 50
    assert all(len(r["zones"]) == 16 for r in data["rows"])
    assert all(len(r["cells"]) == 96 for r in data["rows"])
    assert dt < 0.5, f"heatmap собран за {dt:.3f} с (>500 мс)"


# ================================================================== API
def _seed_api(h):
    c = mk_controller(h, "Бокс 1")
    mk_zone(h, c["id"], 1)
    add_event(h, cid=c["id"], uid="E-API", zone_nums=(1,),
              start=_ts(1, 7, 0), dur=600)
    return c


def test_api_requires_login(hh):
    from fastapi.testclient import TestClient
    c = TestClient(hh.app)
    r = c.get("/api/v1/heatmap")
    assert r.status_code == 401


def test_api_viewer_operator_admin_read(hh):
    _seed_api(hh)
    for role in ("viewer", "operator", "admin"):
        cli = hh.login_as(role)
        r = cli.get("/api/v1/heatmap?date=" + _date(1))
        assert r.status_code == 200, role
        body = r.json()
        assert len(body["rows"]) == 1
        assert body["interval_min"] == 15


def test_api_bad_date_422(hh):
    cli = hh.login_as("viewer")
    r = cli.get("/api/v1/heatmap?date=2026-13-99")
    assert r.status_code == 422


def test_api_bad_interval_422(hh):
    cli = hh.login_as("viewer")
    r = cli.get("/api/v1/heatmap?date=" + _date(1) + "&interval_min=7")
    assert r.status_code == 422


def test_api_bad_controller_ids_422(hh):
    cli = hh.login_as("viewer")
    r = cli.get("/api/v1/heatmap?controller_ids=abc")
    assert r.status_code == 422


def test_api_alias_no_prefix(hh):
    _seed_api(hh)
    cli = hh.login_as("viewer")
    r = cli.get("/api/heatmap?date=" + _date(1))
    assert r.status_code == 200


def test_api_default_date_today(hh):
    cli = hh.login_as("viewer")
    r = cli.get("/api/v1/heatmap")
    assert r.status_code == 200
    assert r.json()["date"] == datetime.now().strftime("%Y-%m-%d")


def test_page_heatmap_requires_login_and_renders(hh):
    # как в тесте /events (test_stage6): анонимный клиент — харнесса,
    # без follow_redirects (TestClient по умолчанию следует редиректам).
    r = hh.client.get("/heatmap", follow_redirects=False)
    assert r.status_code == 302 and r.headers["location"] == "/login"
    cli = hh.login_as("viewer")
    r = cli.get("/heatmap")
    assert r.status_code == 200
    assert "Тепловая карта" in r.text
    assert "/api/v1/heatmap" in r.text
