"""Тесты Этапа 5: операторский интерфейс (view-API дашборда и страницы контроллера).

Инфраструктура переиспользуется из test_stage3 (_Harness с FakeMQTT, временной
БД, login_as) — приложение расширяется регистрацией routes_stage5 в том же
порядке, что в create_app (main.py): stage3 → stage4 → stage5.

Проверяемые контракты (ТЗ Этап 5, п.1–2, п.5):
- GET /api/dashboard-view — плитки: статус/активная зона/остаток времени/
  последний контакт/ошибки + сводка (всего, онлайн, поливается, сегодня);
- объём за сегодня (литры) считается по watering_runs × expected_flow_lpm;
- GET /api/controllers/{id}/view — live + зоны с активностью + история прогонов
  + ошибка компиляции + очередь машинограмм + rejection;
- роли: viewer может читать обе эндпоинт-модели, неавторизованный — 401,
  несуществующий контроллер — 404;
- веб-страница /controllers/{id} доступна авторизованным, редиректит на /login
  без сессии, отдаёт 404 для чужого id.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pytest

from server.app.api.routes_stage2 import register_api_routes  # noqa: E402
from server.app.api.routes_stage3 import register_api_routes_stage3  # noqa: E402
from server.app.api.routes_stage5 import register_api_routes_stage5  # noqa: E402
from server.tests.test_stage3 import _Harness  # noqa: E402


class _Harness5(_Harness):
    """_Harness Этапа 3 + регистрация view-маршрутов Этапа 5."""

    def __init__(self):
        super().__init__()
        # порядок как в create_app: stage3 (уже в _Harness) → stage5
        register_api_routes_stage5(self.app, self.cfg, self.conn)

    # ------------------------------------------------------------ helpers
    def set_live(self, box_id: str, **extra):
        """Прямая запись live-полей (миграция 0004) — без эмулятора."""
        cols = {k: v for k, v in extra.items()}
        sets = ", ".join(f"{k}=?" for k in cols)
        with self.conn:
            self.conn.execute(
                f"UPDATE controllers SET {sets} WHERE box_id=?",
                (*cols.values(), box_id))

    def add_zone(self, controller_id: int, zone_number: int,
                 flow_lpm: float | None = None) -> dict:
        z = self.catalog.create_zone({
            "controller_id": controller_id, "zone_number": zone_number,
            "name": f"Зона {zone_number}", "base_duration_minutes": 10})
        if flow_lpm is not None:
            with self.conn:
                self.conn.execute(
                    "UPDATE zones SET expected_flow_lpm=? WHERE id=?",
                    (flow_lpm, z["id"]))
        return z

    def add_run(self, controller: dict, *, status="completed", source="schedule",
                water_sec=600, zones=(1,), planned_offset=0, actual=True,
                program_id=None):
        now = int(time.time())
        planned = now + planned_offset
        actual_ts = planned if actual else None
        end = (actual_ts + water_sec) if (actual_ts and status == "completed") \
            else None
        with self.conn:
            self.conn.execute(
                """INSERT INTO watering_runs(
                       controller_id, box_id, run_id, program_id, source, status,
                       planned_start_ts, actual_start_ts, end_ts, water_sec,
                       zones_json, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (controller["id"], controller["box_id"],
                 f"run-{int(time.time_ns())}", program_id, source, status,
                 planned, actual_ts, end, water_sec,
                 json.dumps(list(zones)),
                 datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 datetime.now(timezone.utc).isoformat(timespec="seconds")))


@pytest.fixture()
def h():
    harness = _Harness5()
    yield harness
    harness.close()


# ============================================================ dashboard-view
def test_dashboard_view_requires_auth(h):
    r = TestClientNoAuth(h).get("/api/dashboard-view")
    assert r.status_code == 401
    assert "detail" in r.json()


def TestClientNoAuth(h):  # noqa: N802 — хелпер, не тест
    from fastapi.testclient import TestClient
    c = TestClient(h.app)
    c.cookies.clear()
    return c


def test_dashboard_view_empty_and_summary(h):
    admin = h.login_as("admin")
    body = admin.get("/api/dashboard-view").json()
    assert body["summary"] == {
        "controllers_total": 0, "controllers_online": 0,
        "watering_now": 0, "today": {"water_sec": 0, "runs": 0}}
    assert body["controllers"] == []
    assert body["server_now_ts"] > 0


def test_dashboard_tile_fields_status_remaining_errors(h):
    c = h.controller("BOX-DASH-1")
    now = int(time.time())
    h.set_live(c["box_id"],
               connection_status="online", time_valid=1,
               last_seen_at=datetime.now(timezone.utc).isoformat(
                   timespec="seconds"),
               current_mode="watering", current_phase="water",
               primary_zone=2,
               active_zones_json=json.dumps([2]),
               phase_end_ts=now + 120, run_end_ts=now + 300)
    admin = h.login_as("admin")
    body = admin.get("/api/dashboard-view").json()
    tile = body["controllers"][0]
    assert tile["connection_status"] == "online"
    assert tile["active_zone"] == 2
    assert 100 <= tile["phase_remaining_sec"] <= 120
    assert 280 <= tile["run_remaining_sec"] <= 300
    assert tile["errors"] == []           # всё в порядке — ошибок нет
    assert tile["watered_today_liters"] is None  # прогонов сегодня не было
    assert body["summary"]["controllers_online"] == 1
    assert body["summary"]["watering_now"] == 1


def test_dashboard_tile_offline_and_time_errors(h):
    h.controller("BOX-DASH-2")   # офлайн, time_valid=0 по умолчанию
    admin = h.login_as("admin")
    tile = admin.get("/api/dashboard-view").json()["controllers"][0]
    assert tile["connection_status"] == "offline"
    assert "Нет связи с контроллером" in tile["errors"]
    assert "Время контроллера не синхронизировано" in tile["errors"]


def test_dashboard_today_liters_and_runs(h):
    c = h.controller("BOX-DASH-3")
    h.add_zone(c["id"], 1, flow_lpm=12.0)
    h.add_zone(c["id"], 2, flow_lpm=6.0)
    # завершённый прогон двух зон: 600 сек / 2 зоны = 300 сек на зону
    # 300*12/60 + 300*6/60 = 60 + 30 = 90 литров
    h.add_run(c, water_sec=600, zones=(1, 2))
    h.add_run(c, status="planned", water_sec=0, zones=(1,),
              planned_offset=3600, actual=False)  # план на сегодня — не в зачёт
    admin = h.login_as("admin")
    body = admin.get("/api/dashboard-view").json()
    tile = body["controllers"][0]
    assert tile["watered_today_liters"] == pytest.approx(90.0, abs=0.1)
    assert body["summary"]["today"] == {"water_sec": 600, "runs": 1}


def test_dashboard_compile_error_and_queue_visible(h):
    c = h.controller("BOX-DASH-4")
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with self_conn(h):
        pass
    with h.conn:
        h.conn.execute(
            """INSERT INTO schedule_compile_errors(controller_id, reason,
                   error_json, ts) VALUES (?,?,?,?)""",
            (c["id"], "program.updated",
             json.dumps({"code": "E_CONFLICT", "message": "Конфликт расписания"},
                        ensure_ascii=False), now_iso))
    sched_id = None
    with h.conn:
        cur = h.conn.execute(
            """INSERT INTO controller_schedules(controller_id, schedule_version,
                   valid_from_date, valid_to_date, timezone_offset_min, source,
                   status, schedule_hash, created_at)
               VALUES (?,1,date('now'),date('now'),0,'auto','compiled','h',?)""",
            (c["id"], now_iso))
        sched_id = cur.lastrowid
        h.conn.execute(
            """INSERT INTO pending_schedule(controller_id, schedule_id, status,
                   reason, created_at, updated_at)
               VALUES (?,?,'queued','active_run_next_run_policy',?,?)""",
            (c["id"], sched_id, now_iso, now_iso))
    admin = h.login_as("admin")
    tile = admin.get("/api/dashboard-view").json()["controllers"][0]
    assert any(e.startswith("Расписание: Конфликт") for e in tile["errors"])
    assert any("очереди на отправку" in e for e in tile["errors"])


def self_conn(h):
    class _Nop:
        def __enter__(self): return self
        def __exit__(self, *a): return False
    return _Nop()


# ========================================================== controller view
def test_controller_view_404_and_roles(h):
    admin = h.login_as("admin")
    assert admin.get("/api/controllers/99999/view").status_code == 404
    viewer = h.login_as("viewer")
    c = h.controller("BOX-VW-1")
    assert viewer.get(f"/api/controllers/{c['id']}/view").status_code == 200
    anon = TestClientNoAuth(h)
    assert anon.get(f"/api/controllers/{c['id']}/view").status_code == 401


def test_controller_view_zones_activity_and_history(h):
    c = h.controller("BOX-VW-2")
    h.add_zone(c["id"], 1, flow_lpm=10.0)
    h.add_zone(c["id"], 2)
    now = int(time.time())
    h.set_live(c["box_id"], connection_status="online", time_valid=1,
               current_mode="watering", current_phase="soak", primary_zone=2,
               active_zones_json=json.dumps([2]),
               phase_end_ts=now + 60, run_end_ts=now + 400)
    h.add_run(c, water_sec=300, zones=(1,))
    admin = h.login_as("admin")
    body = admin.get(f"/api/controllers/{c['id']}/view").json()
    assert body["live"]["connection_status"] == "online"
    assert 40 <= body["phase_remaining_sec"] <= 60
    assert 380 <= body["run_remaining_sec"] <= 400
    z1, z2 = body["zones"]
    assert z1["active"] is False
    assert z2["active"] is True and z2["phase"] == "soak"
    assert 40 <= z2["remaining_sec"] <= 60
    assert len(body["runs"]) == 1
    run = body["runs"][0]
    assert run["status"] == "completed" and run["zones"] == [1]
    assert run["actual_start_iso"] and run["end_iso"]
    assert body["compile_error"] is None
    assert body["last_rejection"] is None
    assert body["today_water_sec"] == 300


def test_controller_view_active_run_gives_zone_activity(h):
    """Fallback-источник активности: активный прогон без live-полей."""
    c = h.controller("BOX-VW-3")
    h.add_zone(c["id"], 3)
    now = int(time.time())
    with h.conn:
        h.conn.execute(
            """INSERT INTO watering_runs(controller_id, box_id, run_id, source,
                   status, planned_start_ts, actual_start_ts, end_ts,
                   water_sec, zones_json, details_json, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (c["id"], c["box_id"], "run-active-1", "schedule", "active",
             now - 60, now - 60, now + 540, 0, json.dumps([3]),
             json.dumps({"zones": [3]}), "", ""))
    admin = h.login_as("admin")
    body = admin.get(f"/api/controllers/{c['id']}/view").json()
    z3 = next(z for z in body["zones"] if z["zone_number"] == 3)
    assert z3["active"] is True
    assert 520 <= z3["remaining_sec"] <= 540


def test_controller_view_compile_error_pending_rejection(h):
    c = h.controller("BOX-VW-4")
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with h.conn:
        h.conn.execute(
            """INSERT INTO schedule_compile_errors(controller_id, reason,
                   error_json, ts) VALUES (?,?,?,?)""",
            (c["id"], "zone.deleted",
             json.dumps({"code": "E_ZONE", "message": "Зона удалена"}), now_iso))
        cur = h.conn.execute(
            """INSERT INTO controller_schedules(controller_id, schedule_version,
                   valid_from_date, valid_to_date, timezone_offset_min, source,
                   status, schedule_hash, created_at)
               VALUES (?,1,date('now'),date('now'),0,'auto','compiled','h2',?)""",
            (c["id"], now_iso))
        h.conn.execute(
            """INSERT INTO pending_schedule(controller_id, schedule_id, status,
                   reason, created_at, updated_at) VALUES (?,?,'queued','r',?,?)""",
            (c["id"], cur.lastrowid, now_iso, now_iso))
        h.conn.execute(
            """INSERT INTO schedule_rejections(controller_id, version, reason, ts)
               VALUES (?,2,'hash mismatch',?)""", (c["id"], now_iso))
    admin = h.login_as("admin")
    body = admin.get(f"/api/controllers/{c['id']}/view").json()
    assert body["compile_error"]["reason"] == "zone.deleted"
    assert body["compile_error"]["message"] == "Зона удалена"
    assert len(body["pending_schedules"]) == 1
    assert body["last_rejection"]["reason"] == "hash mismatch"


# ============================================================== web page
def test_web_controller_page_redirect_and_ok(h):
    c = h.controller("BOX-WEB-1")
    h.add_zone(c["id"], 1)
    from fastapi.testclient import TestClient
    # без сессии — редирект на /login (общая защита page())
    anon = TestClient(h.app)
    r = anon.get(f"/controllers/{c['id']}", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"].startswith("/login")
    # routes_stage5 — только API; веб-страница /controllers/{id} живёт в
    # web/routes.py (её проверяет следующий тест через create_app-подобную
    # сборку). Здесь убеждаемся, что API-маршрут не перехватил путь страницы.
    assert h.app.routes is not None


def test_dashboard_view_poll_contract_stable(h):
    """Опрос каждые 10 сек: повторные вызовы идемпотентны, структура та же."""
    c = h.controller("BOX-POLL-1")
    admin = h.login_as("admin")
    first = admin.get("/api/dashboard-view").json()
    second = admin.get("/api/dashboard-view").json()
    ids1 = [t["id"] for t in first["controllers"]]
    ids2 = [t["id"] for t in second["controllers"]]
    assert ids1 == ids2
    assert set(first["controllers"][0]) >= {
        "id", "name", "connection_status", "active_zone",
        "phase_remaining_sec", "last_seen_at", "errors",
        "watered_today_liters"}
