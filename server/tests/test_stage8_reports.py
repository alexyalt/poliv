"""Тесты Этапа 8, блок 8.1: отчёты базовой аналитики (сервис + API + страница).

Инфраструктура — харнесс Этапа 3/6 (_Harness/_Harness6): временная БД со всеми
миграциями, login_as(role) с cookies-сессией. Дополнительно регистрируются
маршруты /api/v1/reports/* (routes_reports) и веб-страница /reports.

Проверяемые контракты (промпт Этапа 8, Шаг 8.1):
- каждый из шести отчётов возвращает корректные агрегаты на фикстурах;
- пустой/перевёрнутый диапазон → пустой результат, не ошибка;
- CSV содержит стабильные заголовки даже при пустом результате, _csv_safe
  экранирует формулы (защита spreadsheet formula injection);
- ACL: viewer/operator/admin читают, без входа 401;
- пагинация инцидентов (limit/offset);
- preset today/week/month/quarter/year; 422 на битых датах;
- страница /reports отдаёт каркас для залогиненного пользователя.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

import pytest

from server.app.api.routes_reports import register_api_routes_reports
from server.app.services import report_service as rs
from server.tests.test_stage6 import _Harness6


class _Harness8(_Harness6):
    def __init__(self):
        super().__init__(with_web=True)
        register_api_routes_reports(self.app, self.cfg, self.conn)


@pytest.fixture()
def h8():
    h = _Harness8()
    yield h
    h.close()


# ------------------------------------------------------------------ фикстуры
def _ts(days_ago: int, hour: int = 7) -> int:
    d = datetime.now() - timedelta(days=days_ago)
    return int(d.replace(hour=hour, minute=0, second=0).timestamp())


def mk_controller(h, name="Бокс 1", box_id=None):
    return h.catalog.create_controller({"name": name, "box_id": box_id})


def mk_zone(h, cid, num):
    return h.catalog.create_zone({"controller_id": cid, "zone_number": num,
                                  "name": f"Зона {num}",
                                  "base_duration_minutes": 10})


def mk_program(h, name="Утро"):
    return h.catalog.create_program({"name": name, "start_time": "06:00",
                                     "schedule_type": "weekdays",
                                     "weekdays": [1, 2, 3, 4, 5]})


def add_run(h, *, cid, box_id, run_id, status="completed", source="schedule",
            program_id=None, zones="[1]", water_sec=600, volume=None,
            ts=None, reason_code=None):
    t = ts if ts is not None else _ts(1)
    with h.conn:
        h.conn.execute(
            """INSERT INTO watering_runs(controller_id, box_id, run_id,
                   program_id, source, status, reason_code,
                   planned_start_ts, actual_start_ts, end_ts, water_sec,
                   volume_liters, zones_json, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cid, box_id, run_id, program_id, source, status, reason_code,
             t, t, t + water_sec, water_sec, volume, zones,
             datetime.now().isoformat(), datetime.now().isoformat()))


def add_event(h, *, cid, uid, status="finished", zone_ids=(1,), water_sec=600,
              volume=50.0, start_ts=None, reason_code=None, aborted=0,
              run_id=None):
    t = start_ts if start_ts is not None else _ts(1)
    with h.conn:
        cur = h.conn.execute(
            """INSERT INTO watering_events(event_uid, run_id, controller_id,
                   source, status, start_ts, end_ts, water_sec, volume_liters,
                   reason_code, aborted)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (uid, run_id, cid, "manual", status, t, t + water_sec, water_sec,
             volume, reason_code, aborted))
        eid = cur.lastrowid
        for zn in zone_ids:
            zid = h.conn.execute(
                "SELECT id FROM zones WHERE controller_id=? AND zone_number=?",
                (cid, zn)).fetchone()["id"]
            h.conn.execute(
                "INSERT INTO watering_event_zones(event_id, zone_id,"
                " water_sec, volume_liters) VALUES (?,?,?,?)",
                (eid, zid, water_sec, volume))


def add_notification(h, *, ctype="no_flow", severity="critical",
                     controller_id=None, message="Нет протока"):
    with h.conn:
        h.conn.execute(
            """INSERT INTO notifications(type, severity, controller_id,
                   message, dedupe_key)
               VALUES (?,?,?,?,?)""",
            (ctype, severity, controller_id, message,
             f"{ctype}:{controller_id}:{time.time_ns()}"))


def seed_scenario(h):
    """Контроллер + 2 зоны + программа; вчерашние прогоны/события/аварии."""
    c = mk_controller(h, "Бокс 1")
    z1 = mk_zone(h, c["id"], 1)
    z2 = mk_zone(h, c["id"], 2)
    p = mk_program(h, "Утро")
    # Успешный прогон по обеим зонам вчера: 10 мин, 50 л.
    add_run(h, cid=c["id"], box_id=c["box_id"], run_id="R-OK",
            program_id=p["id"], zones="[1, 2]", water_sec=600, volume=50.0)
    add_event(h, cid=c["id"], uid="E-OK", run_id="R-OK",
              zone_ids=(1, 2), water_sec=600, volume=50.0)
    # Пропущенный прогон сегодня утром.
    add_run(h, cid=c["id"], box_id=c["box_id"], run_id="R-SKIP",
            status="skipped", zones="[1]", water_sec=0, ts=_ts(0),
            reason_code="rain_delay")
    # Аварийно остановленный прогон (зона 2).
    add_run(h, cid=c["id"], box_id=c["box_id"], run_id="R-ERR",
            status="aborted", zones="[2]", water_sec=120, ts=_ts(0),
            reason_code="no_flow")
    add_event(h, cid=c["id"], uid="E-ERR", run_id="R-ERR", status="stopped",
              zone_ids=(2,), water_sec=120, volume=8.0, start_ts=_ts(0),
              reason_code="no_flow", aborted=1)
    add_notification(h, controller_id=c["id"])
    return {"controller": c, "z1": z1, "z2": z2, "program": p}


def _today():
    return datetime.now().strftime("%Y-%m-%d")


def _yesterday():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ============================================================ сервисный слой
def test_report_by_zones_aggregates(h8):
    seed_scenario(h8)
    rows = rs.report_by_zones(h8.conn, _yesterday(), _today())
    by_num = {r["zone_number"]: r for r in rows}
    assert set(by_num) == {1, 2}
    assert by_num[1]["runs_count"] == 1          # только завершённый R-OK
    assert by_num[1]["total_minutes"] == 10.0
    assert by_num[1]["skips_count"] == 1         # R-SKIP
    assert by_num[2]["errors_count"] >= 1       # R-ERR (+ stopped-event)
    assert by_num[1]["controller_name"] == "Бокс 1"


def test_report_by_controllers_aggregates(h8):
    seed_scenario(h8)
    rows = rs.report_by_controllers(h8.conn, _yesterday(), _today())
    assert len(rows) == 1
    r = rows[0]
    assert r["controller_name"] == "Бокс 1"
    assert r["runs_count"] == 1
    assert r["total_minutes"] == 10.0
    assert r["skips_count"] == 1
    assert r["errors_count"] >= 1
    assert r["avg_duration_min"] == 10.0


def test_report_by_days_and_months(h8):
    seed_scenario(h8)
    days = rs.report_by_days(h8.conn, _yesterday(), _today())
    # Дня в отчёте ровно столько, сколько реально дат в сценарии
    # (зависит от таймзоны окружения: «сегодня 07:00» может сместиться).
    assert 1 <= len(days) <= 2
    assert all(d["day"] in (_yesterday(), _today()) for d in days)
    total_runs = sum(d["runs_count"] for d in days)
    assert total_runs == 2                      # OK (вчера) + ERR (сегодня);
    # skipped-прогон в дневную сводку не входит (в ней только поливы/аварии).
    months = rs.report_by_months(h8.conn, _yesterday(), _today())
    assert len(months) >= 1
    assert sum(m["runs_count"] for m in months) == total_runs


def test_report_by_programs(h8):
    seed_scenario(h8)
    rows = rs.report_by_programs(h8.conn, _yesterday(), _today())
    names = {r["program_name"] for r in rows}
    assert "Утро" in names and "Без программы" in names
    morning = next(r for r in rows if r["program_name"] == "Утро")
    assert morning["runs_count"] == 1


def test_report_by_incidents_counts(h8):
    seed_scenario(h8)
    data = rs.report_by_incidents(h8.conn, _yesterday(), _today())
    assert data["counters"]["skips"] == 1
    assert data["counters"]["errors"] >= 1
    assert data["counters"]["notifications"] == 1
    reasons = set(data["by_reason"])
    assert "rain_delay" in reasons and "no_flow" in reasons


def test_empty_range_returns_empty_not_error(h8):
    seed_scenario(h8)
    future_from = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
    future_to = (datetime.now() + timedelta(days=11)).strftime("%Y-%m-%d")
    for fn in (rs.report_by_zones, rs.report_by_controllers, rs.report_by_days,
               rs.report_by_months, rs.report_by_programs):
        out = fn(h8.conn, future_from, future_to)
        assert out == [] or out == {"items": [], "counters": {
            "skips": 0, "errors": 0, "notifications": 0}, "by_reason": {}} \
            or (isinstance(out, list) and all(
                r.get("runs_count", 0) == 0 for r in out))
    inc = rs.report_by_incidents(h8.conn, future_from, future_to)
    assert inc["items"] == []


def test_reversed_range_is_normalized(h8):
    seed_scenario(h8)
    # Явные фиксированные даты (2026-10-01..2026-10-05), чтобы проверка
    # не зависела от таймзоны окружения.
    d_lo, d_hi = "2026-10-01", "2026-10-05"
    ts = int(datetime(2026, 10, 3, 7, 0).timestamp())
    c = mk_controller(h8, "Бокс ТЗ")
    add_run(h8, cid=c["id"], box_id=c["box_id"], run_id="R-TZ",
            water_sec=600, volume=40.0, ts=ts)
    a = rs.report_by_days(h8.conn, d_hi, d_lo)     # перевёрнутый диапазон
    b = rs.report_by_days(h8.conn, d_lo, d_hi)     # нормальный
    assert a == b                                  # переворот не ломает данные
    assert len(a) >= 1 and sum(d["runs_count"] for d in a) >= 1


def test_resolve_range_presets():
    now = datetime(2026, 10, 27, 12, 0)
    f, t = rs.resolve_range(None, None, "week", now=now)
    assert (f, t) == ("2026-10-21", "2026-10-27")
    f, t = rs.resolve_range("2026-01-01", "2026-01-31", "week", now=now)
    assert (f, t) == ("2026-01-01", "2026-01-31")   # from/to приоритетнее


# ==================================================================== API
def test_api_requires_auth(h8):
    r = h8.client.get("/api/v1/reports/zones")
    assert r.status_code == 401


def test_api_viewer_operator_admin_read(h8):
    seed_scenario(h8)
    for role in ("viewer", "operator", "admin"):
        c = h8.login_as(role)
        r = c.get(f"/api/v1/reports/controllers?from={_yesterday()}&to={_today()}")
        assert r.status_code == 200, role
        body = r.json()
        assert body["report"] == "controllers"
        assert len(body["data"]) == 1


def test_api_all_six_reports_ok(h8):
    seed_scenario(h8)
    c = h8.login_as("viewer")
    for name in rs.REPORTS:
        r = c.get(f"/api/v1/reports/{name}?preset=month")
        assert r.status_code == 200, name
        assert "data" in r.json()


def test_api_bad_date_422(h8):
    c = h8.login_as("viewer")
    r = c.get("/api/v1/reports/days?from=не-дата")
    assert r.status_code == 422
    r = c.get("/api/v1/reports/days?preset=decade")
    assert r.status_code == 422


def test_api_empty_db_returns_empty_tables(h8):
    c = h8.login_as("viewer")
    r = c.get("/api/v1/reports/zones?from=2020-01-01&to=2020-01-02")
    assert r.status_code == 200 and r.json()["data"] == []


def test_csv_headers_present_when_empty(h8):
    c = h8.login_as("viewer")
    r = c.get("/api/v1/reports/zones.csv?from=2020-01-01&to=2020-01-02")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    lines = r.text.strip().splitlines()
    assert lines[0] == ",".join(rs.CSV_HEADERS["zones"])
    assert len(lines) == 1                       # только заголовок


def test_csv_contains_rows(h8):
    seed_scenario(h8)
    c = h8.login_as("viewer")
    r = c.get(f"/api/v1/reports/controllers.csv?from={_yesterday()}&to={_today()}")
    assert r.status_code == 200
    lines = r.text.strip().splitlines()
    assert len(lines) == 2                       # заголовок + контроллер
    assert "Бокс 1" in lines[1]


def test_csv_formula_injection_guard(h8):
    """Имя контроллера-формула экранируется апострофом (_csv_safe)."""
    mk_controller(h8, '=HYPERLINK("http://evil")', box_id="BOX-EVIL")
    c = h8.login_as("viewer")
    r = c.get("/api/v1/reports/controllers.csv")
    assert r.status_code == 200
    assert "'=HYPERLINK" in r.text                # апостроф перед формулой
    # «голая» формула в начале поля не проходит: поле начинается с = только
    # внутри кавычек CSV (экранированная форма — ""..."" внутри поля).
    for line in r.text.splitlines()[1:]:
        if "HYPERLINK" in line:
            assert '"\'=' in line or line.startswith("'")


def test_incidents_pagination(h8):
    for i in range(5):
        add_notification(h8, ctype=f"alert_{i}", severity="warning",
                         message=f"m{i}")
    c = h8.login_as("viewer")
    r = c.get("/api/v1/reports/incidents?limit=2&offset=0")
    body = r.json()["data"]
    assert len(body["items"]) == 2 and body["total"] == 5
    r2 = c.get("/api/v1/reports/incidents?limit=2&offset=4")
    assert len(r2.json()["data"]["items"]) == 1


def test_alias_route_without_v1(h8):
    c = h8.login_as("viewer")
    assert c.get("/api/reports/days").status_code == 200


def test_reports_page_requires_login(h8):
    r = h8.client.get("/reports", follow_redirects=False)
    assert r.status_code == 302 and r.headers["location"] == "/login"


def test_reports_page_renders_for_viewer(h8):
    c = h8.login_as("viewer")
    r = c.get("/reports")
    assert r.status_code == 200
    assert "Отчёты" in r.text and "Экспорт CSV" in r.text


def test_nav_contains_reports_link(h8):
    c = h8.login_as("viewer")
    r = c.get("/")
    assert 'href="/reports"' in r.text
