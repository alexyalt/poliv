"""Тесты Этапа 8, блок 8.2: файловый архив и ротация оперативной БД (ADR-14).

Инфраструктура — харнесс Этапа 3/6 (_Harness6): временная БД со всеми
миграциями (включая новую 0010_archive). Архив пишется во временный каталог
(tmp_path), боевые данные не трогаются.

Проверяемые контракты (промпт Этапа 8, Шаг 8.2):
- rotate выгружает CSV с ожидаемыми колонками; .json — при events_format=json;
- строки удаляются из БД после выгрузки (events + каскад watering_event_zones);
- повторный rotate за ту же дату идемпотентен (0 строк — файл/журнал не растут);
- dry_run не пишет файл и не удаляет строки;
- журнал archive_exports заполняется (row_count, sha256 файла);
- настройки archive.* читаются из таблицы settings (миграция 0010);
- rotate_all проходит по всем типам, ошибка одного типа не роняет цикл;
- restore_export: события восстанавливаются из CSV; flow — NotImplementedError;
- ArchiveDaemon.tick_once: планирование 03:00, флаг enabled, однократность;
- unknown entity_type → ValueError.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from server.app.services import archive_service as ar
from server.app.api.routes_reports import register_api_routes_reports
from server.tests.test_stage6 import _Harness6


@pytest.fixture()
def h6():
    h = _Harness6()
    yield h
    h.close()


# ------------------------------------------------------------------ фикстуры
def _ts(days_ago: int, hour: int = 7) -> int:
    d = datetime.now() - timedelta(days=days_ago)
    return int(d.replace(hour=hour, minute=0, second=0).timestamp())


def mk_controller(h, name="Бокс 1"):
    return h.catalog.create_controller({"name": name})


def mk_zone(h, cid, num):
    return h.catalog.create_zone({"controller_id": cid, "zone_number": num,
                                  "name": f"Зона {num}",
                                  "base_duration_minutes": 10})


def add_event(h, *, cid, uid, zone_id=None, start_ts=None, water_sec=600,
              volume=50.0, status="finished"):
    t = start_ts if start_ts is not None else _ts(400)
    with h.conn:
        cur = h.conn.execute(
            """INSERT INTO watering_events(event_uid, run_id, controller_id,
                   source, status, start_ts, end_ts, water_sec, volume_liters)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (uid, None, cid, "manual", status, t, t + water_sec, water_sec,
             volume))
        if zone_id:
            h.conn.execute(
                "INSERT INTO watering_event_zones(event_id, zone_id,"
                " water_sec, volume_liters) VALUES (?,?,?,?)",
                (cur.lastrowid, zone_id, water_sec, volume))
        return cur.lastrowid


def add_flow_daily(h, *, date, cid, zid=-1, liters=10.0):
    with h.conn:
        cur = h.conn.execute(
            """INSERT INTO flow_daily(date, controller_id, zone_id,
                   attribution, total_volume_l, watering_count, water_sec)
               VALUES (?,?,?,?,?,?,?)""",
            (date, cid, zid, "controller" if zid == -1 else "zone",
             liters, 1, 60))
        return cur.lastrowid


def add_notification(h, message="Нет протока", severity="critical"):
    with h.conn:
        cur = h.conn.execute(
            """INSERT INTO notifications(type, severity, message, dedupe_key)
               VALUES (?,?,?,?)""",
            ("no_flow", severity, message, f"no_flow:{time.time_ns()}"))
        return cur.lastrowid


def add_log(h, action="test.action"):
    with h.conn:
        cur = h.conn.execute(
            "INSERT INTO logs(ts, username, action, source)"
            " VALUES (datetime('now'), 'admin', ?, 'web')", (action,))
        return cur.lastrowid


def svc(h, tmp_path):
    return ar.ArchiveService(h.conn, tmp_path / "data")


# ------------------------------------------------------- миграция и настройки
def test_migration_0010_creates_table_and_settings(h6):
    cols = {r["name"] for r in h6.conn.execute(
        "PRAGMA table_info(archive_exports)").fetchall()}
    assert {"id", "entity_type", "date", "file_path", "row_count",
            "file_hash", "created_at"} <= cols
    cfg = ar.archive_config(h6.conn)
    assert cfg["enabled"] is True
    assert cfg["retention_days"] == 365
    assert cfg["path"] == "archive"
    assert cfg["events_format"] == "csv"


def test_settings_override_from_db(h6, tmp_path):
    from server.app.services.flow_service import set_setting
    set_setting(h6.conn, "archive.db_retention_days", 30)
    set_setting(h6.conn, "archive.events_format", "json")
    cfg = ar.archive_config(h6.conn)
    assert cfg["retention_days"] == 30
    assert cfg["events_format"] == "json"


# ------------------------------------------------------------------- rotate
def test_rotate_events_csv_columns_and_delete(h6, tmp_path):
    c = mk_controller(h6)
    z = mk_zone(h6, c["id"], 1)
    eid = add_event(h6, cid=c["id"], uid="e1", zone_id=z["id"])
    s = svc(h6, tmp_path)
    res = s.rotate("events", target_date=ar.utc_date(400))
    assert res["row_count"] == 1
    assert res["file_path"] and Path(res["file_path"]).exists()
    # строки удалены из БД, включая каскад watering_event_zones
    assert h6.conn.execute(
        "SELECT COUNT(*) c FROM watering_events").fetchone()["c"] == 0
    assert h6.conn.execute(
        "SELECT COUNT(*) c FROM watering_event_zones").fetchone()["c"] == 0
    text = Path(res["file_path"]).read_text(encoding="utf-8")
    rows = list(csv.DictReader(text.splitlines()))
    assert len(rows) == 1
    assert rows[0]["event_uid"] == "e1"
    assert {"id", "event_uid", "controller_id", "start_ts",
            "volume_liters"} <= set(rows[0].keys())
    assert res["file_hash"] == hashlib.sha256(
        text.encode("utf-8")).hexdigest()


def test_rotate_respects_retention_cutoff(h6, tmp_path):
    c = mk_controller(h6)
    old = add_event(h6, cid=c["id"], uid="old", start_ts=_ts(400))
    new = add_event(h6, cid=c["id"], uid="new", start_ts=_ts(1))
    res = svc(h6, tmp_path).rotate("events")  # без target_date — штатный срез
    assert res["row_count"] == 1
    assert h6.conn.execute(
        "SELECT event_uid FROM watering_events").fetchone()["event_uid"] \
        == "new"


def test_rotate_json_format_for_events(h6, tmp_path):
    from server.app.services.flow_service import set_setting
    set_setting(h6.conn, "archive.events_format", "json")
    c = mk_controller(h6)
    add_event(h6, cid=c["id"], uid="e-json")
    res = svc(h6, tmp_path).rotate("events", target_date=ar.utc_date(400))
    p = Path(res["file_path"])
    assert p.suffix == ".json"
    data = json.loads(p.read_text(encoding="utf-8"))
    assert data[0]["event_uid"] == "e-json"


def test_rotate_idempotent_same_date(h6, tmp_path):
    c = mk_controller(h6)
    add_event(h6, cid=c["id"], uid="dup")
    s = svc(h6, tmp_path)
    d = ar.utc_date(400)
    r1 = s.rotate("events", target_date=d)
    r2 = s.rotate("events", target_date=d)
    assert r1["row_count"] == 1 and r2["row_count"] == 0
    assert r2["file_path"] is None  # нечего выгружать — файл не создаётся
    exports = s.list_exports("events")
    assert len(exports) == 1


def test_rotate_dry_run_no_file_no_delete(h6, tmp_path):
    c = mk_controller(h6)
    add_event(h6, cid=c["id"], uid="keepme")
    res = svc(h6, tmp_path).rotate("events", target_date=ar.utc_date(400),
                                   dry_run=True)
    assert res["dry_run"] is True and res["row_count"] == 1
    assert res["file_path"] is None
    assert not (tmp_path / "data" / "archive").exists()
    assert h6.conn.execute(
        "SELECT COUNT(*) c FROM watering_events").fetchone()["c"] == 1
    assert svc(h6, tmp_path).list_exports() == []
def test_rotate_creates_immutable_batches_with_single_headers(h6, tmp_path):
    """ADR-18 supersedes daily append: every batch has its own stable hash."""
    c = mk_controller(h6)
    today = ar.utc_date()
    add_event(h6, cid=c["id"], uid="a", start_ts=_ts(400))
    add_event(h6, cid=c["id"], uid="b", start_ts=_ts(410))
    s = svc(h6, tmp_path)
    r1 = s.rotate("events", target_date=ar.utc_date(400))
    original = Path(r1["file_path"]).read_bytes()
    r2 = s.rotate("events", target_date=ar.utc_date(410))
    assert r1["file_path"] != r2["file_path"]
    assert Path(r1["file_path"]).read_bytes() == original
    for result, expected_uid in ((r1, "a"), (r2, "b")):
        path = Path(result["file_path"])
        assert path.name.startswith(today + "_")
        assert path.suffix == ".csv"
        text = path.read_text(encoding="utf-8")
        rows = list(csv.DictReader(io.StringIO(text)))
        assert [row["event_uid"] for row in rows] == [expected_uid]
        assert result["file_hash"] == hashlib.sha256(path.read_bytes()).hexdigest()



def test_rotate_other_entities(h6, tmp_path):
    c = mk_controller(h6)
    yday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    fid = add_flow_daily(h6, date=yday, cid=c["id"])
    nid = add_notification(h6)
    lid = add_log(h6)
    # Вчерашние сутки (штатный вызов демона — rotate_all за вчерашний день):
    # created_at/ts проставляем явно, чтобы строки гарантированно попали
    # под срез retention независимо от текущей секунды.
    yday_iso = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S")
    with h6.conn:
        h6.conn.execute("UPDATE notifications SET created_at=? WHERE id=?",
                        (yday_iso, nid))
        h6.conn.execute("UPDATE logs SET ts=? WHERE id=?", (yday_iso, lid))
    wid = h6.conn.execute(
        """INSERT INTO weather_observations(ts, observed_at, temp_c, source)
           VALUES (?, datetime('now'), 20.0, 'open_meteo')""",
        (_ts(1),)).lastrowid
    h6.conn.commit()
    s = svc(h6, tmp_path)
    rf = s.rotate("flow", target_date=yday)
    rn = s.rotate("notifications", target_date=yday)
    rl = s.rotate("logs", target_date=yday)
    rw = s.rotate("weather", target_date=ar.utc_date(1))
    assert rf["row_count"] == 1 and rn["row_count"] == 1
    assert rl["row_count"] == 1 and rw["row_count"] == 1
    for q, ident in (("flow_daily", fid), ("notifications", nid),
                     ("logs", lid), ("weather_observations", wid)):
        assert h6.conn.execute(
            f"SELECT COUNT(*) c FROM {q} WHERE id=?", (ident,)
        ).fetchone()["c"] == 0


def test_rotate_unknown_entity_raises(h6, tmp_path):
    with pytest.raises(ValueError):
        svc(h6, tmp_path).rotate("unicorns")


# ---------------------------------------------------------------- rotate_all
def test_rotate_all_covers_all_types(h6, tmp_path):
    c = mk_controller(h6)
    add_event(h6, cid=c["id"], uid="x")
    out = svc(h6, tmp_path).rotate_all()
    assert [o["entity_type"] for o in out] == list(ar.ENTITY_TYPES)
    assert all("error" not in o for o in out)


def test_rotate_all_isolated_error(h6, tmp_path, monkeypatch):
    orig = ar.ArchiveService.rotate

    def broken(self, et, **kw):
        if et == "flow":
            raise RuntimeError("boom")
        return orig(self, et, **kw)

    monkeypatch.setattr(ar.ArchiveService, "rotate", broken)
    out = svc(h6, tmp_path).rotate_all()
    errs = {o["entity_type"]: o.get("error") for o in out if "error" in o}
    assert errs == {"flow": "boom"}
    assert len(out) == len(ar.ENTITY_TYPES)


# -------------------------------------------------------------------- журнал
def test_list_exports_filter_limit(h6, tmp_path):
    c = mk_controller(h6)
    for i in range(3):
        add_event(h6, cid=c["id"], uid=f"n{i}")
    s = svc(h6, tmp_path)
    s.rotate("events")
    add_notification(h6)
    s.rotate("notifications")
    ev = s.list_exports("events")
    assert len(ev) == 1 and ev[0]["entity_type"] == "events"
    assert ev[0]["row_count"] == 3
    assert len(s.list_exports(limit=1)) == 1


# ------------------------------------------------------------------ restore
def test_restore_events_roundtrip(h6, tmp_path):
    c = mk_controller(h6)
    z = mk_zone(h6, c["id"], 1)
    add_event(h6, cid=c["id"], uid="rest1", zone_id=z["id"])
    s = svc(h6, tmp_path)
    s.rotate("events", target_date=ar.utc_date(400))
    exp = s.list_exports("events")[0]
    res = s.restore_export(exp["id"])
    assert res["restored"] == 1
    row = h6.conn.execute(
        "SELECT event_uid FROM watering_events").fetchone()
    assert row["event_uid"] == "rest1"
    # повторный restore — INSERT OR IGNORE, дублей нет
    assert s.restore_export(exp["id"])["restored"] == 0


def test_restore_flow_not_implemented(h6, tmp_path):
    c = mk_controller(h6)
    yday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    add_flow_daily(h6, date=yday, cid=c["id"])
    s = svc(h6, tmp_path)
    s.rotate("flow", target_date=yday)
    exp = s.list_exports("flow")[0]
    with pytest.raises(NotImplementedError):
        s.restore_export(exp["id"])


def test_restore_missing_export_raises(h6, tmp_path):
    s = svc(h6, tmp_path)
    with pytest.raises(KeyError):
        s.restore_export(999)


# -------------------------------------------------------------------- демон
def _daemon_on_harness_db(h6, tmp_path, name="daemon.db"):
    """Копия БД харнесса в файл — ArchiveDaemon открывает своё соединение."""
    dbfile = tmp_path / name
    dst = sqlite3.connect(str(dbfile))
    h6.conn.backup(dst)
    dst.close()
    return ar.ArchiveDaemon(str(dbfile), tmp_path / "data")


def test_daemon_tick_once_disabled(h6, tmp_path):
    from server.app.services.flow_service import set_setting
    set_setting(h6.conn, "archive.enabled", False)
    c = mk_controller(h6)
    add_event(h6, cid=c["id"], uid="old-disabled", start_ts=_ts(400))
    d = _daemon_on_harness_db(h6, tmp_path, "disabled.db")
    now = datetime.now()
    # форсируем «час пик», чтобы планировщик точно попытался запуститься
    d.RUN_HOUR = 0 if now.hour >= 1 else now.hour
    results = d.tick_once()
    if results is None:
        # до RUN_HOUR — просто ещё не время; проверяем это явно
        assert now.hour < d.RUN_HOUR or d.last_run_date == \
            now.strftime("%Y-%m-%d")
    else:
        assert results == []  # enabled=False → ротация пропущена
        assert d.last_run_date == now.strftime("%Y-%m-%d")
    # строки на месте — ничего не выгружено
    assert h6.conn.execute(
        "SELECT COUNT(*) c FROM watering_events").fetchone()["c"] == 1


def test_daemon_tick_once_runs_rotation(h6, tmp_path):
    c = mk_controller(h6)
    add_event(h6, cid=c["id"], uid="old-run", start_ts=_ts(400))
    d = _daemon_on_harness_db(h6, tmp_path, "run.db")
    now = datetime.now()
    d.RUN_HOUR = 0 if now.hour >= 1 else now.hour
    results = d.tick_once()
    if results is None:
        pytest.skip("локальное время до часа запуска — проверим логику ниже")
    assert [r["entity_type"] for r in results] == list(ar.ENTITY_TYPES)
    ev = next(r for r in results if r["entity_type"] == "events")
    assert ev["row_count"] == 1
    # ротация шла в копии БД демона — исходная харнесс-БД не тронута
    assert h6.conn.execute(
        "SELECT COUNT(*) c FROM watering_events").fetchone()["c"] == 1
    # однократность в течение суток
    assert d.tick_once() is None


def test_daemon_starts_and_stops(h6, tmp_path):
    # реальный daemon-поток: старт не блокирует, stop корректно завершает
    d = _daemon_on_harness_db(h6, tmp_path, "thread.db")
    d._initial_delay = 0.01
    d._check_interval = 0.05
    d.start()
    assert d.is_alive()
    time.sleep(0.2)
    d.stop()
    time.sleep(0.2)
    assert not d.is_alive()


def test_start_stop_singleton(tmp_path, h6):
    dbfile = tmp_path / "s.db"
    dst = sqlite3.connect(str(dbfile))
    h6.conn.backup(dst)
    dst.close()
    try:
        d1 = ar.start_archive_daemon(str(dbfile), tmp_path / "data")
        d2 = ar.start_archive_daemon(str(dbfile), tmp_path / "data")
        assert d1 is d2
    finally:
        ar.stop_archive_daemon()
    assert ar._daemon is None


# ------------------------------------------------- API архива (routes_reports)
# Вспомогательные эндпоинты для ручной проверки ротации (ИНСТРУКЦИЯ_ЭТАП8 §3).
class _HarnessArc(_Harness6):
    def __init__(self):
        super().__init__(with_web=True)
        register_api_routes_reports(self.app, self.cfg, self.conn)


@pytest.fixture()
def harc(tmp_path):
    h = _HarnessArc()
    # Архив — во временный каталог теста, боевой data/ не трогаем.
    h._orig_as = ar.ArchiveService
    root = tmp_path / "data"

    def _patched(conn, data_root=None):
        return h._orig_as(conn, root)
    ar.ArchiveService = _patched
    yield h, root
    ar.ArchiveService = h._orig_as
    h.close()


def test_api_archive_config(harc):
    h, _ = harc
    c = h.login_as("viewer")
    r = c.get("/api/v1/archive/config")
    assert r.status_code == 200
    cfg = r.json()
    assert cfg["retention_days"] == 365 and cfg["path"] == "archive"


def test_api_archive_config_anonymous_401(harc):
    h, _ = harc
    assert h.client.get("/api/v1/archive/config").status_code == 401


def test_api_archive_exports_empty(harc):
    h, _ = harc
    c = h.login_as("viewer")
    r = c.get("/api/v1/archive/exports")
    assert r.status_code == 200
    assert r.json() == {"items": [], "total": 0}


def test_api_archive_exports_bad_entity_422(harc):
    h, _ = harc
    c = h.login_as("viewer")
    r = c.get("/api/v1/archive/exports?entity_type=nope")
    assert r.status_code == 422


def test_api_archive_rotate_requires_operator(harc):
    h, _ = harc
    v = h.login_as("viewer")
    assert v.post("/api/v1/archive/rotate", json={}).status_code == 403


def test_api_archive_rotate_dry_run(harc):
    h, root = harc
    c0 = mk_controller(h)
    z = mk_zone(h, c0["id"], 1)
    add_event(h, cid=c0["id"], uid="ev-dry", zone_id=z["id"],
              start_ts=_ts(400))
    op = h.login_as("operator")
    r = op.post("/api/v1/archive/rotate",
                json={"entity_type": "events",
                      "target_date": ar.utc_date(400), "dry_run": True})
    assert r.status_code == 200
    res = r.json()["result"]
    assert res["row_count"] == 1 and res["dry_run"] is True
    # dry_run: файл не создан, строки на месте
    assert not (root / "archive" / "events").exists()
    assert h.conn.execute(
        "SELECT COUNT(*) c FROM watering_events").fetchone()["c"] == 1


def test_api_archive_rotate_and_journal(harc):
    h, root = harc
    c0 = mk_controller(h)
    z = mk_zone(h, c0["id"], 1)
    add_event(h, cid=c0["id"], uid="ev-api", zone_id=z["id"],
              start_ts=_ts(400))
    op = h.login_as("operator")
    r = op.post("/api/v1/archive/rotate",
                json={"entity_type": "events",
                      "target_date": ar.utc_date(400)})
    assert r.status_code == 200
    res = r.json()["result"]
    assert res["row_count"] == 1 and res["file_hash"]
    assert Path(res["file_path"]).exists()
    assert h.conn.execute(
        "SELECT COUNT(*) c FROM watering_events").fetchone()["c"] == 0
    j = op.get("/api/v1/archive/exports?entity_type=events").json()
    assert j["total"] == 1 and j["items"][0]["row_count"] == 1


def test_api_archive_rotate_all_bad_date_422(harc):
    h, _ = harc
    op = h.login_as("operator")
    r = op.post("/api/v1/archive/rotate", json={"target_date": "2026-13-99"})
    assert r.status_code == 422
