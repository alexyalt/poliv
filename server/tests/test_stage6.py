"""Тесты Этапа 6: события полива, мониторинг расхода, уведомления.

Инфраструктура переиспользуется из test_stage3 (_Harness с FakeMQTT, временной
БД, login_as). Отличие от рекомендации qwen_chat/stage6_recomendations: в этом
проекте нет фикстуры `h` и модуля routes_stage6 c /api/stage6/* — реальные
эндпоинты зарегистрированы как /api/events, /api/flow/*, /api/notifications,
/api/controllers/{id}/clear-error (см. server/app/api/routes_stage6.py), а
сервисы — синглтоны с init_*/get_* (event/flow/notification).

Проверяемые контракты (ТЗ Этап 6):
- EventService: сохранение события, дедупликация по event_uid (ТЗ §9.2),
  отбрасывание неизвестного статуса;
- FlowService: дневные агрегаты flow_daily по finished-событию, авария по
  reason_code → emergency_lock + critical-уведомление;
- MQTT-пайплайн: poliv/+/event → watering_events, poliv/+/flow → live-поля +
  детекция no_flow;
- REST API: роли (viewer читает, неавторизованный 401, viewer без права на
  clear-error 403), пагинация событий, CSV-экспорт, счётчик unread,
  mark_read/read-all, идемпотентность clear-error;
- NotificationService: дедупликация по dedupe_key.
"""
from __future__ import annotations

import time

import pytest

from server.app.api.routes_stage6 import register_api_routes_stage6
from server.app.services.event_service import (
    init_event_service, set_event_service)
from server.app.services.flow_service import (
    init_flow_service, set_flow_service, set_setting, get_setting)
from server.app.services.notification_service import (
    init_notification_service, set_notification_service)
from server.tests.test_stage3 import _Harness


class _Harness6(_Harness):
    """_Harness Этапа 3 + сервисы и API Этапа 6 (порядок как в create_app)."""

    def __init__(self):
        super().__init__()
        # Сервисы-синглтоны поверх той же conn, что и у приложения/MQTT.
        self.events = init_event_service(self.conn)
        self.flow = init_flow_service(self.conn)
        self.notifications = init_notification_service(self.conn)
        # MQTT-клиент подключает event/flow-обработчики (как lifespan в main).
        self.mqtt.attach_stage6_services(event_service=self.events,
                                         flow_service=self.flow)
        # API Этапа 6 — после stage3/stage2? Порядок: как в create_app
        # (свои пути /api/events, /api/flow/*, /api/notifications — конфликтов
        # с ранее зарегистрированными нет).
        register_api_routes_stage6(self.app, self.cfg, self.conn)

    def close(self):
        super().close()
        # Синглтоны не должны «протекать» между тестами (conn уже закрыта).
        set_event_service(None)
        set_flow_service(None)
        set_notification_service(None)

    # ------------------------------------------------------------ helpers
    def add_zone(self, controller_id: int, zone_number: int,
                 expected_flow_lpm: float | None = None) -> dict:
        z = self.catalog.create_zone({
            "controller_id": controller_id, "zone_number": zone_number,
            "name": f"Зона {zone_number}", "base_duration_minutes": 10})
        if expected_flow_lpm is not None:
            with self.conn:
                self.conn.execute(
                    "UPDATE zones SET expected_flow_lpm=? WHERE id=?",
                    (expected_flow_lpm, z["id"]))
        return z

    def event_payload(self, box_id: str, status: str, **extra) -> dict:
        now = int(time.time())
        p = {
            "protocol_version": "1.0",
            "box_id": box_id,
            "ts": time.time(),
            "status": status,
            "event_uid": extra.pop("event_uid", f"ev-{box_id}-{status}-{now}"),
            "source": "schedule",
            "start_ts": now - 600,
            "active_zones": [1],
            "buffered": False,
        }
        if status in ("finished", "stopped"):
            p.update({"end_ts": now, "water_sec": 600, "volume_liters": 50.0,
                      "aborted": status == "stopped"})
        p.update(extra)
        return p

    def flow_payload(self, box_id: str, **extra) -> dict:
        p = {
            "protocol_version": "1.0",
            "box_id": box_id,
            "ts": time.time(),
            "flow_enabled": True,
            "active_zones": [1],
            "total_liters": 10.0,
            "instant_lpm": 8.0,
        }
        p.update(extra)
        return p

    def inject_event(self, box_id: str, p: dict):
        self.fake.inject_incoming(f"poliv/{box_id}/event", p)

    def inject_flow(self, box_id: str, p: dict):
        self.fake.inject_incoming(f"poliv/{box_id}/flow", p)

    def events_rows(self):
        return self.conn.execute(
            "SELECT * FROM watering_events ORDER BY id").fetchall()

    def daily_rows(self):
        return self.conn.execute(
            "SELECT * FROM flow_daily ORDER BY id").fetchall()

    def notif_rows(self):
        return self.conn.execute(
            "SELECT * FROM notifications ORDER BY id").fetchall()


@pytest.fixture()
def h6():
    h = _Harness6()
    try:
        yield h
    finally:
        h.close()


# ============================================================ Блок 1: сервисы
def test_event_saved_and_dedup_by_uid(h6):
    """Событие сохраняется; повтор с тем же event_uid игнорируется (§9.2)."""
    c = h6.controller("BOX-E1")
    uid = "uid-dup-1"
    eid = h6.events.handle_event("BOX-E1",
                                 h6.event_payload("BOX-E1", "started",
                                                  event_uid=uid))
    assert eid is not None
    dup = h6.events.handle_event("BOX-E1",
                                 h6.event_payload("BOX-E1", "started",
                                                  event_uid=uid))
    assert dup is None
    assert len(h6.events_rows()) == 1
    row = h6.events_rows()[0]
    assert row["controller_id"] == c["id"]
    assert row["status"] == "started"
    assert row["source"] == "schedule"


def test_event_unknown_status_and_unknown_box_dropped(h6):
    h6.controller("BOX-E2")
    bad = h6.event_payload("BOX-E2", "started")
    bad["status"] = "paused"                      # вне EVENT_STATUSES
    assert h6.events.handle_event("BOX-E2", bad) is None
    ok = h6.event_payload("BOX-E2", "finished")
    assert h6.events.handle_event("BOX-NOPE", ok) is None   # чужой box_id
    assert len(h6.events_rows()) == 0


def test_finished_event_creates_daily_aggregate(h6):
    """finished-событие → запись flow_daily (атрибуция по зоне)."""
    c = h6.controller("BOX-D1")
    h6.add_zone(c["id"], 1, expected_flow_lpm=8.0)
    ev = h6.event_payload("BOX-D1", "finished", volume_liters=42.0)
    assert h6.events.handle_event("BOX-D1", ev) is not None
    rows = h6.daily_rows()
    assert len(rows) == 1
    r = rows[0]
    assert r["attribution"] == "zone"
    assert abs(r["total_volume_l"] - 42.0) < 1e-6
    assert r["watering_count"] == 1


def test_emergency_reason_locks_and_notifies(h6):
    """reason_code=no_flow у stopped-события → блокировка + critical-уведомление."""
    c = h6.controller("BOX-M1")
    ev = h6.event_payload("BOX-M1", "stopped", reason_code="no_flow",
                          aborted=True)
    assert h6.events.handle_event("BOX-M1", ev) is not None
    row = h6.row("BOX-M1")
    lock = row["emergency_lock_until_ts"]
    assert lock is not None and lock > int(time.time())
    assert h6.flow.is_emergency_locked(c["id"])
    n = h6.notif_rows()
    assert len(n) == 1
    assert n[0]["type"] == "flow_emergency"
    assert n[0]["severity"] == "critical"
    # повтор того же события не создаёт второй аварии (dedupe по event_uid)
    assert h6.events.handle_event("BOX-M1", ev) is None
    assert len(h6.notif_rows()) == 1


def test_notification_dedupe_key(h6):
    a = h6.notifications.create(type="system", severity="info",
                                message="тест", dedupe_key="k-1")
    b = h6.notifications.create(type="system", severity="info",
                                message="тест-дубль", dedupe_key="k-1")
    assert a == b and a is not None
    assert len(h6.notif_rows()) == 1


# ======================================================= Блок 2: MQTT-пайплайн
def test_mqtt_event_topic_persists_event(h6):
    """poliv/+/event через FakeMQTT → строка в watering_events."""
    c = h6.controller("BOX-Q1")
    h6.inject_event("BOX-Q1", h6.event_payload("BOX-Q1", "started"))
    rows = h6.events_rows()
    assert len(rows) == 1
    assert rows[0]["controller_id"] == c["id"]


def test_mqtt_flow_updates_live_and_detects_no_flow(h6):
    """poliv/+/flow: live-поля обновляются; нулевой поток во время полива
    дольше задержки → авария no_flow (детерминированно: delay=0)."""
    c = h6.controller("BOX-Q2")
    h6.add_zone(c["id"], 1, expected_flow_lpm=8.0)
    # полив «идёт»: активная зона в live-состоянии
    with h6.conn:
        h6.conn.execute(
            "UPDATE controllers SET active_zones_json='[1]', current_mode="
            "'watering' WHERE box_id='BOX-Q2'")
    set_setting(h6.conn, "flow.no_flow_delay_sec", 0)
    h6.flow._no_flow_watch.clear()
    # первый кадр: поток есть — ничего не происходит
    h6.inject_flow("BOX-Q2", h6.flow_payload("BOX-Q2", instant_lpm=7.5,
                                             total_liters=7.5))
    r = h6.row("BOX-Q2")
    assert abs(r["instant_lpm"] - 7.5) < 1e-6
    assert r["flow_total_liters"] == 7.5
    assert h6.notif_rows() == []
    # нулевой поток → наблюдение; второй кадр после delay=0 → авария
    h6.inject_flow("BOX-Q2", h6.flow_payload("BOX-Q2", instant_lpm=0.0))
    h6.inject_flow("BOX-Q2", h6.flow_payload("BOX-Q2", instant_lpm=0.0))
    assert h6.flow.is_emergency_locked(c["id"])
    n = h6.notif_rows()
    assert len(n) == 1 and n[0]["severity"] == "critical"


# ============================================================= Блок 3: REST API
def test_api_requires_auth_and_roles(h6):
    r = h6.client.get("/api/events")                   # без сессии
    assert r.status_code == 401
    viewer = h6.login_as("viewer")
    c = h6.controller("BOX-A1")
    h6.inject_event("BOX-A1", h6.event_payload("BOX-A1", "started"))
    r = viewer.get("/api/events")
    assert r.status_code == 200
    body = r.json()
    assert body["total_items"] == 1
    assert body["items"][0]["status"] == "started"
    # viewer не может сбрасывать аварию — 403 (operator+)
    r = viewer.post(f"/api/controllers/{c['id']}/clear-error", json={})
    assert r.status_code == 403


def test_api_events_filters_pagination_csv(h6):
    viewer = h6.login_as("viewer")
    c = h6.controller("BOX-A2")
    for i in range(3):
        h6.inject_event("BOX-A2", h6.event_payload(
            "BOX-A2", "finished", event_uid=f"pg-{i}", volume_liters=10.0))
    r = viewer.get("/api/events?page=1&page_size=2&controller_id=%d" % c["id"])
    assert r.status_code == 200
    body = r.json()
    assert body["page_size"] == 2 and body["total_items"] == 3
    assert body["total_pages"] == 2
    r = viewer.get("/api/events?status=started")
    assert r.json()["total_items"] == 0
    r = viewer.get("/api/events/export.csv")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "event_uid" in r.text and r.text.count("\n") >= 4  # заголовок+3


def test_api_flow_summary_daily_settings(h6):
    viewer = h6.login_as("viewer")
    c = h6.controller("BOX-A3")
    h6.add_zone(c["id"], 1, expected_flow_lpm=8.0)
    h6.inject_event("BOX-A3", h6.event_payload("BOX-A3", "finished",
                                               volume_liters=30.0))
    r = viewer.get("/api/flow/daily")
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1 and items[0]["controller_name"]
    r = viewer.get("/api/flow/summary")
    assert r.status_code == 200
    # контракт summary(): volume_l / runs / water_sec / anomalies
    s = r.json()
    assert float(s["volume_l"]) == pytest.approx(30.0)
    assert s["runs"] == 1 and s["anomalies"] == 0
    r = viewer.get("/api/flow/settings")
    assert r.status_code == 200
    th = r.json()
    assert "no_flow_delay_sec" in th and "overflow_percent" in th
    assert "sound_enabled" in th


def test_api_notifications_lifecycle(h6):
    viewer = h6.login_as("viewer")
    c = h6.controller("BOX-A4")
    nid = h6.notifications.create(type="flow_emergency", severity="critical",
                                  message="нет потока", controller_id=c["id"])
    r = viewer.get("/api/notifications/unread-count")
    assert r.json()["unread_count"] == 1
    assert r.json()["max_unread_critical_id"] == nid
    r = viewer.get("/api/notifications")
    assert r.json()["items"][0]["message"] == "нет потока"
    r = viewer.post(f"/api/notifications/{nid}/read")
    assert r.json()["status"] == "ok"
    assert viewer.get("/api/notifications/unread-count").json()["unread_count"] == 0
    # read-all идемпотентен
    h6.notifications.create(type="system", severity="info", message="ещё")
    r = viewer.post("/api/notifications/read-all")
    assert r.status_code == 200 and r.json()["marked"] >= 1
    assert viewer.post("/api/notifications/read-all").json()["marked"] == 0


def test_api_clear_error_operator(h6):
    operator = h6.login_as("operator")
    c = h6.controller("BOX-A5")
    # активируем аварию через событие (путь сервиса)
    h6.inject_event("BOX-A5", h6.event_payload("BOX-A5", "stopped",
                                               reason_code="over_flow",
                                               aborted=True))
    assert h6.flow.is_emergency_locked(c["id"])
    r = operator.post(f"/api/controllers/{c['id']}/clear-error",
                      json={"reason": "проверка"})
    assert r.status_code == 200
    assert r.json()["cleared"] is True
    assert not h6.flow.is_emergency_locked(c["id"])
    assert h6.row("BOX-A5")["emergency_lock_until_ts"] is None
    # уведомления аварии помечены разрешёнными
    n = h6.notif_rows()[0]
    assert n["resolved_at"] is not None
    # повторный сброс — cleared=False (уже чисто), но не ошибка
    r = operator.post(f"/api/controllers/{c['id']}/clear-error", json={})
    assert r.status_code == 200 and r.json()["cleared"] is False
    # чужого контроллера нет — 404
    assert operator.post("/api/controllers/999999/clear-error",
                         json={}).status_code == 404
