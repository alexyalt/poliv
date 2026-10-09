"""Тесты Этапа 3: базовый MQTT-обмен, сервис команд, API живого состояния.

Инфраструктура: FakeMQTT — мок paho-клиента (без реального брокера):
- publish() записывает (topic, payload, qos, retain);
- inject_incoming() прогоняет входящее сообщение через on_message клиента;
- ack_for() — хелпер ответа контроллера (command_ack).

Боевой TestClient не используется целиком из-за lifespan (он поднимал бы
реальный MqttServerClient на общей БД): здесь приложение собирается вручную
(create_app + ручной attach фейкового клиента), что даёт детерминированные
тесты без сети. Интеграция с реальным брокером — отдельный тест с skipif.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

os.environ.setdefault(
    "POLIV_CONFIG_LOCAL",
    str(Path(__file__).resolve().parents[2] / "config" / "config.local.toml"),
)

from fastapi.testclient import TestClient  # noqa: E402

from server.app.api.routes_stage2 import register_api_routes  # noqa: E402
from server.app.api.routes_stage3 import register_api_routes_stage3  # noqa: E402
from server.app.infra.config import load_config  # noqa: E402
from server.app.infra.db import init_db  # noqa: E402
from server.app.infra.mqtt_client import MqttServerClient  # noqa: E402
from server.app.services.auth_service import AuthService  # noqa: E402
from server.app.services.catalog_service import CatalogService  # noqa: E402
from server.app.services.mqtt_command_service import (  # noqa: E402
    ControllerOffline,
    MqttCommandService,
)
from server.app.services.user_service import UserService  # noqa: E402


# ------------------------------------------------------------------ FakeMQTT
class FakePahoClient:
    """Мок paho.Client: пишет публикации, умеет доставлять входящие сообщения."""

    def __init__(self):
        self.published: list[dict] = []
        self.subscriptions: list[tuple[str, int]] = []
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None
        self._lock = threading.Lock()

    # --- API paho, который использует серверный клиент ---------------------
    def username_pw_set(self, user, password=None):
        self.credentials = (user, password)

    def connect(self, host, port, keepalive=60):
        pass

    def loop_start(self):
        pass

    def loop_stop(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self, topic, qos=0):
        self.subscriptions.append((topic, qos))

    def publish(self, topic, payload, qos=0, retain=False):
        with self._lock:
            self.published.append({
                "topic": topic,
                "payload": json.loads(payload) if isinstance(payload, str) else payload,
                "qos": qos,
                "retain": retain,
            })

    # --- хелперы теста ------------------------------------------------------
    def inject_incoming(self, topic: str, payload: dict | str):
        class _Msg:
            pass
        m = _Msg()
        m.topic = topic
        m.payload = payload.encode("utf-8") if isinstance(payload, str) else \
            json.dumps(payload).encode("utf-8")
        self.on_message(None, None, m)

    def find(self, topic_prefix: str) -> list[dict]:
        with self._lock:
            return [p for p in self.published if p["topic"].startswith(topic_prefix)]

    def ack_for(self, command_payload: dict, status: str, message: str = ""):
        """Ответ контроллера на команду (topiс command_ack)."""
        box_id = command_payload["topic"].split("/")[1]
        self.inject_incoming(f"poliv/{box_id}/command_ack", {
            "protocol_version": "1.0",
            "box_id": box_id,
            "ts": time.time(),
            "command_id": command_payload["payload"]["command_id"],
            "command": command_payload["payload"]["command"],
            "status": status,
            "message": message,
        })


def _fast_cfg():
    cfg = load_config()
    cfg.raw.setdefault("mqtt", {}).update({
        "command_timeout_sec": 0.2,
        "command_retries": 2,
        "offline_threshold_min": 1,
    })
    return cfg


class _Harness:
    """Сборка приложения + MQTT-клиента на фейковом брокере, временная БД."""

    def __init__(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="poliv-st3-"))
        self.cfg = _fast_cfg()
        db_path = self.tmpdir / "test.db"
        self.conn = None
        # init_db привязан к cfg.db_path — временно подменяем путь
        orig_db_path = type(self.cfg).db_path
        try:
            type(self.cfg).db_path = property(lambda s: db_path)
            self.conn = init_db(self.cfg)
        finally:
            type(self.cfg).db_path = orig_db_path
        self.db_path = db_path

        self.auth = AuthService(self.conn, self.cfg)
        self.auth.ensure_admin()
        self.catalog = CatalogService(self.conn)
        self.users = UserService(self.conn, self.auth)
        self.cmd = MqttCommandService(self.cfg, self.conn)

        from fastapi import FastAPI
        app = FastAPI()
        app.state.cfg = self.cfg
        app.state.db = self.conn
        app.state.auth = self.auth
        app.state.catalog = self.catalog
        app.state.users = self.users
        app.state.command_service = self.cmd
        # Порядок регистрации как в create_app (main.py): stage3 ДО stage2,
        # иначе GET /api/controllers/live перехватывается маршрутом
        # /api/controllers/{controller_id} (422 на разбор int("live")).
        register_api_routes_stage3(app, self.cfg, self.conn, self.cmd)
        register_api_routes(app, self.cfg, self.conn, self.catalog, self.users)
        self.app = app
        self.client = TestClient(app)

        # MQTT: клиент с фейковым paho, БЕЗ фоновых потоков (детерминизм)
        self.fake = FakePahoClient()
        self.mqtt = MqttServerClient(self.cfg, client_factory=lambda: self.fake,
                                     db_path=str(db_path))
        self.mqtt.start()          # старт без побочных сетевых эффектов
        # фоновый maintenance-поток в тестах не нужен
        self.mqtt._stop.set()
        self.cmd.attach(self.mqtt)

    # ------------------------------------------------------------- helpers
    def controller(self, box_id: str = "BOX-T1") -> dict:
        return self.catalog.create_controller({"name": f"Бокс {box_id}",
                                               "box_id": box_id})

    def login_as(self, role: str) -> TestClient:
        """Клиент, залогиненный под пользователем заданной роли."""
        username = f"user_{role}"
        password = "test-pw-123"
        if not self.conn.execute("SELECT 1 FROM users WHERE username=?",
                                 (username,)).fetchone():
            self.users.create_user({"username": username, "password": password,
                                    "role": role}, actor="admin")
        token, user, msg = self.auth.login(username, password, "test-ip", "pytest")
        assert token, msg
        c = TestClient(self.app)
        c.cookies.set("poliv_session", token)
        return c

    def hello_payload(self, box_id: str, **extra) -> dict:
        p = {
            "protocol_version": "1.0",
            "box_id": box_id,
            "ts": time.time(),
            "online": True,
            "firmware_version": "sim-1.0.0",
            "mode": "idle",
            "phase": "idle",
            "active_zones": [],
            "buffer_size": 0,
            "flow_enabled": False,
            "service_mode": False,
            "schedule_version": 0,
        }
        p.update(extra)
        return p

    def status_payload(self, box_id: str, **extra) -> dict:
        p = self.hello_payload(box_id)
        p.pop("firmware_version", None)
        p.update(extra)
        return p

    def row(self, box_id: str) -> sqlite3.Row:
        return self.conn.execute(
            "SELECT * FROM controllers WHERE box_id=?", (box_id,)).fetchone()

    def logs(self, action: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM logs WHERE action=? ORDER BY id", (action,)).fetchall()

    def close(self):
        self.mqtt.stop()


# ===================================================================== БЛОК 8
# ---------------------------------------------------------------- 1. hello/status
def test_hello_marks_online_and_persists_fields():
    h = _Harness()
    h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload(
        "BOX-EMUL-01", mode="idle", firmware_version="fw-2.1", ip_address="10.0.0.5"))
    r = h.row("BOX-EMUL-01")
    assert r["connection_status"] == "online"
    assert r["last_seen_at"] and r["last_hello_at"]
    assert r["firmware_version"] == "fw-2.1"
    assert r["ip_address"] == "10.0.0.5"
    assert r["current_mode"] == "idle"
    # hello также порождает служебные публикации: time + schedule-заглушка v0
    times = h.fake.find("poliv/BOX-EMUL-01/time")
    assert times and times[-1]["payload"]["unix_ts"]
    scheds = h.fake.find("poliv/BOX-EMUL-01/schedule")
    assert scheds and scheds[-1]["payload"]["schedule_version"] == 0
    assert scheds[-1]["payload"]["runs"] == []
    # журнал: событие hello с source=mqtt
    assert any(l["source"] == "mqtt" for l in h.logs("controller.hello"))
    h.close()


def test_status_updates_mode_phase_zones_lastseen():
    h = _Harness()
    h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    before = h.row("BOX-EMUL-01")["last_seen_at"]
    h.fake.inject_incoming("poliv/BOX-EMUL-01/status", h.status_payload(
        "BOX-EMUL-01", mode="manual", phase="water", active_zones=[3, 4],
        display_zones=[3], flow_enabled=True, instant_lpm=12.5,
        flow_total_liters=42.0, buffer_size=7))
    r = h.row("BOX-EMUL-01")
    assert r["connection_status"] == "online"
    assert r["current_mode"] == "manual"
    assert r["current_phase"] == "water"
    assert json.loads(r["active_zones_json"]) == [3, 4]
    assert r["primary_zone"] == 3
    assert r["instant_lpm"] == 12.5
    assert r["flow_total_liters"] == 42.0
    assert r["last_seen_at"] >= before
    h.close()


# ------------------------------------------------------------------- 2. lwt/offline
def test_lwt_sets_offline():
    h = _Harness()
    h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    assert h.row("BOX-EMUL-01")["connection_status"] == "online"
    h.fake.inject_incoming("poliv/BOX-EMUL-01/lwt",
                           {"box_id": "BOX-EMUL-01", "online": False})
    assert h.row("BOX-EMUL-01")["connection_status"] == "offline"
    assert h.logs("controller.offline_lwt")
    h.close()


def test_offline_threshold_scan():
    """Нет статусов дольше offline_threshold_min (в харнесе = 1 минута) → offline."""
    h = _Harness()
    h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    # откатываем last_seen_at в прошлое на 5 минут
    stale = (datetime.now(timezone.utc) - timedelta(minutes=5)
             ).isoformat(timespec="seconds")
    with h.mqtt.conn:
        h.mqtt.conn.execute("UPDATE controllers SET last_seen_at=? WHERE box_id='BOX-EMUL-01'",
                            (stale,))
    changed = h.mqtt.mark_stale_offline()
    assert changed == 1
    assert h.row("BOX-EMUL-01")["connection_status"] == "offline"
    assert h.logs("controller.offline_threshold")
    h.close()


# ------------------------------------------------------------ 3. send_command/ack
def _capture_publish(fake: FakePahoClient):
    orig = fake.publish

    holder: dict = {}

    def wrapper(topic, payload, qos=0, retain=False):
        orig(topic, payload, qos=qos, retain=retain)
        parsed = json.loads(payload) if isinstance(payload, str) else payload
        if topic.endswith("/command"):
            holder["cmd"] = {"topic": topic, "payload": parsed, "qos": qos}

    fake.publish = wrapper
    return holder


def test_send_command_contract_qos1_and_completed_ack():
    h = _Harness()
    h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    holder = _capture_publish(h.fake)
    results: dict = {}

    def responder():
        for _ in range(200):
            cmd = holder.get("cmd")
            if cmd:
                results["cmd"] = cmd
                h.fake.ack_for(cmd, "accepted")
                h.fake.ack_for(cmd, "completed", "зона 2 открыта на 300 с")
                return
            time.sleep(0.01)
    t = threading.Thread(target=responder)
    t.start()
    res = h.cmd.send_command("BOX-EMUL-01", "zone_open",
                             {"zone": 2, "duration_sec": 300}, source="web")
    t.join(timeout=2)
    cmd = results["cmd"]
    assert cmd["topic"] == "poliv/BOX-EMUL-01/command"
    assert cmd["qos"] == h.cfg.mqtt_qos_command == 1
    p = cmd["payload"]
    for field in ("protocol_version", "command_id", "command", "ts", "source", "params"):
        assert field in p, f"нет контрактного поля {field}"
    assert p["command"] == "zone_open"
    assert p["params"] == {"zone": 2, "duration_sec": 300}
    assert res["status"] == "completed"
    assert res["attempts"] == 1
    # pending закрыт после финального ack
    assert not h.mqtt._pending
    assert h.logs("command.sent") and h.logs("command.acked")
    h.close()


def test_command_timeout_repeats_same_command_id():
    h = _Harness()
    h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    holder = _capture_publish(h.fake)
    # ack НЕ приходит никогда → таймауты и повторы
    res = h.cmd.send_command("BOX-EMUL-01", "ping", {}, source="web")
    assert res["status"] == "timeout"
    cmds = h.fake.find("poliv/BOX-EMUL-01/command")
    retries = h.cfg.mqtt_command_retries   # 2 в харнесе
    assert len(cmds) == 1 + retries        # первая попытка + повторы
    ids = {c["payload"]["command_id"] for c in cmds}
    assert len(ids) == 1, "повторы должны идти с ТЕМ ЖЕ command_id"
    assert res["attempts"] == 1 + retries
    assert len(h.logs("command.timeout")) >= retries
    h.close()


def test_duplicate_final_ack_ignored():
    """Второй финальный ack на ту же команду не ломает состояние pending."""
    h = _Harness()
    h.controller("BOX-EMUL-01")
    cid = "cid-dup-1"
    h.mqtt.register_pending(cid)
    ack = {"protocol_version": "1.0", "ts": time.time(), "command_id": cid,
           "command": "ping", "status": "completed", "message": "ok"}
    h.fake.inject_incoming("poliv/BOX-EMUL-01/command_ack", ack)
    entry = h.mqtt._pending[cid]
    assert entry["final"] and entry["status"] == "completed"
    # дублирующий ack поверх уже финализированной записи — без падения
    h.fake.inject_incoming("poliv/BOX-EMUL-01/command_ack",
                           {**ack, "status": "rejected"})
    assert h.mqtt._pending[cid]["status"] == "completed"
    # ack на неизвестную команду — игнор
    h.fake.inject_incoming("poliv/BOX-EMUL-01/command_ack",
                           {**ack, "command_id": "unknown-cid"})
    h.close()


# ----------------------------------------------------------- 4. offline → 409
def test_offline_controller_409_no_publish():
    h = _Harness()
    c = h.controller("BOX-OFF")
    admin = h.login_as("admin")
    n_before = len(h.fake.published)
    r = admin.post(f"/api/controllers/{c['id']}/actions/zone_open",
                   json={"params": {"zone": 1, "duration_sec": 60}})
    assert r.status_code == 409
    assert r.json()["detail"] == "controller_offline"
    assert len(h.fake.published) == n_before, "для офлайна публикаций быть не должно"
    rej = h.logs("command.rejected")
    assert rej and rej[-1]["object_id"] == "BOX-OFF"
    h.close()


# --------------------------------------------------- 5. невалидный JSON — warning
def test_invalid_payloads_do_not_crash_server():
    h = _Harness()
    h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    good_seen = h.row("BOX-EMUL-01")["last_seen_at"]
    # битый JSON
    h.fake.inject_incoming("poliv/BOX-EMUL-01/status", "{not json")
    # JSON-массив вместо объекта
    h.fake.inject_incoming("poliv/BOX-EMUL-01/status", "[1,2,3]")
    # объект без обязательных полей
    h.fake.inject_incoming("poliv/BOX-EMUL-01/status", {"foo": "bar"})
    # невалидное событие
    h.fake.inject_incoming("poliv/BOX-EMUL-01/event", {"protocol_version": "1.0"})
    # сообщение от неизвестного box_id
    h.fake.inject_incoming("poliv/UNKNOWN-BOX/status", h.status_payload("UNKNOWN-BOX"))
    # состояние не изменилось, сервер жив
    assert h.row("BOX-EMUL-01")["last_seen_at"] == good_seen
    r = h.login_as("viewer").get("/api/controllers/live")
    assert r.status_code == 200
    h.close()


# -------------------------------------------------------------- 6. роли API
def test_roles_viewer_403_operator_202():
    h = _Harness()
    c = h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    viewer = h.login_as("viewer")
    r = viewer.post(f"/api/controllers/{c['id']}/actions/ping", json={})
    assert r.status_code == 403
    # viewer может читать live
    assert viewer.get(f"/api/controllers/{c['id']}/live").status_code == 200

    holder = _capture_publish(h.fake)
    operator = h.login_as("operator")

    def responder():
        for _ in range(200):
            cmd = holder.get("cmd")
            if cmd:
                h.fake.ack_for(cmd, "accepted")
                h.fake.ack_for(cmd, "completed", "pong")
                return
            time.sleep(0.01)
    t = threading.Thread(target=responder)
    t.start()
    r = operator.post(f"/api/controllers/{c['id']}/actions/ping", json={})
    t.join(timeout=2)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "accepted"
    assert body["command_id"]
    assert body["ack_status"] == "completed"
    h.close()


def test_reboot_requires_admin():
    h = _Harness()
    c = h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    operator = h.login_as("operator")
    r = operator.post(f"/api/controllers/{c['id']}/actions/reboot", json={})
    assert r.status_code == 403  # reboot — только admin
    h.close()


def test_strict_param_validation_422():
    h = _Harness()
    c = h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/hello", h.hello_payload("BOX-EMUL-01"))
    admin = h.login_as("admin")
    bad_cases = [
        ("zone_open", {"zone": 0, "duration_sec": 10}),          # zone < 1
        ("zone_open", {"zone": 1}),                              # нет duration_sec
        ("zone_open", {"zone": "abc", "duration_sec": 10}),      # не число
        ("pause_controller", {}),                                # ни duration, ни until
        ("zone_open", {"zone": 1, "duration_sec": 5, "junk": 1}),  # лишнее поле
        ("nosuch_action", {}),                                   # неизвестное действие
    ]
    for action, params in bad_cases:
        r = admin.post(f"/api/controllers/{c['id']}/actions/{action}",
                       json={"params": params})
        assert r.status_code == 422, (action, params, r.text)
        assert "detail" in r.json()
    h.close()


# ------------------------------------------------------- live view model API
def test_live_view_model_fields():
    h = _Harness()
    c = h.controller("BOX-EMUL-01")
    h.fake.inject_incoming("poliv/BOX-EMUL-01/status", h.status_payload(
        "BOX-EMUL-01", mode="manual", phase="water", active_zones=[1],
        schedule_version=0))
    admin = h.login_as("admin")
    one = admin.get(f"/api/controllers/{c['id']}/live").json()
    for key in ("connection_status", "mode", "phase", "active_zones",
                "last_seen_at", "schedule_version"):
        assert key in one, key
    assert one["connection_status"] == "online"
    assert one["active_zones"] == [1]
    allr = admin.get("/api/controllers/live").json()
    assert isinstance(allr, list) and len(allr) == 1
    h.close()


# ----------------------------------------------- эмулятор: дедупликация команд
def test_emulator_deduplicates_command_ids():
    """Повтор команды с тем же command_id → ignored_duplicate без действия."""
    from emulator.controller_sim import ControllerSim

    sim = ControllerSim(box_id="BOX-SIM-01", status_interval=5)
    published: list[dict] = []

    class _FakePub:
        def publish(self, topic, payload, qos=0, retain=False):
            published.append({"topic": topic,
                              "payload": json.loads(payload)})
    sim._client = _FakePub()

    cmd = {"protocol_version": "1.0", "box_id": "BOX-SIM-01", "ts": time.time(),
           "command_id": "cid-1", "command": "zone_open",
           "params": {"zone": 1, "duration_sec": 60}}
    sim._on_command(cmd)
    statuses_first = [p["payload"]["status"] for p in published
                      if p["topic"].endswith("command_ack")]
    assert "accepted" in statuses_first
    # ждём отложенный completed (threading.Timer 0.3 с)
    deadline = time.time() + 2
    while time.time() < deadline and "completed" not in [
            p["payload"]["status"] for p in published
            if p["topic"].endswith("command_ack")]:
        time.sleep(0.05)
    assert sim.active_zones == [1] and sim.mode == "manual"
    published.clear()
    sim._on_command(cmd)  # повтор того же command_id
    rep = [p["payload"]["status"] for p in published
           if p["topic"].endswith("command_ack")]
    assert rep == ["ignored_duplicate"]
    # действие НЕ выполнено повторно: полив той же зоны не перезапущен
    assert sim.active_zones == [1]
    assert sim._current_event is not None  # событие старта не задублировано


# ---------------------------------------------------- 7. integration с брокером
_BROKER_HOST = os.environ.get("POLIV_TEST_MQTT_HOST")
_HAS_PAHO = True
try:
    import paho.mqtt.client as _mqtt_probe  # noqa: F401
except ImportError:
    _HAS_PAHO = False


def test_integration_real_broker():
    """Интеграция с реальным брокером: включается env POLIV_TEST_MQTT_HOST.

    Без брокера в окружении — пропускается (по заданию допустим mark.skipif;
    здесь эквивалентный явный pytest.skip, чтобы не зависеть от порядка env).
    """
    if not _BROKER_HOST or not _HAS_PAHO:
        import pytest
        pytest.skip("нет брокера в окружении (POLIV_TEST_MQTT_HOST)")
    import paho.mqtt.client as mqtt

    h = _Harness()
    c = h.controller("BOX-INT-01")
    # переключаем конфиг на реальный брокер и пересоздаём клиент
    h.cfg.raw["mqtt"]["host"] = _BROKER_HOST
    h.cfg.raw["mqtt"]["port"] = int(os.environ.get("POLIV_TEST_MQTT_PORT", "1883"))
    h.mqtt.stop()
    h.mqtt = MqttServerClient(h.cfg, db_path=str(h.db_path))
    h.mqtt.start()
    h.cmd.attach(h.mqtt)

    pub = mqtt.Client(client_id="pytest-controller")
    pub.connect(_BROKER_HOST, int(os.environ.get("POLIV_TEST_MQTT_PORT", "1883")))
    pub.loop_start()
    hello = h.hello_payload("BOX-INT-01", mode="idle")
    pub.publish("poliv/BOX-INT-01/hello", json.dumps(hello), qos=1)
    deadline = time.time() + 10
    online = False
    while time.time() < deadline:
        if h.row("BOX-INT-01")["connection_status"] == "online":
            online = True
            break
        time.sleep(0.2)
    pub.loop_stop()
    pub.disconnect()
    h.close()
    assert online, "сервер не увидел hello через реальный брокер"
