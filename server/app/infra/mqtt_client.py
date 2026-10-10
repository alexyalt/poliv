"""Серверный MQTT-клиент Этапа 3 (paho-mqtt, Артефакт 0.5).

Принципы:
- работает в ФОНОВОМ ПОТОКЕ (client.loop_start()), запуск/останов — явными
  вызовами из main.py (lifespan-обёртка); при импорте модуля побочных эффектов нет;
- СОБСТВЕННОЕ sqlite-соединение потока (короткие транзакции через `with conn:`),
  общее соединение FastAPI не трогается (check_same_thread этому потоку не нужен);
- входящие сообщения валидируются по контракту; невалидные → log.warning, сервер
  НЕ падает и не меняет состояние;
- события (event) на Этапе 3 только логируются в logs/mqtt.log и журнал БД
  (source=mqtt) — хранение в БД появится на Этапе 6; с Этапа 4 события полива
  дополнительно передаются в ScheduleService (журнал watering_runs);
- Этап 4: schedule_request/hello отвечают НАСТОЯЩЕЙ машинограммой через
  ScheduleService (компиляция + публикация); если сервис не подключён или
  компиляция невозможна — сохраняется ответ заглушкой v0 (деградация Этапа 3,
  контроллер не остаётся без ответа).

Топики подписки: poliv/+/hello, status, lwt, command_ack, schedule_ack, event, flow.
Публикации: poliv/{box_id}/time (при hello и раз в 6 ч), poliv/server/heartbeat
(retained, каждые 30 с), poliv/{box_id}/schedule (заглушка), poliv/{box_id}/command
(через mqtt_command_service).
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from ..infra.config import Config
from ..infra.logging import get_logger

log = get_logger("poliv.mqtt")

PROTOCOL_VERSION = "1.0"

# Входящие топики (Артефакт 0.5 §2.1)
SUBSCRIPTIONS = [
    ("poliv/+/hello", 1),
    ("poliv/+/status", 1),
    ("poliv/+/lwt", 1),
    ("poliv/+/command_ack", 1),
    ("poliv/+/schedule_ack", 1),
    ("poliv/+/event", 1),
    ("poliv/+/flow", 1),
    # Запрос машинограммы от контроллера: топик входит в контракт (§2.1),
    # поэтому подписываемся и отвечаем заглушкой Этапа 3 (см. _send_schedule_stub).
    ("poliv/+/schedule_request", 1),
]

VALID_MODES = {"idle", "schedule", "manual", "paused", "error", "service",
               "updating", "provisioning"}
VALID_PHASES = {"idle", "water", "soak", "waiting", "paused", "error"}

TIME_SYNC_PERIOD_SEC = 6 * 3600      # синхронизация времени раз в 6 часов (ТЗ §9.2)
HEARTBEAT_PERIOD_SEC = 30            # retained heartbeat сервера (блок 3 задания)
OFFLINE_SCAN_PERIOD_SEC = 60         # офлайн-порог проверяется раз в минуту


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _as_list_of_ints(value: Any) -> Optional[list[int]]:
    """active_zones/display_zones: список целых (bool отклоняется как подкласс int)."""
    if not isinstance(value, list):
        return None
    out: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int):
            return None
        out.append(item)
    return out


class MqttServerClient:
    """MQTT-клиент сервера: приём состояний контроллеров + инфраструктура ack."""

    def __init__(self, cfg: Config, client_factory=None, db_path: Optional[str] = None):
        self.cfg = cfg
        # Ожидание ack'ов команд (сервис команд опрашивает эти структуры):
        #   command_id -> {final: bool, status: str|None, message: str|None}
        self._pending: dict[str, dict[str, Any]] = {}
        self._pending_lock = threading.Lock()
        self._last_time_sync: dict[str, float] = {}
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._db_path = db_path or str(cfg.db_path)
        self._conn: Optional[sqlite3.Connection] = None
        self._factory = client_factory
        self.client = None  # paho-клиент или мок (в тестах)
        self.connected = False
        self._sched_seq = 0  # счётчик ответов schedule (для idempotency-логов)
        # Этап 4: сервис машинограмм (attach из main.py). Без него — деградация
        # к заглушке v0 (поведение Этапа 3).
        self.schedule_service = None
        # Этап 6: сервисы событий/расхода (attach из main.py). Без них —
        # поведение Этапа 3..5 (события только логируются, flow — live-поля).
        self.event_service = None
        self.flow_service = None

    def attach_schedule_service(self, svc) -> None:
        """Lifespan/DI: подключить ScheduleService для реальной рассылки."""
        self.schedule_service = svc

    def attach_stage6_services(self, event_service=None,
                               flow_service=None) -> None:
        """Lifespan/DI: подключить EventService/FlowService (Этап 6)."""
        if event_service is not None:
            self.event_service = event_service
        if flow_service is not None:
            self.flow_service = flow_service

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        """Подключение и запуск фоновых циклов. Идемпотентен."""
        if self.client is not None:
            return
        self.conn  # создаём СОБСТВЕННОЕ соединение этого потока обслуживания
        c = self._factory() if self._factory else self._make_paho_client()
        self.client = c
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        try:
            username = self.cfg.mqtt_username
            password = self.cfg.mqtt_password
            if username:
                c.username_pw_set(username, password or None)
            c.connect(self.cfg.mqtt_host, self.cfg.mqtt_port,
                      keepalive=self.cfg.mqtt_keepalive_sec)
        except Exception as exc:  # брокер может ещё не работать — не роняем сервер
            log.warning("MQTT: не удалось подключиться к брокеру %s:%s — %s. "
                        "Будут повторные попытки.", self.cfg.mqtt_host,
                        self.cfg.mqtt_port, exc)
            try:
                c.reconnect_async = True  # noqa: B011 (атрибут игнорируется paho)
            except Exception:
                pass
        c.loop_start()
        self._spawn(self._maintenance_loop, "mqtt-maintenance")
        log.info("MQTT-клиент сервера запущен (broker %s:%d)",
                 self.cfg.mqtt_host, self.cfg.mqtt_port)

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)
        if self.client is not None:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        log.info("MQTT-клиент сервера остановлен")

    def _spawn(self, target, name: str) -> None:
        t = threading.Thread(target=target, name=name, daemon=True)
        self._threads.append(t)
        t.start()

    def _make_paho_client(self):
        # Импорт ВНУТРИ метода: модуль можно импортировать без установленного paho
        # (тесты используют мок), и при импорте нет побочных эффектов.
        import paho.mqtt.client as mqtt

        try:
            return mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"poliv-server-{int(time.time())}",
                protocol=mqtt.MQTTv311,
            )
        except (AttributeError, TypeError):
            return mqtt.Client(client_id=f"poliv-server-{int(time.time())}")

    @property
    def conn(self) -> sqlite3.Connection:
        """Соединение SQLite для потока MQTT (своё, короткие транзакции)."""
        if self._conn is None:
            # check_same_thread=False: соединение создаётся в потоке create_app,
            # а сообщения paho обрабатываются в thread-loop'ах paho и maintenance-
            # потоке. Все операции сериализуются self._db_lock — гонок нет.
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode = WAL")
        return self._conn

    # ---------------------------------------------------------------- paho callbacks
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        # Совместимость CallbackAPIVersion VERSION2/VERSION1: subscription_add_*
        for topic, qos in SUBSCRIPTIONS:
            client.subscribe(topic, qos)
        self.connected = True
        log.info("MQTT: подключение к брокеру установлено (rc=%s), подписки активны",
                 reason_code)

    def _on_disconnect(self, client, userdata, *args):
        self.connected = False
        log.warning("MQTT: соединение с брокером потеряно")

    def _on_message(self, client, userdata, msg):
        try:
            parts = msg.topic.split("/")
            # poliv/{box_id}/{kind}
            if len(parts) != 3 or parts[0] != "poliv":
                return
            box_id, kind = parts[1], parts[2]
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                log.warning("MQTT: %s от %s — невалидный JSON (%s), пропущено",
                            kind, box_id, exc)
                return
            if not isinstance(payload, dict):
                log.warning("MQTT: %s от %s — JSON не объект, пропущено", kind, box_id)
                return
            handler = getattr(self, f"_handle_{kind}", None)
            if handler is None:
                log.warning("MQTT: неизвестный тип сообщения '%s' от %s", kind, box_id)
                return
            handler(box_id, payload)
        except Exception:  # любая ошибка обработки — сервер живёт дальше
            log.exception("MQTT: ошибка обработки %s (сообщение пропущено)",
                          getattr(msg, "topic", "?"))

    # ------------------------------------------------------------------ handlers
    def _touch_seen(self, box_id: str) -> None:
        # идемпотентно: event/flow не должны двигать last_seen_at — иначе
        # порог offline_threshold_min по status'ам никогда не срабатывает
        # (контроллер «жив» по flow даже без статусов).
        pass

    def _require_box(self, box_id: str) -> bool:
        row = self.conn.execute(
            "SELECT id FROM controllers WHERE box_id=?", (box_id,)
        ).fetchone()
        if row is None:
            log.warning("MQTT: сообщение от неизвестного box_id=%s — игнорируется",
                        box_id)
            return False
        return True

    def _validate_common(self, box_id: str, kind: str, p: dict,
                         required: tuple[str, ...]) -> bool:
        if p.get("box_id", box_id) != box_id:
            log.warning("MQTT: %s от %s — box_id в payload не совпадает (%s)",
                        kind, box_id, p.get("box_id"))
            return False
        # Этап 4 final (P1-1): aborted/buffered — булевы флаги; bool(False)
        # проходит проверку «наличие», но не проходит p.get(f) is None.
        missing = [f for f in required if p.get(f) is None]
        if missing:
            log.warning("MQTT: %s от %s — отсутствуют обязательные поля %s",
                        kind, box_id, missing)
            return False
        return True

    def _apply_state_fields(self, box_id: str, p: dict, *, hello: bool) -> None:
        """Общие поля hello/status: online, режим/фаза/зоны, версия, flow-поля."""
        sets: dict[str, Any] = {
            "connection_status": "online",
            "last_seen_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }
        mode = p.get("mode")
        if isinstance(mode, str) and mode in VALID_MODES:
            sets["current_mode"] = mode
        elif mode is not None:
            log.warning("MQTT: status/hello от %s — недопустимый mode=%r", box_id, mode)
        phase = p.get("phase")
        if isinstance(phase, str) and phase in VALID_PHASES:
            sets["current_phase"] = phase
        az = _as_list_of_ints(p.get("active_zones"))
        if az is not None:
            sets["active_zones_json"] = json.dumps(az)
            pz = p.get("primary_zone")
            if isinstance(pz, int) and not isinstance(pz, bool):
                sets["primary_zone"] = pz
            elif az:
                sets["primary_zone"] = az[0]
        dz = _as_list_of_ints(p.get("display_zones"))
        if dz is not None:
            sets["display_zones_json"] = json.dumps(dz)
        sv = p.get("schedule_version")
        if isinstance(sv, int) and not isinstance(sv, bool):
            sets["schedule_version"] = sv
        sh = p.get("schedule_hash")
        if isinstance(sh, str):
            sets["schedule_hash"] = sh
        tv = p.get("time_valid")
        if isinstance(tv, bool):
            sets["time_valid"] = 1 if tv else 0
        fw = p.get("firmware_version")
        if isinstance(fw, str):
            sets["firmware_version"] = fw
        fe = p.get("flow_enabled")
        if isinstance(fe, bool):
            sets["flow_enabled"] = 1 if fe else 0
        ft = p.get("flow_total_liters")
        if isinstance(ft, (int, float)) and not isinstance(ft, bool):
            sets["flow_total_liters"] = float(ft)
        il = p.get("instant_lpm")
        if isinstance(il, (int, float)) and not isinstance(il, bool):
            sets["instant_lpm"] = float(il)
        sm = p.get("service_mode")
        if isinstance(sm, bool):
            sets["service_mode_active"] = 1 if sm else 0
        for key in ("pause_until_ts", "emergency_lock_until_ts"):
            v = p.get(key)
            if isinstance(v, int) and not isinstance(v, bool):
                sets[key] = v
        # buffer_size — неотправленные события; на Этапе 3 принимаем и логируем,
        # хранилище событий — Этап 6.
        bs = p.get("buffer_size")
        if isinstance(bs, int) and not isinstance(bs, bool) and bs > 0:
            log.info("MQTT: %s buffer_size=%s (хранение событий — Этап 6)", box_id, bs)
        if hello:
            sets["last_hello_at"] = utcnow_iso()
            ip = p.get("ip_address") or p.get("ip")
            if isinstance(ip, str):
                sets["ip_address"] = ip

        cols = ", ".join(f"{k}=?" for k in sets)
        with self.conn:
            self.conn.execute(
                f"UPDATE controllers SET {cols} WHERE box_id=?",
                (*sets.values(), box_id),
            )

    def _handle_hello(self, box_id: str, p: dict) -> None:
        if not self._require_box(box_id):
            return
        if not self._validate_common(
            box_id, "hello", p, ("protocol_version", "ts", "online")
        ):
            return
        self._apply_state_fields(box_id, p, hello=True)
        self.write_log("controller.hello", box_id,
                       {"firmware_version": p.get("firmware_version")})
        log.info("MQTT: hello от %s — контроллер online", box_id)
        # Синхронизация времени при старте контроллера (ТЗ §9.2)
        self.publish_time(box_id, force=True)
        # Машинограмма при hello: Этап 4 — реальная компиляция/рассылка через
        # ScheduleService; без сервиса — деградация к заглушке v0 (Этап 3).
        self.send_schedule(box_id)

    def _handle_status(self, box_id: str, p: dict) -> None:
        if not self._require_box(box_id):
            return
        if not self._validate_common(
            box_id, "status", p,
            ("protocol_version", "ts", "box_id", "online", "mode", "phase",
             "active_zones", "buffer_size", "flow_enabled", "service_mode"),
        ):
            return
        if p.get("online") is not True:
            log.warning("MQTT: status от %s — online!=true, поле проигнорировано",
                        box_id)
        self._apply_state_fields(box_id, p, hello=False)

    def _handle_lwt(self, box_id: str, p: dict) -> None:
        # retained-последняя воля: online=false → offline (ТЗ §2.1/§2.2)
        if not self._require_box(box_id):
            return
        online = p.get("online", False)
        if online is not False:
            log.warning("MQTT: lwt от %s — unexpected online=%r, всё равно offline",
                        box_id, online)
        with self.conn:
            self.conn.execute(
                "UPDATE controllers SET connection_status='offline', updated_at=? "
                "WHERE box_id=?",
                (utcnow_iso(), box_id),
            )
        self.write_log("controller.offline_lwt", box_id, {})
        log.info("MQTT: lwt от %s — контроллер offline", box_id)

    def _handle_command_ack(self, box_id: str, p: dict) -> None:
        if not self._validate_common(
            box_id, "command_ack", p,
            ("protocol_version", "ts", "command_id", "command", "status"),
        ):
            return
        status = p.get("status")
        if status not in {"accepted", "completed", "rejected", "error",
                          "ignored_duplicate"}:
            log.warning("MQTT: command_ack от %s — неизвестный status=%r",
                        box_id, status)
        cid = p["command_id"]
        with self._pending_lock:
            entry = self._pending.get(cid)
            if entry is None:
                # Дублирующий/запоздалый ack на уже закрытую команду — игнорируем
                log.info("MQTT: command_ack %s от %s — команда не найдена среди "
                         "pending (дубликат или поздний ack), игнорируется",
                         cid[:8], box_id)
                return
            if status == "accepted":
                entry["ack_accepted"] = True
                return  # accepted — промежуточный, ждём completed/rejected/error
            if entry.get("final"):
                # Дублирующий ack поверх уже финализированной команды: первый
                # финальный статус канонический (§6.3), повтор игнорируется.
                log.info("MQTT: command_ack %s от %s — команда уже финализирована "
                         "(%s), дублирующий ack '%s' игнорируется",
                         cid[:8], box_id, entry.get("status"), status)
                return
            entry["final"] = True
            entry["status"] = status
            entry["message"] = p.get("message")
        self.write_log("command.acked", box_id,
                       {"command_id": cid, "command": p.get("command"),
                        "status": status}, source="mqtt")
        log.info("MQTT: command_ack %s от %s: %s", cid[:8], box_id, status)

    def _handle_schedule_ack(self, box_id: str, p: dict) -> None:
        if not self._validate_common(
            box_id, "schedule_ack", p,
            ("protocol_version", "ts", "schedule_version", "status"),
        ):
            return
        log.info("MQTT: schedule_ack от %s: v%s %s", box_id,
                 p.get("schedule_version"), p.get("status"))
        # Этап 4: sent→acknowledged/failed + watering_runs(planned) — сервис.
        svc = self.schedule_service
        if svc is not None:
            try:
                svc.on_schedule_ack(box_id, p)
            except Exception:
                log.exception("MQTT: ScheduleService.on_schedule_ack упал для %s",
                              box_id)

    def _validate_event_payload(self, box_id: str, p: dict) -> bool:
        """Этап 4 final (P1-1): раздельные схемы событий по статусу.

        started:  event_uid, source, start_ts, active_zones, buffered
                  (end_ts/water_sec/volume_liters НЕ требуются);
        finished/stopped: event_uid, source, start_ts, end_ts, water_sec,
                  volume_liters, active_zones, aborted, buffered.
        """
        status = p.get("status")
        if status == "started":
            required = ("protocol_version", "event_uid", "ts", "box_id",
                        "source", "start_ts", "active_zones", "buffered")
        elif status in ("finished", "stopped"):
            required = ("protocol_version", "event_uid", "ts", "box_id",
                        "source", "start_ts", "end_ts", "water_sec",
                        "volume_liters", "active_zones", "aborted", "buffered")
        else:
            log.warning("MQTT: event от %s — неизвестный статус %r",
                        box_id, status)
            return False
        return self._validate_common(box_id, "event", p, required)

    def _handle_event(self, box_id: str, p: dict) -> None:
        # Этап 3: события НЕ хранятся в БД (Этап 6) — только валидация,
        # лог logs/mqtt.log и журнал (source=mqtt).
        if not self._validate_event_payload(box_id, p):
            return
        if not isinstance(_as_list_of_ints(p.get("active_zones")), list):
            log.warning("MQTT: event от %s — active_zones не список целых", box_id)
            return
        self._touch_seen(box_id)
        log.info("MQTT: event от %s uid=%s type=%s water_sec=%s volume=%s "
                 "(в БД не сохраняется — Этап 6)", box_id, p.get("event_uid"),
                 p.get("status"), p.get("water_sec"), p.get("volume_liters"))
        self.write_log("controller.event", box_id,
                       {"event_uid": p.get("event_uid"), "status": p.get("status"),
                        "water_sec": p.get("water_sec"),
                        "volume_liters": p.get("volume_liters"),
                        "active_zones": p.get("active_zones")}, source="mqtt")
        # Этап 6: сохранение события в БД (дедупликация по event_uid, ТЗ §9.2)
        # + дневные агрегаты расхода + аварии/уведомления (EventService → FlowService).
        esvc = self.event_service
        if esvc is not None:
            try:
                esvc.handle_event(box_id, p)
            except Exception:
                log.exception("MQTT: EventService.handle_event упал для %s", box_id)
        # Этап 4: журнал прогонов watering_runs (план→факт) из событий полива.
        svc = self.schedule_service
        if svc is not None:
            try:
                svc.on_event(box_id, p)
            except Exception:
                log.exception("MQTT: ScheduleService.on_event упал для %s", box_id)

    def _handle_flow(self, box_id: str, p: dict) -> None:
        if not self._require_box(box_id):
            return
        if not self._validate_common(
            box_id, "flow", p,
            ("protocol_version", "ts", "box_id", "flow_enabled", "active_zones"),
        ):
            return
        sets = {"last_seen_at": utcnow_iso(), "updated_at": utcnow_iso()}
        fe = p.get("flow_enabled")
        if isinstance(fe, bool):
            sets["flow_enabled"] = 1 if fe else 0
        tl = p.get("total_liters")
        if isinstance(tl, (int, float)) and not isinstance(tl, bool):
            sets["flow_total_liters"] = float(tl)
        il = p.get("instant_lpm")
        if isinstance(il, (int, float)) and not isinstance(il, bool):
            sets["instant_lpm"] = float(il)
        cols = ", ".join(f"{k}=?" for k in sets)
        with self.conn:
            self.conn.execute(
                f"UPDATE controllers SET {cols} WHERE box_id=?",
                (*sets.values(), box_id),
            )
        # Этап 6: детекция аварий потока (no_flow/over_flow) во время полива.
        fsvc = self.flow_service
        if fsvc is not None:
            try:
                fsvc.handle_flow(box_id, p)
            except Exception:
                log.exception("MQTT: FlowService.handle_flow упал для %s", box_id)

    # ------------------------------------------------------------------ publish
    def publish(self, topic: str, payload: dict, qos: int = 1,
                retain: bool = False) -> None:
        if self.client is None:
            log.warning("MQTT: публикация в %s до старта клиента — пропущена", topic)
            return
        try:
            self.client.publish(topic, json.dumps(payload, ensure_ascii=False),
                                qos=qos, retain=retain)
        except Exception as exc:
            log.warning("MQTT: не удалось опубликовать %s: %s", topic, exc)

    def publish_time(self, box_id: str, force: bool = False) -> None:
        now = time.monotonic()
        last = self._last_time_sync.get(box_id, 0.0)
        if not force and now - last < TIME_SYNC_PERIOD_SEC:
            return
        self._last_time_sync[box_id] = now
        local = datetime.now().astimezone()
        offset_min = int(local.utcoffset().total_seconds() // 60) if local.utcoffset() else 0
        self.publish(f"poliv/{box_id}/time", {
            "protocol_version": PROTOCOL_VERSION,
            "ts": int(time.time()),
            "unix_ts": int(datetime.now(timezone.utc).timestamp()),
            "timezone_offset_min": offset_min,
            "source": "server",
        }, qos=0)
        log.debug("MQTT: time отправлен %s", box_id)

    def publish_heartbeat(self) -> None:
        self.publish("poliv/server/heartbeat", {
            "protocol_version": PROTOCOL_VERSION,
            "ts": int(time.time()),
            "online": True,
        }, qos=1, retain=True)

    def send_schedule(self, box_id: str) -> None:
        """Ответ контроллеру машинограммой (hello / schedule_request).

        Этап 4: если ScheduleService подключён — реальная компиляция и
        публикация актуальной версии (с учётом apply_policy). Иначе —
        деградация к заглушке v0 (поведение Этапа 3): контроллер не остаётся
        без ответа.
        """
        if not self._require_box(box_id):
            return
        svc = self.schedule_service
        if svc is not None:
            try:
                svc.on_hello(box_id)
                return
            except Exception:
                log.exception("MQTT: ScheduleService.on_hello упал для %s — "
                              "деградация к заглушке", box_id)
        self._send_schedule_stub(box_id)

    def send_schedule_stub(self, box_id: str) -> None:
        """Публичный вход для обработчика schedule_request (и тестов)."""
        if not self._require_box(box_id):
            return
        self._send_schedule_stub(box_id)

    def _handle_schedule_request(self, box_id: str, p: dict) -> None:
        # poliv/+/schedule_request не в списке подписок задания, но топик входит
        # в контракт (§2.1) — поддерживаем, если брокер доставит.
        self.send_schedule(box_id)

    def _send_schedule_stub(self, box_id: str) -> None:
        """Заглушка Этапа 3 (деградация): пустая машинограмма v0."""
        today = date.today()
        self._sched_seq += 1
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "box_id": box_id,
            "schedule_version": 0,
            "schedule_hash": f"stub-{box_id}-{self._sched_seq}",
            "valid_from_date": today.isoformat(),
            "valid_to_date": (today + timedelta(days=7)).isoformat(),
            "timezone_offset_min": 0,
            "generated_ts": int(time.time()),
            "source": "auto",
            "runs": [],
        }
        self.publish(f"poliv/{box_id}/schedule", payload,
                     qos=self.cfg.mqtt_qos_command)
        log.info("MQTT: schedule (заглушка Этапа 3, v0, runs=[]) отправлен %s",
                 box_id)

    # ------------------------------------------------------------- pending ack API
    def register_pending(self, command_id: str) -> dict[str, Any]:
        entry = {"final": False, "status": None, "message": None,
                 "ack_accepted": False}
        with self._pending_lock:
            self._pending[command_id] = entry
        return entry

    def unregister_pending(self, command_id: str) -> None:
        with self._pending_lock:
            self._pending.pop(command_id, None)

    # ---------------------------------------------------------------- maintenance
    def _maintenance_loop(self) -> None:
        next_heartbeat = 0.0
        next_scan = 0.0
        while not self._stop.is_set():
            now = time.monotonic()
            if now >= next_heartbeat:
                self.publish_heartbeat()
                next_heartbeat = now + HEARTBEAT_PERIOD_SEC
            if now >= next_scan:
                try:
                    self.mark_stale_offline()
                    self.resync_times()
                except Exception:
                    log.exception("MQTT: ошибка фоновой проверки статусов")
                next_scan = now + OFFLINE_SCAN_PERIOD_SEC
            self._stop.wait(1.0)

    def mark_stale_offline(self) -> int:
        """Офлайн по порогу offline_threshold_min (нет сообщений дольше порога)."""
        threshold_min = self.cfg.mqtt_offline_threshold_min
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(minutes=threshold_min)).isoformat(timespec="seconds")
        rows = self.conn.execute(
            "SELECT box_id FROM controllers WHERE connection_status='online' "
            "AND (last_seen_at IS NULL OR last_seen_at < ?)", (cutoff,)
        ).fetchall()
        count = 0
        for r in rows:
            with self.conn:
                self.conn.execute(
                    "UPDATE controllers SET connection_status='offline', updated_at=? "
                    "WHERE box_id=?", (utcnow_iso(), r["box_id"]),
                )
            self.write_log("controller.offline_threshold", r["box_id"],
                           {"cutoff": cutoff}, source="mqtt")
            log.info("MQTT: %s offline по порогу (%d мин без статусов)",
                     r["box_id"], threshold_min)
            count += 1
        return count

    def resync_times(self) -> None:
        """Периодическая синхронизация времени online-контроллеров (раз в 6 ч)."""
        rows = self.conn.execute(
            "SELECT box_id FROM controllers WHERE connection_status='online'"
        ).fetchall()
        for r in rows:
            self.publish_time(r["box_id"])

    # ----------------------------------------------------------------------- logs
    def write_log(self, action: str, box_id: str, details: dict,
                  source: str = "mqtt") -> None:
        """Журнал действий (таблица logs) собственным соединением, source=mqtt."""
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO logs(ts, username, action, object_type, object_id, "
                    "details_json, source) VALUES (?, ?, ?, 'controller', ?, ?, ?)",
                    (utcnow_iso(), None, action, box_id,
                     json.dumps(details, ensure_ascii=False), source),
                )
        except Exception:
            log.exception("MQTT: не удалось запись в журнал (action=%s)", action)


_instance: Optional[MqttServerClient] = None
_instance_lock = threading.Lock()


def get_mqtt_client() -> Optional[MqttServerClient]:
    """Текущий экземпляр (или None, если MQTT не запущен)."""
    return _instance


def start_mqtt(cfg: Config, client_factory=None, db_path: Optional[str] = None,
               command_service=None, schedule_service=None,
               event_service=None, flow_service=None) -> MqttServerClient:
    """Lifespan-подобный старт: вызывается из main.py, НЕ при импорте модуля."""
    global _instance
    with _instance_lock:
        if _instance is not None:
            return _instance
        inst = MqttServerClient(cfg, client_factory=client_factory, db_path=db_path)
        if schedule_service is not None:
            inst.attach_schedule_service(schedule_service)
        inst.attach_stage6_services(event_service=event_service,
                                    flow_service=flow_service)
        inst.start()
        if command_service is not None:
            command_service.attach(inst)
        _instance = inst
        return inst


def stop_mqtt() -> None:
    global _instance
    with _instance_lock:
        if _instance is not None:
            _instance.stop()
            _instance = None
