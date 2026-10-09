"""Сервис команд MQTT (Этап 3, Блок 4; контракт — Артефакт 0.5 §5–6).

Отправка команд контроллеру:
- command_id = uuid4; публикация в poliv/{box_id}/command с QoS [mqtt].qos_command;
- общие поля команды (§5.1): protocol_version, command_id, command, ts, source,
  user_id (опц.), params;
- ожидание command_ack: command_timeout_sec, повторы command_retries раз с
  ТЕМ ЖЕ command_id (§6.2); статусы ack: accepted/completed/rejected/error/
  ignored_duplicate (§6.1);
- контроллер offline -> исключение ControllerOffline (API отдаёт 409
  controller_offline), команда НЕ ставится в очередь (очередь — позже),
  факт пишется в журнал logs;
- идемпотентность на стороне контроллера проверяет эмулятор (дедупликация
  по command_id);
- журнал logs: command.sent, command.acked, command.timeout, command.rejected.

Сервис не создаёт MQTT-клиент сам: клиент (MqttServerClient) подключается через
attach() при старте приложения (main.py), либо инжектируется в тестах.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from ..infra.config import Config
from ..infra.logging import get_logger

log = get_logger("poliv.mqtt.cmd")

PROTOCOL_VERSION = "1.0"

# Команды Этапа 3 (список — Артефакт 0.5 §5.2; полные контракты параметров — §5.3..§5.12)
COMMAND_NAMES = {
    "ping", "zone_open", "zone_close", "stop_all",
    "pause_controller", "resume_controller", "reboot",
}

ACK_FINAL_STATUSES = {"completed", "rejected", "error", "ignored_duplicate"}


class CommandError(ValueError):
    """Ошибка валидации команды (API: 422)."""


class ControllerOffline(RuntimeError):
    """Контроллер офлайн: команду не отправляем (API: 409 controller_offline)."""

    def __init__(self, box_id: str):
        super().__init__(f"Контроллер {box_id} офлайн — команда не отправлена")
        self.box_id = box_id


class MqttUnavailable(RuntimeError):
    """MQTT-брокер не подключён на сервере (API: 503 mqtt_unavailable)."""


def _parse_int_strict(value: Any, name: str, minimum: int) -> int:
    if isinstance(value, bool):
        raise CommandError(f"Поле «{name}» должно быть целым числом")
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        raise CommandError(f"Ожидается целое число: поле «{name}»") from None
    if isinstance(value, float) and not value.is_integer():
        raise CommandError(f"Ожидается целое число: поле «{name}»")
    if ivalue < minimum:
        raise CommandError(f"Поле «{name}» должно быть >= {minimum}")
    return ivalue


def _parse_optional_int(value: Any, name: str, minimum: int) -> Optional[int]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return _parse_int_strict(value, name, minimum)


def _clean_reason(value: Any, name: str = "reason") -> str:
    text = "" if value is None else str(value).strip()
    if len(text) > 200:
        raise CommandError(f"Поле «{name}» длиннее 200 символов")
    return text


def validate_command_params(command: str, params: dict) -> dict:
    """Строгая валидация параметров под контракт §5.3–§5.12. Возвращает чистые params."""
    if not isinstance(params, dict):
        raise CommandError("params должен быть JSON-объектом")
    cleaned: dict[str, Any] = {}
    if command == "ping":
        pass  # параметров нет
    elif command == "zone_open":
        cleaned["zone"] = _parse_int_strict(params.get("zone"), "zone", 1)
        cleaned["duration_sec"] = _parse_int_strict(
            params.get("duration_sec"), "duration_sec", 1)
        mm = params.get("manual_mode")
        if mm is not None:
            if mm not in ("pause_resume", "replace_current"):
                raise CommandError(
                    "manual_mode: допустимы pause_resume / replace_current")
            cleaned["manual_mode"] = mm
        for flag in ("allow_during_pause", "override_block"):
            if flag in params:
                if not isinstance(params[flag], bool):
                    raise CommandError(f"Поле «{flag}» должно быть булевым")
                cleaned[flag] = params[flag]
    elif command == "zone_close":
        cleaned["zone"] = _parse_int_strict(params.get("zone"), "zone", 1)
    elif command == "stop_all":
        cleaned["reason"] = _clean_reason(params.get("reason"))
        if "abort_run" in params:
            if not isinstance(params["abort_run"], bool):
                raise CommandError("Поле «abort_run» должно быть булевым")
            cleaned["abort_run"] = params["abort_run"]
        put = _parse_optional_int(params.get("pause_until_ts"), "pause_until_ts", 0)
        if put is not None:
            cleaned["pause_until_ts"] = put
    elif command == "pause_controller":
        duration = _parse_optional_int(params.get("duration_sec"), "duration_sec", 1)
        until = _parse_optional_int(params.get("until_ts"), "until_ts", 0)
        if duration is None and until is None:
            raise CommandError(
                "Укажите duration_sec или until_ts (условно обязательное поле)")
        if duration is not None:
            cleaned["duration_sec"] = duration
        if until is not None:
            cleaned["until_ts"] = until
        cleaned["reason"] = _clean_reason(params.get("reason"))
        if "abort_current" in params:
            if not isinstance(params["abort_current"], bool):
                raise CommandError("Поле «abort_current» должно быть булевым")
            cleaned["abort_current"] = params["abort_current"]
    elif command == "resume_controller":
        cleaned["reason"] = _clean_reason(params.get("reason"))
    elif command == "reboot":
        cleaned["reason"] = _clean_reason(params.get("reason"))
    else:
        raise CommandError(f"Неизвестная команда: {command}")
    extra = set(params) - set(cleaned)
    if extra:
        raise CommandError(
            "Неизвестные поля params: " + ", ".join(sorted(extra)))
    return cleaned


class MqttCommandService:
    """Публичный API отправки команд + ожидание подтверждения."""

    def __init__(self, cfg: Config, conn: sqlite3.Connection, mqtt=None,
                 db_path: str | None = None):
        self.cfg = cfg
        self.conn = conn          # соединение потока FastAPI (для чтения controllers/logs)
        # Фоновые потоки send_command_async не могут писать в общее соединение
        # FastAPI (check_same_thread=True) — свой короткий коннект на поток.
        self._db_path = db_path
        self._mqtt = mqtt         # MqttServerClient (или None до старта)
        self._lock = threading.Lock()
        # Отложенные тесты: sleep можно заменить на быструю имитацию времени
        self._sleep = time.sleep
        self._monotonic = time.monotonic

    def _log_conn(self) -> sqlite3.Connection:
        if self._db_path is not None:
            return sqlite3.connect(self._db_path)
        return self.conn  # тестовый/однопоточный режим: общее соединение

    # ------------------------------------------------------------------ wiring
    def attach(self, mqtt_client) -> None:
        """Подключить запущенный MqttServerClient (вызывается из main.py/start_mqtt)."""
        self._mqtt = mqtt_client

    @property
    def mqtt(self):
        return self._mqtt

    # ------------------------------------------------------------------- journal
    def write_log(self, action: str, box_id: str, details: dict,
                  username: Optional[str] = None, source: str = "api") -> None:
        try:
            lc = self._log_conn()
            with lc:
                lc.execute(
                    "INSERT INTO logs(ts, username, action, object_type, object_id,"
                    " details_json, source) VALUES (?, ?, ?, 'controller', ?, ?, ?)",
                    (datetime.now(timezone.utc).isoformat(timespec="seconds"),
                     username, action, box_id,
                     json.dumps(details, ensure_ascii=False), source),
                )
            if lc is not self.conn:
                lc.close()
        except Exception:
            log.exception("Команды: не удалось записать в журнал (action=%s)", action)

    def _controller_row(self, controller_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT id, box_id, name, connection_status FROM controllers "
            "WHERE id=? AND deleted_at IS NULL", (controller_id,)
        ).fetchone()

    # ---------------------------------------------------------------- send_command
    def send_command(self, box_id: str, command: str, params: dict | None,
                     source: str = "web", user_id: Optional[int] = None,
                     username: Optional[str] = None) -> dict:
        """Отправить команду и дождаться финального command_ack.

        Возвращает dict: command_id, command, status ('completed'|'rejected'|
        'error'|'ignored_duplicate'|'timeout'), message, attempts.
        Исключения: ControllerOffline (409), CommandError (422).
        """
        if command not in COMMAND_NAMES:
            raise CommandError(f"Неизвестная команда: {command}")
        params = validate_command_params(command, params or {})

        row = self.conn.execute(
            "SELECT id, connection_status FROM controllers WHERE box_id=?",
            (box_id,)).fetchone()
        if row is None:
            raise CommandError(f"Контроллер {box_id} не найден")
        if row["connection_status"] != "online":
            # Очередь на Этапе 3 НЕ ведём (ТЗ §4.4: изменения расписания могут
            # быть поставлены в очередь — позже). Фиксируем отказ в журнале.
            self.write_log("command.rejected", box_id,
                           {"command": command, "reason": "controller_offline"},
                           username=username)
            log.warning("Команды: %s офлайн — команда %s НЕ отправлена",
                        box_id, command)
            raise ControllerOffline(box_id)

        command_id = str(uuid.uuid4())
        # ts: вещественное число (секунды Unix) — контракт Артефакта 0.5 §5.1;
        # int() здесь отбрасывал дробную часть и ломал точные сравнения в тестах.
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "command_id": command_id,
            "command": command,
            "ts": self._monotonic_to_unix(),
            "source": source,
            "params": params,
        }
        if user_id is not None:
            payload["user_id"] = user_id

        timeout = self.cfg.mqtt_command_timeout_sec
        retries = max(0, self.cfg.mqtt_command_retries)
        qos = self.cfg.mqtt_qos_command
        topic = f"poliv/{box_id}/command"

        if self._mqtt is None:
            # Брокер не подключён (MQTT-клиент сервера не запущен): публикаций
            # нет. Офлайн-контроллер — как и раньше 409; online без брокера — 503.
            if row["connection_status"] != "online":
                raise ControllerOffline(box_id)
            raise MqttUnavailable(
                f"MQTT-брокер недоступен: команда «{command}» не отправлена")
        mqtt = self._mqtt

        attempts = 0
        with self._lock:  # последовательная отправка: простая и предсказуемая
            entry = mqtt.register_pending(command_id)
            try:
                while True:
                    attempts += 1
                    mqtt.publish(topic, payload, qos=qos)
                    self.write_log("command.sent", box_id,
                                   {"command_id": command_id, "command": command,
                                    "attempt": attempts}, username=username)
                    log.info("Команды: %s -> %s (id=%s, попытка %d, QoS%d)",
                             command, box_id, command_id[:8], attempts, qos)
                    deadline = self._monotonic() + timeout
                    final = False
                    while self._monotonic() < deadline:
                        if entry["final"]:
                            final = True
                            break
                        self._sleep(0.02)
                    if final:
                        break
                    if attempts > retries:
                        break
                    log.warning("Команды: таймаут ack %s (%s), повтор %d/%d "
                                "с тем же command_id", command_id[:8], box_id,
                                attempts, retries)
                    self.write_log("command.timeout", box_id,
                                   {"command_id": command_id, "command": command,
                                    "attempt": attempts}, username=username)
            finally:
                status = entry.get("status") or "timeout"
                message = entry.get("message")
                mqtt.unregister_pending(command_id)

        result = {
            "command_id": command_id,
            "command": command,
            "status": status,
            "message": message,
            "attempts": attempts,
        }
        if status == "rejected":
            self.write_log("command.rejected", box_id,
                           {"command_id": command_id, "command": command,
                            "ack_status": status, "message": message},
                           username=username)
        elif status == "timeout":
            self.write_log("command.timeout", box_id,
                           {"command_id": command_id, "command": command,
                            "attempts": attempts}, username=username)
        log.info("Команды: итог %s (%s) от %s: %s", command_id[:8], command,
                 box_id, status)
        return result

    def send_command_for_controller(self, controller_id: int, command: str,
                                    params: dict | None, source: str = "web",
                                    user_id: Optional[int] = None,
                                    username: Optional[str] = None) -> dict:
        """То же, но по внутреннему id контроллера (используется API/вебом)."""
        row = self._controller_row(controller_id)
        if row is None:
            raise CommandError(f"Контроллер #{controller_id} не найден")
        return self.send_command(row["box_id"], command, params, source,
                                 user_id=user_id, username=username)

    def _monotonic_to_unix(self) -> int:
        return int(datetime.now(timezone.utc).timestamp())

    def send_command_async(self, controller_id: int, command: str,
                           params: dict | None, source: str = "web",
                           user_id: Optional[int] = None,
                           username: Optional[str] = None) -> str:
        """Неблокирующая отправка для веб-формы (Блок 6): публикует команду и
        сразу возвращает command_id; финальный ack обработчик пишет в app.log.

        Проверки «до публикации»: контроллер существует, online, MQTT
        подключён, команда/параметры валидны — иначе ControllerOffline /
        MqttUnavailable / CommandError (веб-маршрут конвертирует их во
        flash-сообщение).
        """
        row = self._controller_row(controller_id)
        if row is None:
            raise CommandError(f"Контроллер #{controller_id} не найден")
        box_id = row["box_id"]
        if command not in COMMAND_NAMES:
            raise CommandError(f"Неизвестная команда: {command}")
        params = validate_command_params(command, params or {})
        if row["connection_status"] != "online":
            self.write_log("command.rejected", box_id,
                           {"command": command, "reason": "controller_offline"},
                           username=username)
            raise ControllerOffline(box_id)
        if self._mqtt is None:
            raise MqttUnavailable(
                f"MQTT-брокер недоступен: команда «{command}» не отправлена")

        import threading

        command_id = str(uuid.uuid4())
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "command_id": command_id,
            "command": command,
            "ts": self._monotonic_to_unix(),
            "source": source,
            "params": params,
        }
        if user_id is not None:
            payload["user_id"] = user_id
        topic = f"poliv/{box_id}/command"
        qos = self.cfg.mqtt_qos_command
        timeout = self.cfg.mqtt_command_timeout_sec
        retries = max(0, self.cfg.mqtt_command_retries)

        entry = self._mqtt.register_pending(command_id)
        mqtt = self._mqtt

        def _worker():
            attempts = 0
            try:
                while True:
                    attempts += 1
                    mqtt.publish(topic, payload, qos=qos)
                    self.write_log("command.sent", box_id,
                                   {"command_id": command_id, "command": command,
                                    "attempt": attempts}, username=username)
                    deadline = self._monotonic() + timeout
                    final = False
                    while self._monotonic() < deadline:
                        if entry["final"]:
                            final = True
                            break
                        self._sleep(0.05)
                    if final or attempts > retries:
                        break
                    self.write_log("command.timeout", box_id,
                                   {"command_id": command_id, "command": command,
                                    "attempt": attempts}, username=username)
                status = entry.get("status") or "timeout"
                message = entry.get("message")
                if status == "rejected":
                    self.write_log("command.rejected", box_id,
                                   {"command_id": command_id, "command": command,
                                    "ack_status": status, "message": message},
                                   username=username)
                elif status == "timeout":
                    self.write_log("command.timeout", box_id,
                                   {"command_id": command_id, "command": command,
                                    "attempts": attempts}, username=username)
                else:
                    self.write_log("command.acked", box_id,
                                   {"command_id": command_id, "command": command,
                                    "ack_status": status, "message": message},
                                   username=username)
                log.info("Команды (async): итог %s (%s) от %s: %s",
                         command_id[:8], command, box_id, status)
            finally:
                mqtt.unregister_pending(command_id)

        threading.Thread(target=_worker, daemon=True,
                         name=f"cmd-{command_id[:8]}").start()
        return command_id
