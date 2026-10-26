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
from contextlib import nullcontext
from ..infra.connection_scope import scoped_task
from datetime import datetime, timezone
from typing import Any, Optional

from ..infra.db import connect_path
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


class EmergencyLockError(RuntimeError):
    """Аудит 06.10 (P1-1): активная аварийная блокировка контроллера —
    возобновляющие/запускающие команды (zone_open, resume_controller) не
    отправляются (API/веб: 409 emergency_locked). stop_all и безопасные
    диагностики (ping, zone_close) остаются доступны."""

    def __init__(self, box_id: str):
        super().__init__(
            f"Контроллер {box_id}: активна аварийная блокировка — "
            f"запуск полива запрещён до снятия аварии оператором")
        self.box_id = box_id


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
                 db_path: str | None = None, flow_service=None):
        self.cfg = cfg
        self.conn = conn          # соединение потока FastAPI (для чтения controllers/logs)
        # Фоновые потоки send_command_async не могут писать в общее соединение
        # FastAPI (check_same_thread=True) — свой короткий коннект на поток.
        self._db_path = db_path
        self._mqtt = mqtt         # MqttServerClient (или None до старта)
        # Аудит 06.10 (P1-1): FlowService для проверки аварийной блокировки;
        # если не инжектирован — используется собственный SQL-запрос (ниже).
        self._flow_service = flow_service
        self._lock = threading.RLock()  # protects publication and cancellation only
        self._worker_slots = threading.BoundedSemaphore(16)
        self._stop_slots = threading.BoundedSemaphore(64)
        self._workers = set()
        self._closing = threading.Event()
        self._lanes = {}
        self._stop_generation = {}

        # Отложенные тесты: sleep можно заменить на быструю имитацию времени
        self._sleep = time.sleep
        self._monotonic = time.monotonic

    def _log_conn(self) -> sqlite3.Connection:
        if self._db_path is not None:
            return connect_path(self._db_path, check_same_thread=True)
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

    # ------------------------------------------------- аварийная блокировка (аудит P1-1)
    # Команды, которые ЗАПРЕЩЕНЫ при активной аварийной блокировке: они
    # запускают или возобновляют полив. Остальные (stop_all, zone_close, ping,
    # reboot) остаются доступны — остановка и диагностика нужнее.
    EMERGENCY_BLOCKED_COMMANDS = {"zone_open", "resume_controller"}

    def is_emergency_locked(self, controller_id: int) -> bool:
        """Актуальная серверная аварийная блокировка контроллера."""
        row = self.conn.execute(
            "SELECT emergency_lock_until_ts FROM controllers WHERE id=?",
            (controller_id,)).fetchone()
        if row is None or row["emergency_lock_until_ts"] is None:
            return False
        try:
            return int(row["emergency_lock_until_ts"]) > time.time()
        except (TypeError, ValueError):
            return False

    def _check_emergency_lock(self, controller_id: int, command: str,
                              box_id: str,
                              username: Optional[str] = None) -> None:
        """Бросает EmergencyLockError для запускающих команд под блокировкой."""
        if command in self.EMERGENCY_BLOCKED_COMMANDS and \
                self.is_emergency_locked(controller_id):
            self.write_log("command.rejected", box_id,
                           {"command": command, "reason": "emergency_locked"},
                           username=username)
            log.warning("Команды: %s — команда %s отклонена: активна аварийная "
                        "блокировка", box_id, command)
            raise EmergencyLockError(box_id)

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

        # Аудит 06.10 (P1-1): централизованный запрет запуска полива при
        # активной аварийной блокировке — ВСЕ пути отправки (API/веб/сервисы)
        # проходят через send_command/send_command_async, проверка здесь.
        self._check_emergency_lock(row["id"], command, box_id,
                                   username=username)

        from .block_service import check_manual, BlockError
        try:
            check_manual(self.conn, row['id'], command, params)
        except BlockError as exc:
            raise CommandError(str(exc)) from exc
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

        generation, lane = self._command_lane(box_id, command)
        if self._mqtt is None:
            # Брокер не подключён (MQTT-клиент сервера не запущен): публикаций
            # нет. Офлайн-контроллер — как и раньше 409; online без брокера — 503.
            if row["connection_status"] != "online":
                raise ControllerOffline(box_id)
            raise MqttUnavailable(
                f"MQTT-брокер недоступен: команда «{command}» не отправлена")
        mqtt = self._mqtt

        return self._execute_command(mqtt, box_id, command, payload, username,
                                     generation, lane)

    def _command_lane(self, box_id, command):
        with self._lock:
            if command == "stop_all":
                self._stop_generation[box_id] = self._stop_generation.get(box_id, 0) + 1
            return (self._stop_generation.get(box_id, 0),
                    self._lanes.setdefault(box_id, threading.Lock()))

    def _execute_command(self, mqtt, box_id, command, payload, username,
                         generation, lane):
        command_id = payload["command_id"]
        timeout = self.cfg.mqtt_command_timeout_sec
        retries = max(0, self.cfg.mqtt_command_retries)
        topic = f"poliv/{box_id}/command"
        attempts = 0
        entry = mqtt.register_pending(command_id, box_id=box_id, command=command)

        def superseded():
            return (self._closing.is_set() or (command != "stop_all" and
                    generation != self._stop_generation.get(box_id, 0)))

        try:
            # Stops bypass the lane, even while another command awaits ACK.
            with (nullcontext() if command == "stop_all" else lane):
                while True:
                    with self._lock:
                        if superseded():
                            entry.update(status="rejected", message="superseded_by_stop", final=True)
                            break
                        # Validate again after waiting for the ordinary command lane.
                        row = self.conn.execute(
                            "SELECT id, connection_status FROM controllers WHERE box_id=?",
                            (box_id,)).fetchone()
                        if row is None or row["connection_status"] != "online":
                            raise ControllerOffline(box_id)
                        self._check_emergency_lock(row["id"], command, box_id, username=username)
                        from .block_service import check_manual, BlockError
                        try:
                            check_manual(self.conn, row['id'], command, payload['params'])
                        except BlockError as exc:
                            raise CommandError(str(exc)) from exc
                        attempts += 1
                        pub = mqtt.publish(topic, payload, qos=self.cfg.mqtt_qos_command)
                    if isinstance(pub, dict) and not pub.get("ok", True):
                        self.write_log("command.publish_failed", box_id,
                                       {"command_id": command_id, "command": command,
                                        "attempt": attempts, "error": pub.get("error")},
                                       username=username)
                        raise MqttUnavailable("MQTT command publication failed")
                    self.write_log("command.sent", box_id,
                                   {"command_id": command_id, "command": command,
                                    "attempt": attempts}, username=username)
                    deadline = self._monotonic() + timeout
                    while self._monotonic() < deadline and not entry["final"]:
                        with self._lock:
                            if superseded():
                                entry.update(status="rejected", message="superseded_by_stop", final=True)
                                break
                        self._sleep(0.02)
                    if entry["final"] or attempts > retries:
                        break
                    self.write_log("command.timeout", box_id,
                                   {"command_id": command_id, "command": command,
                                    "attempt": attempts}, username=username)
        finally:
            mqtt.unregister_pending(command_id)
        status = entry.get("status") or "timeout"
        result = {"command_id": command_id, "command": command, "status": status,
                  "message": entry.get("message"), "attempts": attempts}
        self.write_log("command.rejected" if status == "rejected" else
                       "command.timeout" if status == "timeout" else "command.acked",
                       box_id, result, username=username)
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
        # Аудит 06.10 (P1-1): та же централизованная блокировка запуска, что и
        # в синхронном send_command.
        self._check_emergency_lock(row["id"], command, box_id,
                                   username=username)
        if self._mqtt is None:
            raise MqttUnavailable(
                f"MQTT-брокер недоступен: команда «{command}» не отправлена")

        import threading

        from .block_service import check_manual, BlockError
        try:
            check_manual(self.conn, row['id'], command, params)
        except BlockError as exc:
            raise CommandError(str(exc)) from exc
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
        mqtt = self._mqtt
        slots = self._stop_slots if command == "stop_all" else self._worker_slots
        if self._closing.is_set() or not slots.acquire(blocking=False):
            raise MqttUnavailable("Command worker capacity exhausted")
        generation, lane = self._command_lane(box_id, command)

        @scoped_task
        def _worker():
            try:
                self._execute_command(mqtt, box_id, command, payload, username,
                                      generation, lane)
            except Exception:
                log.exception("Asynchronous command failed: %s", command_id)
            finally:
                slots.release()
                with self._lock:
                    self._workers.discard(threading.current_thread())

        worker = threading.Thread(target=_worker, daemon=True, name=f"cmd-{command_id[:8]}")
        with self._lock:
            self._workers.add(worker)
        try:
            worker.start()
        except BaseException:
            with self._lock:
                self._workers.discard(worker)
            slots.release()
            raise
        return command_id

    def close(self):
        self._closing.set()
        with self._lock:
            workers = tuple(self._workers)
        for worker in workers:
            worker.join(timeout=2)
