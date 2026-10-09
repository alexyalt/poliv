"""Эмулятор одного контроллера автополива (Этап 3, Блок 7).

Поведение по Артефакту 0.5:
- при подключении ставит LWT: poliv/{box_id}/lwt, retained, {"online": false};
- публикует hello при старте (и при возврате «в онлайн»);
- status — по интервалу (--status-interval, по умолчанию 30 с) и при смене состояния;
- подписка poliv/{box_id}/command; дедупликация по command_id (последние 50):
  повторный command_id -> ack ignored_duplicate БЕЗ повторного действия;
- выполняет zone_open / zone_close / stop_all / pause_controller /
  resume_controller / ping / reboot через локальную машину состояний
  (mode/phase/active_zones, таймеры); ack: сначала accepted, затем completed;
- flow: во время полива раз в 10 с (имитация lpm и суммарного литража),
  в простое раз в 30 с;
- event: старт/остановка полива (event_uid=uuid4, source=manual|schedule,
  water_sec, volume_liters) — сервер Этапа 3 только логирует события; с Этапа 4
  события schedule-прогонов формируют watering_runs на сервере;
- schedule_request при старте (и по клавише 's'); после получения schedule
  отвечает schedule_ack;
- ЭТАП 4 — исполнение машинограммы (_on_schedule + _tick_schedule): запуски
  (runs) стартуют по date+start_minute_local (с учётом timezone_offset_min),
  шаги (steps) исполняются последовательно: phase=water открывает active_zones,
  phase=soak — реле выключены, показывается display_zones (§3.9); параллельные
  шаги с parallel_allowed открывают несколько зон одним шагом; ручной прогон
  ставит график на паузу и возобновляется после его завершения (ТЗ п.5);
  stop_all/pause_controller останавливают schedule-прогон (run_id сохраняется
  для журнала planned→active); max_start_delay_sec — опоздавший запуск
  пропускается, max_catchup_sec — ограничение догона;
- time: принимает poliv/{box_id}/time и логирует расхождение с локальным временем.

Интерактивные клавиши (при запуске в терминале):
  o — имитировать обрыв связи (offline) / возврат (online)
  e — авария: mode=error, error_code=4001 (повторно 'e' — снять аварию)
  w — ручной полив зоны 1 на 60 секунд
  s — отправить schedule_request

CLI-флаги имитаций: --offline-after N (через N секунд — обрыв соединения).

Лог: консоль + logs/emulator.log (файл не коммитится, в .gitignore).

Запуск:
  python emulator/controller_sim.py --box-id BOX-EMUL-01 --broker 127.0.0.1 \
      --port 1883 --user poliv_box_01 --password secret --status-interval 5
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PROTOCOL_VERSION = "1.0"
DEDUP_SIZE = 50                 # хранить последние 50 command_id
FLOW_ACTIVE_PERIOD = 10.0       # flow во время полива — раз в 10 с
FLOW_IDLE_PERIOD = 30.0         # flow в простое — раз в 30 с
LPM_BASE = 12.0                 # имитация производительности, л/мин


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ControllerSim:
    """Один эмулируемый контроллер: MQTT-клиент + машина состояний."""

    def __init__(self, box_id: str, broker: str = "127.0.0.1", port: int = 1883,
                 user: str | None = None, password: str | None = None,
                 status_interval: float = 30.0, zones: int = 16,
                 offline_after: float | None = None, log=None):
        self.box_id = box_id
        self.broker = broker
        self.port = port
        self.user = user
        self.password = password
        self.status_interval = max(1.0, float(status_interval))
        self.zones_total = max(1, int(zones))
        self.offline_after = offline_after
        self.log = log or logging.getLogger("poliv.emulator")

        # --- состояние контроллера -----------------------------------------
        self.mode = "idle"            # idle|manual|schedule|paused|error|service
        self.phase = "idle"           # idle|water|soak|waiting|paused|error
        self.active_zones: list[int] = []
        self.display_zones: list[int] = []
        self.primary_zone: int | None = None
        self.error_code: int | None = None
        self.schedule_version = 0
        self.schedule_hash = ""
        self.time_valid = True
        self.buffer_size = 0          # неотправленные события (буфер не переполняется)
        self.flow_enabled = False
        self.flow_total_liters = 0.0
        self.instant_lpm = 0.0
        self.service_mode = False
        self.pause_until_ts: int | None = None
        self.online = True
        self.firmware_version = "sim-1.0.0"

        # --- таймеры полива ---------------------------------------------------
        self._run_end_ts: float | None = None     # конец текущего прогона (manual/schedule)
        self._water_until: float | None = None    # конец фазы полива
        self._soak_until: float | None = None     # конец фазы замачивания
        self._pending_zone_events: list[tuple[int, int]] = []  # очередь (zone, duration)
        self._current_event: dict | None = None   # незакрытое событие полива
        # источник текущего прогона: "manual" (клавиша w / zone_open) или
        # "schedule" (Этап 4 — исполнение машинограммы); сбрасывается в _finish_run().
        self._run_source: str | None = None

        # --- Этап 4: исполнение машинограммы ---------------------------------
        self.schedule_runs: list[dict] = []      # runs из последней машинограммы
        self.schedule_options: dict = {}         # options (§3.2): max_start_delay_sec...
        self.schedule_tz_offset_min: int = 0     # timezone_offset_min машинограммы
        self._sched_state: dict | None = None    # активный schedule-прогон:
        #   {run, run_idx, step_idx, started_wall, paused, pause_saved_until,
        #    skipped_reason}
        self._sched_done_run_ids: set[str] = set()   # выполненные/пропущенные run_id
        self._sim_now: float | None = None       # инъекция времени для тестов

        self._seen_commands: list[str] = []       # дедупликация command_id (последние 50)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._client = None
        self._last_status_mono = 0.0
        self._last_flow_mono = 0.0
        self._connected_evt = threading.Event()
        self._manual_disconnect = False

    # ------------------------------------------------------------------ MQTT
    def _make_client(self):
        import paho.mqtt.client as mqtt
        try:
            c = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id=f"{self.box_id}-{uuid.uuid4().hex[:6]}",
                            protocol=mqtt.MQTTv311)
        except (AttributeError, TypeError):
            c = mqtt.Client(client_id=f"{self.box_id}-{uuid.uuid4().hex[:6]}")
        if self.user:
            c.username_pw_set(self.user, self.password or None)
        # LWT: retained, online=false (Артефакт 0.5 §2.2)
        lwt_payload = json.dumps({
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "ts": time.time(),
            "online": False,
            "reason": "lwt",
        }, ensure_ascii=False)
        c.will_set(f"poliv/{self.box_id}/lwt", lwt_payload, qos=1, retain=True)
        c.on_connect = self._on_connect
        c.on_message = self._on_message
        return c

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        client.subscribe(f"poliv/{self.box_id}/command", 1)
        client.subscribe(f"poliv/{self.box_id}/schedule", 1)
        client.subscribe(f"poliv/{self.box_id}/time", 0)
        self._connected_evt.set()
        self.log.info("[%s] подключение к брокеру %s:%d (rc=%s)", self.box_id,
                      self.broker, self.port, reason_code)

    def start(self) -> None:
        self._client = self._make_client()
        self._client.connect(self.broker, self.port, keepalive=60)
        self._client.loop_start()
        self._spawn(self._main_loop, "sim-main")
        self._spawn(self._keyboard_loop, "sim-keys")
        if self.offline_after is not None:
            self._spawn(self._offline_after_worker, "sim-offline-after")
        # hello ждём установления соединения
        self._spawn(self._greeting, "sim-greet")

    def _greeting(self) -> None:
        if self._connected_evt.wait(timeout=15):
            self.publish_hello()
            self.publish_schedule_request("startup")

    def stop(self) -> None:
        self._stop.set()
        if self._client is not None:
            # Дефект Б: штатный (graceful) disconnect не триггерит волю брокера,
            # поэтому перед отключением имитируем LWT retained-сообщением.
            if self.online:
                self._publish_lwt_offline("shutdown")
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass

    def _publish(self, kind: str, payload: dict, qos: int = 1,
                 retain: bool = False) -> None:
        # online=False имитирует обрыв связи: сообщения НЕ должны уходить
        # на брокер. В тестах с инъекцией времени (_sim_now) допустима
        # «симуляция офлайна» без реального клиента — публикуем перехватчику.
        if self._client is None or (not self.online and self._sim_now is None):
            return
        try:
            self._client.publish(f"poliv/{self.box_id}/{kind}",
                                 json.dumps(payload, ensure_ascii=False),
                                 qos=qos, retain=retain)
        except Exception as exc:
            self.log.warning("[%s] публикация %s не удалась: %s", self.box_id,
                             kind, exc)

    def _publish_lwt_offline(self, reason: str) -> None:
        """Имитация LWT при ШТАТНОМ (graceful) отключении — Дефект Б.

        Брокер выдаёт will только при аварийном обрыве TCP; корректный
        disconnect() волю НЕ триггерит, и сервер видел бы «зombie-online» до
        срабатывания offline_threshold_min. Поэтому перед loop_stop()/disconnect()
        публикуем в poliv/{box_id}/lwt retained-сообщение online=false — ровно то,
        что выдал бы брокер от имени контроллера. Публикация идёт напрямую через
        клиент (в обход _publish и флага online): событие должно уйти даже когда
        self.online уже выставлен в False.
        """
        if self._client is None:
            return
        lwt_payload = json.dumps({
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "ts": time.time(),
            "online": False,
            "reason": reason,          # "lwt-sim" | "shutdown"
        }, ensure_ascii=False)
        try:
            self._client.publish(f"poliv/{self.box_id}/lwt", lwt_payload,
                                 qos=1, retain=True)
        except Exception as exc:
            self.log.warning("[%s] имитация LWT (%s) не удалась: %s",
                             self.box_id, reason, exc)

    # ------------------------------------------------------------- сообщения
    def _on_message(self, client, userdata, msg):
        try:
            kind = msg.topic.rsplit("/", 1)[-1]
            try:
                p = json.loads(msg.payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self.log.warning("[%s] %s: невалидный JSON — игнор", self.box_id, kind)
                return
            handler = getattr(self, f"_on_{kind}", None)
            if handler is None:
                self.log.warning("[%s] неизвестный топик %s", self.box_id, msg.topic)
                return
            handler(p)
        except Exception:
            self.log.exception("[%s] ошибка обработки %s", self.box_id,
                               getattr(msg, "topic", "?"))

    def _on_time(self, p: dict) -> None:
        """Серверная синхронизация времени: логируем расхождение (ТЗ §9.2)."""
        server_ts = p.get("unix_ts") or p.get("ts")
        if isinstance(server_ts, (int, float)):
            drift = time.time() - float(server_ts)
            self.log.info("[%s] time от сервера: расхождение %+0.1f с (локальное −%s)",
                          self.box_id, drift, "ок" if abs(drift) < 5 else "ПРОБЛЕМА")
            self.time_valid = abs(drift) < 60

    def _now(self) -> float:
        """Текущее время: wall-clock или инъекция (детерминированные тесты)."""
        return time.time() if self._sim_now is None else float(self._sim_now)

    def _local_minute(self, ts: float) -> int:
        """Минута локальных суток контроллера по unix-ts (с учётом смещения §3.2)."""
        shifted = ts + self.schedule_tz_offset_min * 60
        d = datetime.fromtimestamp(shifted, tz=timezone.utc)
        return d.hour * 60 + d.minute

    def _local_date(self, ts: float) -> str:
        shifted = ts + self.schedule_tz_offset_min * 60
        return datetime.fromtimestamp(shifted, tz=timezone.utc).date().isoformat()

    def _on_schedule(self, p: dict) -> None:
        """Этап 4: приём и ИСПОЛНЕНИЕ машинограммы (ТЗ: «эмулятор реально
        выполняет расписание»).

        apply_policy (на стороне контроллера, ТЗ п.4): версия всегда принимается
        и запоминается; если прямо сейчас идёт schedule-прогон той же версии —
        ничего не трогаем; активного прогона нет — график подменяется целиком
        (текущий run_id помечается отменённым, planned-записи новых прогонов
      создаёт сервер при ack). Ручной/ставший на паузу прогон новой версией
        НЕ прерывается — он продолжится, а график стартует после его завершения.
        """
        sv = p.get("schedule_version")
        runs = p.get("runs") or []
        opts = p.get("options") or {}
        with self._lock:
            self.schedule_version = sv if isinstance(sv, int) else 0
            self.schedule_hash = str(p.get("schedule_hash", ""))
            self.schedule_runs = [r for r in runs if isinstance(r, dict)]
            self.schedule_options = opts if isinstance(opts, dict) else {}
            tz = p.get("timezone_offset_min")
            self.schedule_tz_offset_min = tz if isinstance(tz, int) else 0
            active = self._sched_state
            replaced = (active is not None
                        and active.get("run") is not None
                        and active["run"].get("schedule_version") != self.schedule_version)
            if replaced and not active.get("manual_active"):
                # новая версия до завершения прогона старой: текущий запуск
                # отменяется (не выполняется), остальные planned остаются в БД
                self._cancel_run(active["run"]["run_id"], "replaced_by_new_version")
                if active.get("event_open"):
                    # §3.11: прерванный schedule-прогон обязан породить
                    # finished(aborted) с run_id — сервер закроет planned-запись
                    ev = active.pop("sched_event", None) or self._current_event
                    if ev is not None:
                        self._current_event = ev
                        self._emit_event("finished", source="schedule",
                                         zones=ev.get("zones") or [],
                                         aborted=True,
                                         run_id=str(active["run"].get("run_id")))
                    self._current_event = None
                self._sched_state = None
                if self.mode == "schedule":
                    # прогон прерван заменой версии — контроллер обязан
                    # вернуться в idle и немедленно сканировать новые runs
                    # (next_wait_ts=0), иначе остаток старого 300-секундного
                    # окна удерживал бы старт r-new до конца «виртуальных» суток
                    self.mode, self.phase = "idle", "idle"
                    self.active_zones, self.display_zones = [], []
                    self.primary_zone, self.flow_enabled = None, False
            elif replaced:
                # идёт ручной прогон поверх графика — замену применим позже
                active["pending_new"] = True
            if self._sched_state is None and self.mode in ("idle", "paused"):
                self._sched_state = {"run": None, "started_wall": None,
                                     "manual_active": False, "skipped_reason": None}
        self.log.info("[%s] машинограмма v%s принята к ИСПОЛНЕНИЮ (runs=%d, "
                      "tz=%+d мин, заменa прогона: %s)", self.box_id,
                      self.schedule_version, len(self.schedule_runs),
                      self.schedule_tz_offset_min, "да" if replaced else "нет")
        self._publish("schedule_ack", {
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "ts": self._now(),
            "schedule_version": self.schedule_version,
            "schedule_hash": self.schedule_hash,
            "status": "applied",
            "message": "машинограмма принята, исполнение активно (Этап 4)",
        })

    # -------------------------------------------------------------- команды
    def _dedup(self, command_id: str) -> bool:
        """True, если command_id уже обрабатывался (идемпотентность §6.3)."""
        with self._lock:
            if command_id in self._seen_commands:
                return True
            self._seen_commands.append(command_id)
            if len(self._seen_commands) > DEDUP_SIZE:
                self._seen_commands.pop(0)
            return False

    def _ack(self, p_cmd: dict, status: str, message: str = "") -> None:
        self._publish("command_ack", {
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "ts": time.time(),
            "command_id": p_cmd.get("command_id"),
            "command": p_cmd.get("command"),
            "status": status,
            "message": message,
        })

    def _on_command(self, p: dict) -> None:
        required = ("command_id", "command")
        if any(p.get(k) is None for k in required):
            self.log.warning("[%s] command без обязательных полей %s — игнор",
                             self.box_id, [k for k in required if p.get(k) is None])
            return
        cid = str(p["command_id"])
        cmd = p["command"]
        params = p.get("params") or {}
        if self._dedup(cid):
            self.log.info("[%s] ДУБЛИКАТ команды %s (id=%s) — ignored_duplicate,"
                          " действие НЕ повторяется", self.box_id, cmd, cid[:8])
            self._ack(p, "ignored_duplicate", "command_id уже выполнен ранее")
            return
        self.log.info("[%s] команда %s id=%s params=%s", self.box_id, cmd,
                      cid[:8], params)
        self._ack(p, "accepted")            # промежуточный ack
        ok, message = self._execute(cmd, params)
        # completed/отклонение — следующим ack (короткая пауза, как реальное исполнение)
        threading.Timer(0.3, lambda: self._ack(p,
                    "completed" if ok else "rejected", message)).start()

    def _execute(self, command: str, params: dict) -> tuple[bool, str]:
        with self._lock:
            if command == "ping":
                return True, "pong"
            if command == "zone_open":
                zone = params.get("zone")
                dur = params.get("duration_sec")
                if not isinstance(zone, int) or not 1 <= zone <= self.zones_total:
                    return False, f"зона {zone} вне диапазона 1..{self.zones_total}"
                if not isinstance(dur, int) or dur < 1:
                    return False, "duration_sec должен быть целым >= 1"
                if self.mode == "error":
                    return False, "аварийная блокировка — снимите аварию"
                self._start_manual_water(zone, dur)
                return True, f"зона {zone} открыта на {dur} с"
            if command == "zone_close":
                zone = params.get("zone")
                if zone in self.active_zones:
                    self._finish_run(aborted=True)
                    return True, f"зона {zone} закрыта"
                return True, f"зона {zone} не была открыта"
            if command == "stop_all":
                stopped = bool(self.active_zones)
                sched_running = (self._sched_state is not None
                                 and self._sched_state.get("run") is not None)
                if sched_running:
                    # Этап 4: остановка работает и для schedule-прогона —
                    # запуск завершается прерыванием, остальные по графику
                    # остаются в planned (повторного автоматического старта
                    # этого run_id не будет — ТЗ «остановка работает»)
                    self._finish_schedule_run(self._sched_state, self._now(),
                                              aborted=True)
                    stopped = True
                self._finish_run(aborted=True)
                self.mode, self.phase = "idle", "idle"
                self.pause_until_ts = None
                return True, ("все клапаны закрыты" if stopped
                              else "активных поливов не было")
            if command == "pause_controller":
                dur = params.get("duration_sec")
                until = params.get("until_ts")
                if dur is None and until is None:
                    return False, "нужен duration_sec или until_ts"
                if isinstance(dur, int) and dur >= 1:
                    self.pause_until_ts = int(self._now()) + dur
                elif isinstance(until, int):
                    self.pause_until_ts = until
                if self.mode == "schedule" and self._sched_state \
                        and self._sched_state.get("run") is not None:
                    # Этап 4: пауза контроллера ставит на паузу график —
                    # реле закрываются, остаток шага замораживается; после
                    # resume/истечения паузы прогон продолжается с того же шага
                    st = self._sched_state
                    if st.get("step_remaining") is None:
                        st["step_remaining"] = max(0.0, st["wall_ts"]
                                                   + st["step"]["duration_sec"]
                                                   - self._now())
                    self.mode, self.phase = "paused", "paused"
                    self.active_zones = []
                    self.flow_enabled = False
                    self.instant_lpm = 0.0
                elif self.active_zones:
                    self._pause_current_run()
                else:
                    self.mode, self.phase = "paused", "paused"
                self._publish_status(force=True)
                return True, f"пауза до {self.pause_until_ts}"
            if command == "resume_controller":
                if self.mode != "paused":
                    return True, "контроллер не был на паузе"
                self.pause_until_ts = None
                st = self._sched_state
                if st is not None and st.get("run") is not None \
                        and st.get("step_remaining") is not None:
                    # возврат к графику (ТЗ п.3): продолжаем тот же шаг
                    self.mode, self.phase = "schedule", st["step"].get(
                        "phase", "water") or "water"
                    if self.phase == "water":
                        self.active_zones = list(
                            st["step"].get("active_zones") or [])
                        self.display_zones = list(self.active_zones)
                        self.primary_zone = (self.active_zones[0]
                                             if self.active_zones else None)
                        self.flow_enabled = True
                    else:
                        self.display_zones = list(
                            st["step"].get("display_zones") or [])
                        self.flow_enabled = False
                    st["wall_ts"] = self._now()
                elif self._pending_zone_events or self._run_end_ts:
                    self.mode, self.phase = "manual", "water"
                    # возобновление ручного прогона: остаток таймера сохраняем
                    # (пауза его не расходовала — см. _pause_current_run)
                else:
                    self.mode, self.phase = "idle", "idle"
                self._publish_status(force=True)
                return True, "возобновлён"
            if command == "reboot":
                self.log.info("[%s] reboot: имитация перезагрузки", self.box_id)
                threading.Timer(1.0, self._reboot_done).start()
                return True, "перезагрузка запущена"
            return False, f"неизвестная команда: {command}"

    def _reboot_done(self) -> None:
        with self._lock:
            self.mode, self.phase = "provisioning", "idle"
            self.active_zones = []
        self.publish_hello()

    # ------------------------------------------------------- машина состояний
    def _start_manual_water(self, zone: int, duration_sec: int) -> None:
        # Этап 4 (ТЗ п.3): если прямо сейчас идёт schedule-прогон — он
        # ставится на паузу (заморозка остатка шага) и продолжится после
        # завершения ручного полива; свой finished-aborted-событие уже
        # отправит _finish_schedule_run. Иначе — завершаем незакрытый
        # manual-прогон без события (как в Этапе 3).
        st = self._sched_state
        sched_running = (st is not None and st.get("run") is not None
                         and self.mode == "schedule")
        if sched_running:
            if st.get("step_remaining") is None and st.get("step") is not None:
                st["step_remaining"] = max(0.0, st["wall_ts"]
                                           + st["step"]["duration_sec"]
                                           - self._now())
            st["manual_active"] = True
            # открытое schedule-событие прячем в стейт прогона: manual-прогон
            # использует общий слот _current_event; при завершении ручного
            # полива его finished-событие уходит как aborted с run_id (план→факт),
            # а schedule-событие возобновляется — у него сохранён ev_zones/run_id
            st["sched_event"] = self._current_event
            self._current_event = None     # manual-прогон начнёт своё событие
            self._run_source = None
        else:
            self._finish_run(aborted=True, emit_event=False)
        self.mode, self.phase = "manual", "water"
        self.active_zones = [zone]
        self.display_zones = [zone]
        self.primary_zone = zone
        self.flow_enabled = True
        self._water_until = self._now() + duration_sec
        self._run_end_ts = self._water_until
        self._run_source = "manual"     # "schedule" — резерв (Этап 4)
        self._emit_event("started", source=self._run_source, zones=[zone])
        self._publish_status(force=True)

    def _pause_current_run(self) -> None:
        now = self._now()
        if self._water_until is not None:
            remaining = max(0.0, self._water_until - now)
            self._pending_zone_events.append((self.primary_zone or 1,
                                              int(remaining)))
            self._water_until = None
        st = self._sched_state
        if st is not None and st.get("run") is not None \
                and st.get("step_remaining") is None:
            # заморозка остатка шага графика (возврат — в resume/по истечении)
            st["step_remaining"] = max(0.0, st["wall_ts"]
                                       + st["step"]["duration_sec"] - now)
        self.mode, self.phase = "paused", "paused"
        self.active_zones = []
        self.flow_enabled = False
        self.instant_lpm = 0.0

    def _finish_run(self, aborted: bool = False, emit_event: bool = True) -> None:
        if self._current_event is not None and emit_event:
            # Дефект A (ручная проверка Этапа 3): finished-событие обязано нести
            # source прогона — без него вызов падал по сигнатуре
            # _emit_event(status, source, zones, ...).
            self._emit_event("finished", source=self._run_source or "manual",
                             zones=self.active_zones, aborted=aborted)
        manual_was_running = self.mode == "manual"
        self._current_event = None
        self._run_source = None
        self.active_zones = []
        self.display_zones = []
        self.primary_zone = None
        self.flow_enabled = False
        self.instant_lpm = 0.0
        self._water_until = None
        self._soak_until = None
        self._run_end_ts = None
        self._pending_zone_events.clear()
        # Возврат к графику (ТЗ п.3 «после ручного режима график продолжается»):
        # если на время ручного прогона шаг машинограммы был заморожен —
        # размораживаем и продолжаем исполнение с того же шага.
        st = self._sched_state
        if manual_was_running and not aborted and st is not None \
                and st.get("run") is not None:
            # возврат к прерванному ручным поливом schedule-прогону (ТЗ п.3):
            # размораживаем остаток шага и продолжаем с того же шага;
            # возобновляем открытое schedule-событие (оно «пережидало» в st)
            st["manual_active"] = False
            if st.get("sched_event") is not None:
                self._current_event = st.pop("sched_event")
            if st.get("step_remaining") is not None:
                # Возврат к графику (вывод аудитора, проблема А): зоны
                # замороженного шага открываем СРАЗУ, не дожидаясь
                # следующего тика — иначе активные реле «пропадают» на тик.
                self.mode = "schedule"
                self._begin_step(st, self._now())
                return
        if self.mode not in ("error", "paused", "schedule"):
            self.mode, self.phase = "idle", "idle"

    def _tick_state(self) -> None:
        """Продвижение таймеров (вызывается из главного цикла)."""
        now = self._now()
        with self._lock:
            if self.mode == "error":
                return
            if self.mode == "paused":
                if self.pause_until_ts and now >= self.pause_until_ts:
                    # пауза истекла: продолжаем накопленный прогон или в idle
                    if self._pending_zone_events:
                        z, d = self._pending_zone_events.pop(0)
                        self.mode, self.phase = "manual", "water"
                        self.active_zones = [z]
                        self.primary_zone = z
                        self.flow_enabled = True
                        self._water_until = now + max(1, d)
                    else:
                        self.mode, self.phase = "idle", "idle"
                        self.pause_until_ts = None
                    self._publish_status(force=True)
                self._tick_schedule(now)   # график на паузе (заморозка шага)
                return
            if self.mode == "schedule":
                self._tick_schedule(now)
                return
            if self.mode == "manual" and self.active_zones:
                if self._water_until and now >= self._water_until:
                    # фаза замачивания 10 с, затем завершение прогона
                    self.phase = "soak"
                    self._water_until = None
                    self._soak_until = now + 10
                    self.flow_enabled = False
                    self.instant_lpm = 0.0
                    self._publish_status(force=True)
                elif self._soak_until and now >= self._soak_until:
                    self._finish_run()
                    self._publish_status(force=True)
                return
            if self.mode == "manual" and self._pending_zone_events:
                z, d = self._pending_zone_events.pop(0)
                self.mode, self.phase = "manual", "water"
                self.active_zones = [z]
                self.primary_zone = z
                self.flow_enabled = True
                self._water_until = now + max(1, d)
                # прогон продолжается в той же очереди — источник не сменился;
                # на случай пустого значения (после паузы с внешним сбросом) — manual
                self._run_source = self._run_source or "manual"
                self._emit_event("started", source=self._run_source, zones=[z])
                self._publish_status(force=True)
            self._tick_schedule(now)

    # ============================================ ЭТАП 4: исполнение графика
    def _cancel_run(self, run_id: str, reason: str) -> None:
        """Run отменён (замена версии/остановка): больше не стартует."""
        self._sched_done_run_ids.add(run_id)
        self.log.info("[%s] запуск %s… отменён: %s", self.box_id,
                      run_id[:8], reason)

    def _schedule_marked(self, run_id: str) -> bool:
        return run_id in self._sched_done_run_ids

    def _run_start_ts(self, run: dict) -> float:
        """Unix-ts старта запуске по дате+минуте локальных суток (§3.2)."""
        try:
            d = datetime.strptime(str(run["date"]), "%Y-%m-%d")
        except (KeyError, TypeError, ValueError):
            return 0.0
        minute = int(run.get("start_minute_local") or 0)
        naive = d.replace(hour=minute // 60, minute=minute % 60)
        aware = naive.replace(tzinfo=timezone(
            timedelta(minutes=self.schedule_tz_offset_min)))
        return aware.timestamp()

    def _find_due_run(self, now: float) -> tuple[dict | None, float]:
        """Первый не выполненный запуск сегодняшних суток, до которого дошло время.

        Возвращает (run|None, ts_следующего_ожидания). Опоздание сверх
        options.max_start_delay_sec → пропуск (run_id помечается skipped).
        """
        today = self._local_date(now)
        nxt_wait = now + 60.0
        best: dict | None = None
        best_ts = 0.0
        for run in self.schedule_runs:
            rid = str(run.get("run_id") or "")
            if not rid or self._schedule_marked(rid):
                continue
            if str(run.get("date")) != today or not run.get("enabled", True):
                continue
            ts = self._run_start_ts(run)
            if ts <= now:
                # Опоздание сверх max_start_delay_sec → пропуск (run помечается
                # done, событий нет). Проверка на этом месте, а НЕ после
                # выбора best: иначе при одновременном скане нескольких
                # просроченных runs опоздавший (самый ранний) «съедал» бы
                # допустимый запуск с более поздним стартом.
                if now - ts > float(self.schedule_options.get(
                        "max_start_delay_sec", 120)):
                    self._cancel_run(rid, f"опоздание {now - ts:.0f} c")
                    continue
                if best is None or ts < best_ts:      # earliest first (FIFO)
                    best, best_ts = run, ts
            else:
                nxt_wait = min(nxt_wait, ts)
        if best is not None:
            return best, now
        return None, nxt_wait

    def _step_end_ts(self, st: dict) -> float:
        """Момент окончания текущего шага с «заморозкой» при паузах (ТЗ п.3):
        пока контроллер на паузе или идёт ручной полив, остаток шага хранится
        в step_remaining и не расходуется."""
        if st.get("step_remaining") is not None:
            return st["wall_ts"] + st["step_remaining"]
        return st["wall_ts"] + st["step"]["duration_sec"]

    def _step_paused(self, st: dict, now: float) -> bool:
        return self.mode == "paused" or st.get("manual_active")

    def _begin_step(self, st: dict, now: float) -> None:
        steps = st["run"].get("steps") or []
        i = st["step_idx"]
        if i >= len(steps):
            return
        step = steps[i]
        st["step"] = step
        st["wall_ts"] = now
        st["step_remaining"] = None
        dur = max(0, int(step.get("duration_sec") or 0))
        phase = step.get("phase") or "water"
        zones = [int(z) for z in (step.get("active_zones") or [])
                 if isinstance(z, int)]
        dzones = [int(z) for z in (step.get("display_zones") or [])
                  if isinstance(z, int)] or list(zones)
        if phase == "soak":
            # §3.9: реле выключены, активные зоны пусты — показываем замачивание
            self.active_zones = []
            self.display_zones = dzones
            self.primary_zone = dzones[0] if dzones else None
            self.phase = "soak"
            self.flow_enabled = False
            self.instant_lpm = 0.0
        else:
            self.active_zones = zones
            self.display_zones = zones
            self.primary_zone = zones[0] if zones else None
            self.phase = "water"
            self.flow_enabled = True
            if not st.get("event_open"):
                st["event_open"] = True
                st["ev_zones"] = list(zones)
                self._emit_event("started", source="schedule", zones=zones,
                                 run_id=str(st["run"].get("run_id")))
        self.log.info("[%s] график v%s: шаг %s/%s (%s) зоны=%s show=%s %d c",
                      self.box_id, st["run"].get("schedule_version"),
                      step.get("seq", i + 1), len(steps), phase,
                      zones or "-", dzones, dur)
        self._publish_status(force=True)

    def _finish_step(self, st: dict, now: float) -> None:
        """Закрыть истёкший шаг: сдвинуть step_idx. Следующий шаг открывает
        вызывающий тик (_tick_schedule) — иначе «сгоревший» на этом тике шаг
        тут же открылся бы с тем же now и весь прогон съедался за один тик."""
        st["step_remaining"] = None
        st["step_idx"] += 1

    def _finish_schedule_run(self, st: dict, now: float, aborted: bool) -> None:
        run = st.get("run") or {}
        rid = str(run.get("run_id") or "")
        if rid:
            self._sched_done_run_ids.add(rid)
        if st.get("event_open"):
            # §3.9: на момент завершения прогона реле закрыты — событие несёт
            # зоны, которые реально поливались в этом прогоне (ev_zones),
            # а не текущие active_zones (после soak они пусты)
            self._emit_event("finished", source="schedule",
                             zones=st.get("ev_zones") or [],
                             run_id=rid or None, aborted=aborted)
        st["event_open"] = False
        st["run"] = None
        st["started_wall"] = None
        st["step_idx"] = 0
        st["step_remaining"] = None
        st["manual_active"] = False
        st["next_wait_ts"] = now + 30.0
        if self.mode == "schedule":
            self.mode, self.phase = "idle", "idle"
            self.active_zones = []
            self.display_zones = []
            self.primary_zone = None
            self.flow_enabled = False
            self.instant_lpm = 0.0
        self.log.info("[%s] график: запуск %s… %s", self.box_id, rid[:8],
                      "остановлен" if aborted else "завершён")
        self._publish_status(force=True)

    def _tick_schedule(self, now: float) -> None:
        """Продвижение исполнения машинограммы (вызывается из _tick_state под
        блокировкой). Режимы error/service — график стоит (аварийная
        блокировка); в paused/manual прогон ставится на паузу и продолжается
        после возврата (ТЗ п.3: «пауза при ручном режиме / возврат к графику»)."""
        if self.mode in ("error", "service", "provisioning"):
            return
        st = self._sched_state
        if st is None:
            if self.schedule_runs and self.mode == "idle":
                self._sched_state = st = {"run": None, "started_wall": None,
                                          "manual_active": False,
                                          "next_wait_ts": now}
            else:
                return
        if st.get("run") is not None:
            steps = st["run"].get("steps") or []
            if st["step_idx"] >= len(steps):
                # гонка не должна оставаться открытой (шаги могли измениться
                # при замене версии) — закрываем прогон штатно
                self._finish_schedule_run(st, now, aborted=False)
                return
            st["step"] = steps[st["step_idx"]]
            if self.mode == "paused":
                # пауза контроллера: фиксируем остаток шага (один раз),
                # таймер останавливается до resume/истечения паузы
                if st.get("step_remaining") is None:
                    st["step_remaining"] = max(0.0,
                                               st["wall_ts"]
                                               + st["step"]["duration_sec"] - now)
                return
            if self.mode == "manual" and st.get("manual_active") \
                    and self._run_source == "manual":
                # идёт ручной полив поверх графика — ждём его завершения
                # (возврат к графику сделает _finish_run)
                return
            # Продвигаемся по истёкшим шагам. Ключевое правило: следующий шаг
            # открывается строго на следующем тике (см. конец функции) —
            # _finish_step только закрывает таймер текущего шага и двигает
            # step_idx. Иначе «сгоревший» на этом тике шаг тут же открывался
            # бы с тем же now и его dur тоже считался истёкшим — весь прогон
            # съедался за один тик (дефект, из-за которого soak-шаг никогда
            # не наблюдался, а активная зона оказывалась из последнего шага).
            advanced = False
            while st.get("run") is not None \
                    and st["step_idx"] < len(st["run"].get("steps") or []) \
                    and self._step_end_ts(st) <= now:
                self._finish_step(st, now)      # только сдвиг step_idx
                advanced = True
                if st["step_idx"] >= len(st["run"].get("steps") or []):
                    self._finish_schedule_run(st, now, aborted=False)
                    break
                # следующий шаг открывается СРАЗУ, но уже от текущего now:
                # если и он истёк (длинная пауза тиков/опоздание на старт) —
                # цикл догонит его на этой же итерации; в штатном 1-секундном
                # тике за один проход сгорает не более одного шага
                self._begin_step(st, now)
            if advanced and st.get("run") is not None and self.mode == "schedule":
                self.phase = st["step"].get("phase") or "water"
                self._publish_status(force=True)
            return
        if self.mode in ("manual", "paused"):
            # график ждёт завершения ручного/приостановленного прогона
            st["next_wait_ts"] = now + 5.0
            return
        if now < st.get("next_wait_ts", 0):
            return
        run, nxt = self._find_due_run(now)
        st["next_wait_ts"] = nxt
        if run is None:
            return
        st["run"] = dict(run, schedule_version=self.schedule_version)
        st["step_idx"] = 0
        st["started_wall"] = self._run_start_ts(run)
        st["mode_before_run"] = self.mode
        self.mode, self.phase = "schedule", "waiting"
        self.log.info("[%s] график: СТАРТ запуска %s… (программа %s, %s %02d:%02d,"
                      " шагов=%d)", self.box_id, str(run.get("run_id"))[:8],
                      run.get("program_id"), run.get("date"),
                      int(run.get("start_minute_local") or 0) // 60,
                      int(run.get("start_minute_local") or 0) % 60,
                      len(run.get("steps") or []))
        self._begin_step(st, now)

    # --------------------------------------------------------------- события
    def _emit_event(self, status: str, source: str, zones: list[int],
                    aborted: bool = False, run_id: str | None = None) -> None:
        now = self._now()
        if status == "started":
            self._current_event = {
                "event_uid": str(uuid.uuid4()),
                "source": source,
                "start_ts": int(now),
                "zones": list(zones),
                "run_id": run_id,
            }
            # ТЗ §3.11 / Этап 4: сервер переводит planned→active в
            # watering_runs только по событию started — публикуем его сразу.
            p = {"protocol_version": PROTOCOL_VERSION, "box_id": self.box_id,
                 "event_uid": self._current_event["event_uid"], "ts": now,
                 "status": "started", "source": source,
                 "active_zones": list(zones), "start_ts": int(now),
                 # P1-1 (схема started): buffered обязателен
                 "buffered": False}
            if run_id:
                p["run_id"] = run_id
            self._publish("event", p)
            return
        ev = self._current_event
        if ev is None:
            return
        water_sec = max(0, int(now - ev["start_ts"]))
        volume = round(water_sec * LPM_BASE / 60.0, 1)
        # Этап 4 final (P1-1): у события завершения СВОЙ event_uid —
        # дублирование uid начала и конца конфликта с будущей дедупликацией
        # по event_uid (ТЗ §9.2). Привязка событий друг к другу — через run_id.
        payload_ev = {
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "event_uid": str(uuid.uuid4()),
            "started_event_uid": ev["event_uid"],
            "ts": now,
            "source": ev["source"],
            "active_zones": ev["zones"],
            "start_ts": ev["start_ts"],
            "end_ts": int(now),
            "water_sec": water_sec,
            "volume_liters": volume,
            # Этап 4 final (P1-1): сервер валидирует статусы событий по схеме
            # started | finished | stopped (ТЗ §3.11) — «completed» невалиден.
            "status": "stopped" if aborted else "finished",
            # P1-1: обязательные поля схем — aborted (finished/stopped) и
            # buffered (оба статуса)
            "aborted": bool(aborted),
            "buffered": False,
        }
        # Этап 4: schedule-прогон несёт run_id машинограммы — сервер связывает
        # событие с плановой записью watering_runs (план→факт).
        if ev.get("run_id"):
            payload_ev["run_id"] = ev["run_id"]
        self._publish("event", payload_ev)
        self.log.info("[%s] event %s: зоны %s, полив %d с, %.1f л%s",
                      self.box_id, status, ev["zones"], water_sec, volume,
                      f", run={ev['run_id'][:8]}…" if ev.get("run_id") else "")
        self._current_event = None

    # ------------------------------------------------------------ публикации
    def publish_hello(self) -> None:
        with self._lock:
            self.online = True
        p = self._base_state()
        p.update({
            "online": True,
            "firmware_version": self.firmware_version,
            "ip_address": _local_ip(),
            "zones_total": self.zones_total,
        })
        self._publish("hello", p)
        self.log.info("[%s] hello опубликован — online", self.box_id)
        self._publish_status(force=True)

    def _base_state(self) -> dict:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "ts": time.time(),
            "mode": self.mode,
            "phase": self.phase,
            "active_zones": list(self.active_zones),
            "display_zones": list(self.display_zones),
            "primary_zone": self.primary_zone,
            "schedule_version": self.schedule_version,
            "schedule_hash": self.schedule_hash,
            "time_valid": self.time_valid,
            "buffer_size": self.buffer_size,
            "flow_enabled": self.flow_enabled,
            "flow_total_liters": round(self.flow_total_liters, 1),
            "instant_lpm": round(self.instant_lpm, 1),
            "service_mode": self.service_mode,
            "pause_until_ts": self.pause_until_ts,
            "error_code": self.error_code,
        }

    def _publish_status(self, force: bool = False) -> None:
        if not self.online:
            return
        p = self._base_state()
        p["online"] = True
        self._publish("status", p)
        self._last_status_mono = time.monotonic()
        if force:
            self.log.debug("[%s] status (смена состояния): mode=%s phase=%s zones=%s",
                           self.box_id, self.mode, self.phase, self.active_zones)

    def _publish_flow(self) -> None:
        if self.flow_enabled:
            self.instant_lpm = LPM_BASE + (hash(str(int(time.time()))) % 20) / 10.0
            self.flow_total_liters += self.instant_lpm * 10.0 / 60.0
        else:
            self.instant_lpm = 0.0
        self._publish("flow", {
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "ts": time.time(),
            "flow_enabled": self.flow_enabled,
            "active_zones": list(self.active_zones),
            "total_liters": round(self.flow_total_liters, 1),
            "instant_lpm": round(self.instant_lpm, 1),
        })

    def publish_schedule_request(self, reason: str = "startup") -> None:
        self._publish("schedule_request", {
            "protocol_version": PROTOCOL_VERSION,
            "box_id": self.box_id,
            "ts": time.time(),
            "current_schedule_version": self.schedule_version,
            "reason": reason,
        })
        self.log.info("[%s] schedule_request (%s)", self.box_id, reason)

    # ------------------------------------------------------------- connection
    def go_offline(self, drop_connection: bool = True) -> None:
        """Имитация обрыва: флаг offline + (опционально) реальный разрыв TCP.

        Реальный разрыв заставляет брокер выдать retained-LWT → сервер видит
        offline мгновенно. При остановке клиента hello/status/flow не публикуются.
        """
        with self._lock:
            self.online = False
        self.log.warning("[%s] ОФЛАЙН (имитация обрыва связи)", self.box_id)
        if drop_connection and self._client is not None:
            # Дефект Б: перед разрывом соединения имитируем LWT — брокер при
            # корректном disconnect волю не выдаёт, публикуем retained-офлайн сами.
            self._publish_lwt_offline("lwt-sim")
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
            self._connected_evt.clear()

    def go_online(self) -> None:
        if self.online:
            return
        self.log.info("[%s] ВОЗВРАТ в онлайн: переподключение", self.box_id)
        self._client = self._make_client()
        try:
            self._client.connect(self.broker, self.port, keepalive=60)
            self._client.loop_start()
        except Exception as exc:
            self.log.error("[%s] не удалось переподключиться: %s", self.box_id, exc)
            return
        self._connected_evt.set()
        self.publish_hello()
        self.publish_schedule_request("reconnect")

    def toggle_online(self) -> None:
        if self.online:
            self.go_offline()
        else:
            self.go_online()

    def inject_error(self) -> None:
        with self._lock:
            if self.mode == "error":
                self.mode, self.phase = "idle", "idle"
                self.error_code = None
                self.log.info("[%s] авария снята", self.box_id)
            else:
                self.mode, self.phase = "error", "error"
                self.error_code = 4001
                self.log.warning("[%s] АВАРИЯ: mode=error, error_code=4001",
                                 self.box_id)
        self._publish_status(force=True)

    def manual_water_zone1(self) -> None:
        ok, msg = self._execute("zone_open", {"zone": 1, "duration_sec": 60})
        self.log.info("[%s] ручной полив (клавиша w): %s", self.box_id, msg)

    # ---------------------------------------------------------------- cycles
    def _spawn(self, target, name: str) -> None:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()

    def _main_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick_state()
                now = time.monotonic()
                if self.online and now - self._last_status_mono >= self.status_interval:
                    self._publish_status()
                period = FLOW_ACTIVE_PERIOD if self.flow_enabled else FLOW_IDLE_PERIOD
                if self.online and now - self._last_flow_mono >= period:
                    self._publish_flow()
                    self._last_flow_mono = now
            except Exception:
                self.log.exception("[%s] ошибка главного цикла", self.box_id)
            self._stop.wait(1.0)

    def _offline_after_worker(self) -> None:
        assert self.offline_after is not None
        if not self._stop.wait(float(self.offline_after)):
            self.go_offline()

    def _keyboard_loop(self) -> None:
        """Интерактивные клавиши o/e/w/s (только при интерактивном терминале)."""
        if not sys.stdin or not sys.stdin.isatty():
            return
        self.log.info("[%s] клавиши: o=офлайн/онлайн, e=авария, w=полив зоны 1, "
                      "s=schedule_request", self.box_id)
        while not self._stop.is_set():
            try:
                line = sys.stdin.readline()
            except Exception:
                return
            if not line:
                return
            key = line.strip().lower()[:1]
            if key == "o":
                self.toggle_online()
            elif key == "e":
                self.inject_error()
            elif key == "w":
                self.manual_water_zone1()
            elif key == "s":
                self.publish_schedule_request("manual_key")
            elif key in ("q",):
                self._stop.set()


def _local_ip() -> str:
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Эмулятор контроллера автополива (Этап 3)")
    ap.add_argument("--box-id", default="BOX-EMUL-01",
                    help="box_id контроллера (должен существовать в БД сервера)")
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--user", default=None, help="логин MQTT на брокере")
    ap.add_argument("--password", default=None, help="пароль MQTT на брокере")
    ap.add_argument("--status-interval", type=float, default=30.0,
                    help="интервал status, с (для ручной проверки удобно 5)")
    ap.add_argument("--zones", type=int, default=16)
    ap.add_argument("--offline-after", type=float, default=None,
                    help="через N секунд имитировать обрыв связи (LWT)")
    ap.add_argument("--db", default=None,
                    help="путь к poliv.db: автопрокидка box_id в controllers, "
                         "если контроллера ещё нет (по умолчанию data/poliv.db)")
    return ap


def ensure_box_in_db(box_id: str, db_path: Path) -> None:
    """Если box_id отсутствует в таблице controllers — добавить (ручная проверка).

    Сервер узнаёт box_id только из зарегистрированных контроллеров; чтобы не
    гонять UI ради ручной проверки, эмулятор может прокинуть запись сам.
    Колонки — по миграции 0001 (zones_count в controllers нет, зоны — таблица zones).
    """
    if not db_path.exists():
        return
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT 1 FROM controllers WHERE box_id=?", (box_id,))
        if cur.fetchone() is None:
            now = utcnow_iso()
            conn.execute(
                "INSERT INTO controllers(name, box_id, created_at, updated_at)"
                " VALUES (?,?,?,?)",
                (f"Эмулятор {box_id}", box_id, now, now))
            conn.commit()
            print(f"[emulator] контроллер {box_id} добавлен в БД {db_path}")
        conn.close()
    except sqlite3.Error as exc:
        print(f"[emulator] не удалось проверить БД ({db_path}): {exc}")


def setup_logging(log_file: Path) -> logging.Logger:
    log = logging.getLogger("poliv.emulator")
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)
    except OSError:
        pass
    return log


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    log = setup_logging(ROOT / "logs" / "emulator.log")
    db_path = Path(args.db) if args.db else ROOT / "data" / "poliv.db"
    ensure_box_in_db(args.box_id, db_path)

    sim = ControllerSim(
        box_id=args.box_id, broker=args.broker, port=args.port,
        user=args.user, password=args.password,
        status_interval=args.status_interval, zones=args.zones,
        offline_after=args.offline_after, log=log)
    sim.start()
    log.info("[%s] эмулятор запущен: broker=%s:%d, status=%gs, зон=%d",
             args.box_id, args.broker, args.port, args.status_interval, args.zones)

    stop_sig = threading.Event()

    def _sig(_signum, _frame):
        stop_sig.set()

    for signame in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signame):
            try:
                signal.signal(getattr(signal, signame), _sig)
            except ValueError:
                pass  # не главный поток (тесты)
    try:
        while not stop_sig.wait(1.0) and not sim._stop.is_set():
            pass
    except KeyboardInterrupt:
        pass
    sim.stop()
    log.info("[%s] эмулятор остановлен", args.box_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
