"""Регресс-тесты дефектов эмулятора (config_robustness_and_emulator_fix).

Блок 3 (Дефект A): finished-событие обязано нести source прогона; вызов
_finish_run() без аргументов не должен падать по сигнатуре _emit_event.

Блок 4 (Дефект B): штатный offline/disconnect не триггерит волю брокера,
поэтому эмулятор сам публикует retained-LWT (online=false) в poliv/{box}/lwt.

Тесты работают БЕЗ брокера: paho-клиент подменяется мок-объектом, который
записывает publish(topic, payload, qos, retain).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from emulator.controller_sim import ControllerSim  # noqa: E402


class FakeClient:
    """Мок paho-клиента: пишет все публикации, остальное — no-op."""

    def __init__(self):
        self.published: list[tuple[str, dict, int, bool]] = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, json.loads(payload), int(qos), bool(retain)))

    def loop_stop(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self, *a, **k):
        pass


def _make_sim() -> ControllerSim:
    sim = ControllerSim(box_id="BOX-TEST", status_interval=5.0)
    sim._client = FakeClient()
    return sim


# ------------------------------------------------------------------ Дефект A
def test_manual_water_finished_event_has_source_manual():
    """manual-полив -> тики water/soak -> event finished c source='manual'."""
    sim = _make_sim()

    # Запуск ручного полива зоны 3 на 1 с (машина состояний обычная).
    sim._start_manual_water(zone=3, duration_sec=1)
    assert sim.mode == "manual" and sim.phase == "water"
    assert sim._run_source == "manual"

    # Прокручиваем время: полив завершён -> фаза soak, затем finish.
    sim._water_until = time.time() - 0.1          # полив истёк
    sim._tick_state()                              # water -> soak
    assert sim.phase == "soak"
    sim._soak_until = time.time() - 0.1            # замачивание истекло
    sim._tick_state()                              # soak -> finished

    events = [p for p in sim._client.published if p[0].endswith("/event")]
    # Этап 4: прогон публикует пару событий started/finished (план→факт);
    # проверяем finished-событие (source='manual' — Дефект A)
    finished = [e for e in events if e[1].get("status") == "completed"]
    assert len(finished) == 1, f"ожидалось 1 finished-событие, есть {events}"
    topic, payload, qos, retain = finished[0]
    assert topic == "poliv/BOX-TEST/event"
    assert payload["source"] == "manual"           # Дефект A: source присутствует
    assert payload["status"] == "completed"
    assert payload["active_zones"] == [3]

    # После завершения прогона: источник сброшен, состояние idle.
    assert sim._run_source is None
    assert sim.mode == "idle" and sim.phase == "idle"


def test_finish_run_without_current_event_is_noop():
    """_finish_run() вне прогона не падает и ничего не публикует."""
    sim = _make_sim()
    sim._finish_run()                               # без исключений
    assert not [p for p in sim._client.published if p[0].endswith("/event")]


# ------------------------------------------------------------------ Дефект B
def test_go_offline_publishes_retained_lwt():
    sim = _make_sim()
    sim.go_offline(drop_connection=True)

    lwt = [p for p in sim._client.published if p[0] == "poliv/BOX-TEST/lwt"]
    assert len(lwt) == 1, f"ожидалась 1 LWT-публикация, есть {lwt}"
    topic, payload, qos, retain = lwt[0]
    assert retain is True                           # retained — как настоящая воля
    assert qos == 1
    assert payload["online"] is False
    assert payload["reason"] == "lwt-sim"
    assert payload["box_id"] == "BOX-TEST"
    assert "protocol_version" in payload and "ts" in payload
    assert sim.online is False


def test_stop_publishes_retained_lwt_shutdown():
    sim = _make_sim()
    sim.stop()                                      # online ещё True

    lwt = [p for p in sim._client.published if p[0] == "poliv/BOX-TEST/lwt"]
    assert len(lwt) == 1
    topic, payload, qos, retain = lwt[0]
    assert retain is True and qos == 1
    assert payload["online"] is False
    assert payload["reason"] == "shutdown"


def test_go_online_does_not_publish_retained_lwt():
    """go_online() — только переподключение + hello; retained в lwt НЕ шлём."""
    sim = _make_sim()
    sim.go_offline(drop_connection=True)

    class NoConnectClient(FakeClient):
        def connect(self, *a, **k):        # без реального брокера/сетевых попыток
            raise ConnectionRefusedError("no broker in tests")

        def loop_start(self):
            pass

    real_make = sim._make_client
    new_client = NoConnectClient()         # будет создан go_online'ом
    sim._make_client = lambda: new_client
    try:
        sim.go_online()                    # не должен упасть
    finally:
        sim._make_client = real_make

    # Проверяем, что LWT-топик НЕ затронут (go_online retained-офлайн не шлёт):
    # ни старый клиент (1 публикация от go_offline), ни новый.
    lwt_old = [p for p in sim._client.published if p[0] == "poliv/BOX-TEST/lwt"] \
        if sim._client is not new_client else []
    lwt_new = [p for p in new_client.published if p[0] == "poliv/BOX-TEST/lwt"]
    assert lwt_new == []
    assert len(lwt_old) + len(lwt_new) <= 1   # ничего нового в lwt не добавилось
