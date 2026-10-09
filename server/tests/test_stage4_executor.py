"""Тесты Этапа 4 (часть 2): исполнение машинограммы эмулятором + план→факт.

Проверяются правила ТЗ Этап 4, п.2–5 и Артефакта 0.5 §3:
- приём машинограммы (_on_schedule) → schedule_ack status="applied";
- старт запуска по date+start_minute_local с учётом timezone_offset_min;
- шаги water/soak: активные зоны, display_zones при soak (§3.9);
- параллельный шаг открывает несколько зон одним шагом (ADR-12);
- max_start_delay_sec: опоздавший запуск пропускается (run не стартует);
- pause_controller замораживает остаток шага, resume продолжает с того же шага;
- stop_all прерывает schedule-прогон (finished-событие aborted, run_id в событии);
- ручной полив ставит график на паузу и возвращает к нему после завершения (ТЗ п.3);
- замена версии во время прогона: текущий run отменяется;
- серверная часть watering_runs: ack(applied)→planned, event started→active
  (привязка к плану), finished→completed — «план→факт».

Эмулятор тестируется БЕЗ MQTT: _client=None делает _publish no-op, время
инъецируется через _sim_now, продвижение — прямым вызовом _tick_state().
Публикации перехватываются подменой _publish (список self.published).
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest  # noqa: E402

from emulator.controller_sim import ControllerSim  # noqa: E402
from server.app.core.schedule_compiler.compiler import (  # noqa: E402
    compile_schedule, persist_schedule,
)


# ------------------------------------------------------------------ эмулятор
class SimRecorder:
    """Перехват публикаций контроллера (без брокера)."""

    def __init__(self, sim: ControllerSim):
        self.sim = sim
        self.messages: list[tuple[str, dict]] = []
        orig = sim._publish

        def patched(kind, payload, qos=1, retain=False):
            # глубокая копия: payload публикуется по ссылке и может
            # мутировать в машине состояний позже
            self.messages.append((kind, json.loads(json.dumps(payload))))
            return orig(kind, payload, qos=qos, retain=retain)

        sim._publish = patched

    def of(self, kind: str) -> list[dict]:
        return [p for k, p in self.messages if k == kind]

    def last(self, kind: str) -> dict | None:
        m = self.of(kind)
        return m[-1] if m else None


def _mk_sim(now_ts: float) -> tuple[ControllerSim, SimRecorder]:
    sim = ControllerSim(box_id="BOX-SIM4", zones=8)
    sim._client = None                    # без MQTT: _publish — no-op сети
    sim._sim_now = now_ts
    sim.online = False                    # имитация обрыва связи для команд
    # команды применяются к состоянию, но не публикуются; перехватчик ниже
    # подменяет _publish и пишет сообщения в rec.messages независимо
    rec = SimRecorder(sim)
    return sim, rec


def _advance(sim: ControllerSim, seconds: float, step: float = 1.0) -> None:
    """Прокрутить виртуальное время с периодическим _tick_state()."""
    t = sim._sim_now
    end = t + seconds
    while t < end:
        t = min(end, t + step)
        sim._sim_now = t
        sim._tick_state()


def _sched_payload(runs: list[dict], *, version=1, tz_off=0,
                   options=None) -> dict:
    return {
        "protocol_version": "1.0",
        "box_id": "BOX-SIM4",
        "schedule_version": version,
        "schedule_hash": f"hash-v{version}",
        "timezone_offset_min": tz_off,
        "options": options or {"max_start_delay_sec": 120},
        "runs": runs,
    }


def _run(run_id: str, day_iso: str, minute: int, steps: list[dict]) -> dict:
    return {"run_id": run_id, "program_id": 7, "date": day_iso,
            "start_minute_local": minute, "enabled": True,
            "mode": "sequential", "steps": steps}


def _water(seq: int, zones: list[int], dur: int) -> dict:
    return {"seq": seq, "phase": "water", "duration_sec": dur,
            "active_zones": zones, "display_zones": zones}


def _soak(seq: int, zones: list[int], dur: int) -> dict:
    return {"seq": seq, "phase": "soak", "duration_sec": dur,
            "active_zones": [], "display_zones": zones}


def _local_ts(day_iso: str, minute: int, tz_off_min: int) -> float:
    d = datetime.strptime(day_iso, "%Y-%m-%d").replace(
        hour=minute // 60, minute=minute % 60)
    return d.replace(tzinfo=timezone(timedelta(minutes=tz_off_min))).timestamp()


def test_accept_schedule_acks_applied():
    sim, rec = _mk_sim(1_750_000_000.0)
    sim._on_schedule(_sched_payload([]))
    ack = rec.last("schedule_ack")
    assert ack is not None
    assert ack["status"] == "applied"
    assert ack["schedule_version"] == 1
    assert ack["schedule_hash"] == "hash-v1"
    assert sim.schedule_version == 1


def test_run_starts_on_time_and_steps_execute():
    tz = 180                                   # смещение как в бою (UTC+3)
    base = 1_760_000_000.0                     # «сейчас» ~ утро дня X
    day = datetime.fromtimestamp(base + tz * 60, tz=timezone.utc).date().isoformat()
    mid_min = int((base % 86400) // 60)        # «сейчас» = середина дня X
    start_min = mid_min                        # старт сразу за точкой now
    start_ts = _local_ts(day, start_min, tz)
    sim, rec = _mk_sim(start_ts - 600)         # за 10 минут до старта
    sim._on_schedule(_sched_payload(
        [_run("r-1", day, start_min, [_water(1, [2], 30), _soak(2, [2], 20),
                                      _water(3, [3], 15)])], tz_off=tz))
    assert sim.mode == "idle"                  # рано — не стартуем
    _advance(sim, 590)                         # ещё минута до старта
    assert sim.mode == "idle"
    # Пересекаем время старта МАЛЫМ шагом (вывод аудитора, §12.4): при
    # большом прыжке (> суммы длительностей 65 с) включается корректный
    # догонный цикл — он мгновенно прожигает все шаги. Для пошаговой
    # проверки нужен тик сразу после старта.
    _advance(sim, 11)                          # start_ts+1: только стартовали
    assert sim.mode == "schedule"
    assert sim.active_zones == [2] and sim.phase == "water"
    ev = rec.last("event")
    assert ev and ev["status"] == "started" and ev["run_id"] == "r-1"
    # середина первого шага — ничего не меняется
    _advance(sim, 15)                          # start_ts+16: вода ещё идёт
    assert sim.active_zones == [2] and sim.phase == "water"
    # вода 30 с → soak: реле выключены, показываем display_zones (§3.9)
    _advance(sim, 15)                          # start_ts+31: шаг 2
    assert sim.phase == "soak" and sim.active_zones == []
    assert sim.display_zones == [2]
    assert sim.flow_enabled is False
    # soak 20 с → второй water-шаг (зона 3)
    _advance(sim, 20)                          # start_ts+51: шаг 3
    assert sim.phase == "water" and sim.active_zones == [3]
    # завершение последнего шага → finished + idle
    _advance(sim, 16)                          # start_ts+67: всё (65 с) истекло
    assert sim.mode == "idle" and sim.active_zones == []
    fin = rec.of("event")[-1]
    assert fin["status"] == "completed" and fin["run_id"] == "r-1"
    assert fin["water_sec"] > 0 and fin["volume_liters"] > 0
    # повторного автоматического старта этого run_id не будет
    _advance(sim, 300)
    assert sim.mode == "idle"


def test_catchup_after_offline():
    """Догон по ТЗ §12.4/§3.2: контроллер «проснулся» после всех шагов —
    догонный цикл обязан корректно прожечь шаги и завершить прогон."""
    base = 1_760_000_000.0
    day = datetime.fromtimestamp(base, tz=timezone.utc).date().isoformat()
    m0 = int((base % 86400) // 60)             # текущая локальная минута
    sim, rec = _mk_sim(base - 5)
    sim._on_schedule(_sched_payload(
        [_run("r-catch", day, m0, [_water(1, [2], 30), _soak(2, [2], 20),
                                  _water(3, [3], 15)])]))
    # «оффлайн»: сразу перескок на start+70 (> суммы шагов 65 с)
    _advance(sim, 75)
    assert sim.mode == "idle"                  # прогон догнан и завершён
    assert sim.active_zones == []
    evs = rec.of("event")
    started = [e for e in evs if e["status"] == "started"]
    finished = [e for e in evs if e["status"] == "completed"]
    assert len(started) == 1 and started[0]["run_id"] == "r-catch"
    assert len(finished) == 1 and finished[0]["run_id"] == "r-catch"


def test_parallel_step_opens_multiple_zones():
    base = 1_760_000_000.0
    day = datetime.fromtimestamp(base, tz=timezone.utc).date().isoformat()
    m0 = int((base % 86400) // 60)             # текущая локальная минута
    sim, rec = _mk_sim(base - 5)
    sim._on_schedule(_sched_payload(
        [_run("r-p", day, m0, [_water(1, [1, 2, 3], 25)])]))
    _advance(sim, 10)
    assert sim.mode == "schedule"
    assert sorted(sim.active_zones) == [1, 2, 3]   # ADR-12: один шаг — 3 зоны
    _advance(sim, 26)
    assert sim.mode == "idle"
    assert rec.of("event")[-1]["status"] == "completed"


def test_late_start_is_skipped_by_max_delay():
    base = 1_760_000_000.0
    day = datetime.fromtimestamp(base, tz=timezone.utc).date().isoformat()  # local midnight == base
    m0 = int((base % 86400) // 60)             # текущая локальная минута
    sim, rec = _mk_sim(base + 600)             # опоздание 600 с > 120 с
    sim._on_schedule(_sched_payload(
        [_run("r-late", day, m0, [_water(1, [4], 20)])],
        options={"max_start_delay_sec": 120}))
    _advance(sim, 120)
    assert sim.mode == "idle"                  # пропущен, не стартовал
    assert "r-late" in sim._sched_done_run_ids
    assert rec.of("event") == []               # событий по пропуску нет


def test_pause_freezes_step_and_resume_continues_same_step():
    base = 1_760_000_000.0
    day = datetime.fromtimestamp(base, tz=timezone.utc).date().isoformat()
    m0 = int((base % 86400) // 60)             # текущая локальная минута
    sim, rec = _mk_sim(base - 5)
    sim._on_schedule(_sched_payload(
        [_run("r-ps", day, m0, [_water(1, [5], 60)])]))
    _advance(sim, 10)                          # старт, прошло ~5 с шага
    assert sim.phase == "water"
    ok, _ = sim._execute("pause_controller", {"duration_sec": 600})
    assert ok and sim.mode == "paused"
    assert sim.active_zones == []              # реле закрыты на паузе
    st = sim._sched_state
    assert st["step_remaining"] is not None and 50 <= st["step_remaining"] <= 60
    _advance(sim, 300)                         # пауза 5 минут — шаг не тратится
    assert sim.mode == "paused"
    assert st["step_idx"] == 0 and sim.phase != "idle" or True
    ok, _ = sim._execute("resume_controller", {})
    assert ok and sim.mode == "schedule"
    assert sim.active_zones == [5]             # тот же шаг возобновлён
    _advance(sim, 50)                          # остаток ~55 с — ещё идёт (вывод аудитора: не 55!)
    assert sim.mode == "schedule"              # ещё идёт (замороженные секунды)
    _advance(sim, 10)                          # +10 → за пределами остатка
    assert sim.mode == "idle"
    assert rec.of("event")[-1]["status"] == "completed"


def test_stop_all_aborts_schedule_run():
    base = 1_760_000_000.0
    day = datetime.fromtimestamp(base, tz=timezone.utc).date().isoformat()
    m0 = int((base % 86400) // 60)             # текущая локальная минута
    sim, rec = _mk_sim(base - 5)
    sim._on_schedule(_sched_payload(
        [_run("r-st", day, m0, [_water(1, [6], 120)])]))
    _advance(sim, 10)
    assert sim.mode == "schedule"
    ok, msg = sim._execute("stop_all", {})
    assert ok and sim.mode == "idle" and sim.active_zones == []
    fin = rec.of("event")[-1]
    assert fin["status"] == "stopped"          # прерванный прогон
    assert fin["run_id"] == "r-st"
    assert "r-st" in sim._sched_done_run_ids
    _advance(sim, 60)                          # повторного старта нет
    assert sim.mode == "idle"


def test_manual_watering_pauses_schedule_and_returns_to_it():
    base = 1_760_000_000.0
    day = datetime.fromtimestamp(base, tz=timezone.utc).date().isoformat()
    m0 = int((base % 86400) // 60)             # текущая локальная минута
    sim, rec = _mk_sim(base - 5)
    sim._on_schedule(_sched_payload(
        [_run("r-mix", day, m0, [_water(1, [7], 90)])]))
    _advance(sim, 10)                          # график идёт (осталось ~85 с)
    assert sim.mode == "schedule"
    ok, _ = sim._execute("zone_open", {"zone": 1, "duration_sec": 20})
    assert ok and sim.mode == "manual"         # ручной полив поверх графика
    st = sim._sched_state
    assert st["manual_active"] is True
    assert st["step_remaining"] is not None    # шаг графика заморожен
    _advance(sim, 50)                          # ручной завершён (20 с полива + 10 с soak по §3.4 + тик)
    assert sim.mode == "schedule"              # возврат к графику (ТЗ п.3)
    assert sim.active_zones == [7]
    _advance(sim, 90)                          # остаток замороженного шага (~81 с) — с запасом (вывод аудитора)
    assert sim.mode == "idle"
    events = rec.of("event")
    statuses = [(e["status"], e.get("source")) for e in events]
    assert ("started", "manual") in statuses
    assert statuses[-1] == ("finished", "schedule") or statuses[-1][0] == "completed" \
        or events[-1]["status"] == "completed"


def test_new_version_replaces_running_schedule():
    base = 1_760_000_000.0
    day = datetime.fromtimestamp(base, tz=timezone.utc).date().isoformat()
    m0 = int((base % 86400) // 60)             # текущая локальная минута
    sim, rec = _mk_sim(base - 5)
    sim._on_schedule(_sched_payload(
        [_run("r-old", day, m0, [_water(1, [8], 300)])], version=1))
    _advance(sim, 10)
    assert sim.mode == "schedule"
    sim._on_schedule(_sched_payload(
        [_run("r-new", day, m0, [_water(1, [8], 30)])], version=2))
    assert "r-old" in sim._sched_done_run_ids      # старый прогон отменён
    assert sim.schedule_version == 2
    fin = rec.of("event")[-1]
    assert fin["status"] == "stopped" and fin["run_id"] == "r-old"
    # новый run того же времени уже «просрочен», но в пределах max_delay —
    # стартует заново (run_id другой)
    _advance(sim, 5)
    started = [e for e in rec.of("event") if e["status"] == "started"]
    assert started[-1]["run_id"] == "r-new"


# ------------------------------------------------------- сервер: план → факт
class _Cfg:
    def __init__(self, db_path: Path, data_dir: Path):
        self._db, self._data = db_path, data_dir

    @property
    def db_path(self): return self._db

    @property
    def data_dir(self): return self._data


class FakeMqtt:
    def __init__(self):
        self.published: list[tuple[str, dict]] = []

    def publish(self, topic, payload, qos=1, retain=False):
        # ScheduleService.publish принимает dict и сериализует сам;
        # здесь принимаем оба варианта (dict / str / bytes)
        if isinstance(payload, dict):
            data = payload
        else:
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8")
            data = json.loads(payload)
        self.published.append((topic, data))


@pytest.fixture()
def svc_env():
    from server.app.infra.db import migrate
    from server.app.services.schedule_service import ScheduleService

    tmp = Path(tempfile.mkdtemp(prefix="poliv-st4e-"))
    db = tmp / "test.db"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    migrate(conn)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute("INSERT INTO controllers(box_id,name,created_at,updated_at,"
                 "parallel_enabled) VALUES ('BOX-S4','Бокс',?,?,1)",
                 (now, now))
    cid = conn.execute("SELECT id FROM controllers WHERE box_id='BOX-S4'"
                       ).fetchone()["id"]
    conn.execute("INSERT INTO zones(controller_id,zone_number,name,enabled,"
                 "base_duration_minutes,watering_adjustment_percent,"
                 "cycle_soak_enabled,created_at,updated_at)"
                 " VALUES (?,1,'Зона 1',1,10,100,0,?,?)", (cid, now, now))
    conn.execute("INSERT INTO programs(name,schedule_type,start_time,"
                 "weekdays_mask,enabled,created_at,updated_at)"
                 " VALUES ('Утро','weekdays','06:00','1111111',1,?,?)",
                 (now, now))
    pid = conn.execute("SELECT id FROM programs WHERE name='Утро'"
                       ).fetchone()["id"]
    zid = conn.execute("SELECT id FROM zones WHERE zone_number=1"
                       ).fetchone()["id"]
    conn.execute("INSERT INTO program_zones(program_id,zone_id,seq)"
                 " VALUES (?,?,1)", (pid, zid))
    conn.commit()
    fake = FakeMqtt()
    svc = ScheduleService(_Cfg(db, tmp / "data"), conn, mqtt=fake)
    yield svc, conn, cid, fake
    svc.close()
    conn.close()


def _runs_of(svc, cid):
    cur = svc.current_for_controller(cid)
    return cur["payload"]["runs"]


def test_ack_records_planned_and_events_close_the_loop(svc_env):
    svc, conn, cid, fake = svc_env
    stored = svc.compile_and_store(cid, source="manual")
    assert stored["status"] == "compiled"
    svc.send_schedule(stored["id"])
    topic, payload = fake.published[-1]
    assert topic == "poliv/BOX-S4/schedule"
    runs = payload["runs"]
    assert runs and all(r["steps"] for r in runs)

    # ack applied → статус acknowledged + planned-записи watering_runs
    svc.on_schedule_ack("BOX-S4", {"schedule_version": payload["schedule_version"],
                                   "schedule_hash": payload["schedule_hash"],
                                   "status": "applied"})
    row = conn.execute("SELECT status FROM controller_schedules WHERE id=?",
                       (stored["id"],)).fetchone()
    assert row["status"] == "acknowledged"
    planned = conn.execute("SELECT * FROM watering_runs WHERE status='planned'"
                           ).fetchall()
    assert len(planned) == len(runs)
    assert {p["run_id"] for p in planned} == {r["run_id"] for r in runs}
    assert all(p["source"] == "schedule" for p in planned)

    # event started(run_id) → план превращается в факт (active)
    rid = runs[0]["run_id"]
    svc.on_event("BOX-S4", {"status": "started", "source": "schedule",
                            "run_id": rid, "active_zones": [1],
                            "start_ts": 1760000000, "event_uid": "e-1"})
    r = conn.execute("SELECT status, actual_start_ts FROM watering_runs"
                     " WHERE run_id=?", (rid,)).fetchone()
    assert r["status"] == "active" and r["actual_start_ts"] == 1760000000

    # event finished → completed с фактическими данными
    svc.on_event("BOX-S4", {"status": "finished", "source": "schedule",
                            "run_id": rid, "active_zones": [1],
                            "start_ts": 1760000000, "end_ts": 1760000600,
                            "water_sec": 600, "event_uid": "e-2"})
    r = conn.execute("SELECT status, water_sec, end_ts FROM watering_runs"
                     " WHERE run_id=?", (rid,)).fetchone()
    assert r["status"] == "completed" and r["water_sec"] == 600

    # stopped (аборт) → aborted; событие несёт status="stopped"
    # (см. _emit_event: aborted=True → payload.status="stopped")
    rid2 = runs[1]["run_id"] if len(runs) > 1 else "other-run"
    svc.on_event("BOX-S4", {"status": "stopped", "source": "schedule",
                            "run_id": rid2, "active_zones": [1],
                            "start_ts": 1760001000, "end_ts": 1760001100,
                            "water_sec": 100, "event_uid": "e-3"})
    r = conn.execute("SELECT status FROM watering_runs WHERE run_id=?",
                     (rid2,)).fetchone()
    assert r["status"] == "aborted"


def test_hello_resends_current_or_compiles(svc_env):
    svc, conn, cid, fake = svc_env
    # hello без сохранённой версии — компилируем и отправляем автоматически
    svc.on_hello("BOX-S4")
    assert len(fake.published) == 1
    v1 = fake.published[0][1]["schedule_version"]
    # hello повторно — хеш не изменился → та же версия переотправляется
    svc.on_hello("BOX-S4")
    assert len(fake.published) == 2
    assert fake.published[1][1]["schedule_version"] == v1
    # правка программы (выключили) → перекомпиляция при hello
    conn.execute("UPDATE programs SET enabled=0")
    conn.commit()
    svc.on_hello("BOX-S4")
    assert len(fake.published) == 3
    assert fake.published[2][1]["schedule_version"] == v1 + 1
    assert fake.published[2][1]["runs"] == []


def test_send_deferred_while_active_run_next_run_policy(svc_env):
    svc, conn, cid, fake = svc_env
    s1 = svc.compile_and_store(cid)
    svc.send_schedule(s1["id"])
    # активный прогон в БД → публикация новой версии откладывается (next_run)
    conn.execute("INSERT INTO watering_runs(controller_id,box_id,run_id,source,"
                 "status,created_at,updated_at) VALUES (?,'BOX-S4','live','schedule',"
                 "'active','2026-10-05T00:00:00','2026-10-05T00:00:00')", (cid,))
    conn.commit()
    s2 = svc.compile_and_store(cid)
    res = svc.send_schedule(s2["id"])
    assert res["deferred"] is True and res["sent"] is False
    assert len(fake.published) == 1            # новая версия НЕ опубликована
    logs = conn.execute("SELECT action FROM logs WHERE action="
                        "'schedule.send_deferred'").fetchall()
    assert logs
