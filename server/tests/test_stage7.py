"""Тесты Этапа 7: погодные корректировки и сезонность.

Проверяемые контракты (ТЗ Этап 7, Артефакты 0.4/0.5/0.6/0.7 §17.7):
- open_meteo_client: мок httpx — успех, таймаут, 5xx, 4xx (graceful None),
  кэш 5 минут (повторный запрос не дергает сеть), User-Agent с версией;
- weather_service: CRUD правил + валидация, evaluate_rules (все операторы
  gt/lt/eq/gte/lte и все действия skip/reduce/increase), adjust_steps с
  клампом min/max duration, offline-статус current_conditions;
- интеграция с компилятором: правило skip пропускает запуски ДО генерации
  машинограммы (reason_code=weather_skip), reduce_percent меняет
  длительности, weather.enabled=false / rules_enabled=false — no-op,
  offline_policy=disable_corrections при несвежих данных — поправки
  отключаются, use_last — работают на последних сохранённых;
  уже запущенный полив НЕ прерывается (правила влияют только на новую
  машинограмму); журнал weather_rule_applications заполняется schedule_service;
- REST API /api/v1/weather/*: ACL viewer/operator/admin, CRUD, test-dry-run
  (не пишет журнал), applications с пагинацией;
- миграция 0009: up (таблицы+настройки), идемпотентность, down (откат).

Сеть не используется: httpx.get подменяется monkeypatch'ем.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest  # noqa: E402

from server.app.infra import open_meteo_client as omc  # noqa: E402
from server.app.services import weather_service as wx  # noqa: E402


# ------------------------------------------------------------------ фикстуры
def _migrated_conn() -> tuple[sqlite3.Connection, Path]:
    """Чистая БД со всеми миграциями проекта (0001..0009)."""
    tmpdir = Path(tempfile.mkdtemp(prefix="poliv-st7-"))
    conn = sqlite3.connect(tmpdir / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    from server.app.infra.db import migrate
    migrate(conn)
    return conn, tmpdir


@pytest.fixture()
def conn():
    c, _tmp = _migrated_conn()
    yield c
    c.close()


def _set_setting(conn, key: str, value) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO settings(key, value_json, updated_at) VALUES (?,?,?)",
        (key, json.dumps(value), datetime.now(timezone.utc).isoformat()))
    conn.commit()


def _mk_controller(conn, box_id="BOX-W1") -> dict:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur = conn.execute(
        "INSERT INTO controllers(box_id, name, created_at, updated_at) "
        "VALUES (?,?,?,?)", (box_id, f"Бокс {box_id}", now, now))
    conn.commit()
    return dict(conn.execute("SELECT * FROM controllers WHERE id=?",
                             (cur.lastrowid,)).fetchone())


def _mk_zone(conn, controller_id: int, num: int) -> int:
    cur = conn.execute(
        """INSERT INTO zones(controller_id, zone_number, name, enabled,
                             base_duration_minutes, watering_adjustment_percent,
                             cycle_soak_enabled, soak_after_last,
                             created_at, updated_at)
           VALUES (?,?,?,?,?,100,0,0,'2026-01-01T00:00:00+00:00',
                   '2026-01-01T00:00:00+00:00')""",
        (controller_id, num, f"Зона {num}", 1, 10))
    conn.commit()
    return int(cur.lastrowid)


def _mk_program(conn, name="Полив", start_time="06:00",
                created_at="2026-01-05T00:00:00+00:00") -> int:
    # created_at — понедельник: интервальные якоря стабильно попадают в
    # тестовое окно компиляции (today=понедник 2026-10-26).
    cur = conn.execute(
        """INSERT INTO programs(name, description, enabled, schedule_type,
                                weekdays_mask, interval_days, start_time,
                                created_at, updated_at)
           VALUES (?,NULL,1,'weekdays','1111111',NULL,?,?,?)""",
        (name, start_time, created_at, created_at))
    conn.commit()
    return int(cur.lastrowid)


def _link(conn, program_id: int, zone_id: int, seq: int = 1) -> None:
    conn.execute(
        "INSERT INTO program_zones(program_id, zone_id, seq) VALUES (?,?,?)",
        (program_id, zone_id, seq))
    conn.commit()


def _add_obs(conn, *, precip_mm=None, temp_c=None, humidity=None, wind=None,
             ts: int | None = None) -> None:
    ts = ts or int(time.time())
    conn.execute(
        """INSERT INTO weather_observations(observed_at, ts, temp_c,
               humidity_pct, precip_mm, precipitation_mm, wind_ms, source,
               raw_json)
           VALUES (?,?,?,?,?,?,?, 'open_meteo', '{}')""",
        (datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds"),
         ts, temp_c, humidity, precip_mm, precip_mm, wind))
    conn.commit()


def _add_forecast(conn, hours_ahead: int = 24, *, precip_mm=None,
                  precip_prob=None, temp_c=None) -> int:
    now = datetime.now(timezone.utc)
    inserted = 0
    for i in range(hours_ahead):
        t_from = (now + timedelta(hours=i)).isoformat(timespec="seconds")
        t_to = (now + timedelta(hours=i + 1)).isoformat(timespec="seconds")
        conn.execute(
            """INSERT INTO weather_forecasts(forecasted_at, valid_from, valid_to,
                   temp_c, humidity_pct, precip_probability_pct, precip_mm,
                   condition_code, source)
               VALUES (?,?,?,?,?,?,?,61,'open_meteo')""",
            (now.isoformat(timespec="seconds"), t_from, t_to, temp_c, 70.0,
             precip_prob, precip_mm))
        inserted += 1
    conn.commit()
    return inserted


# ==================================================== 1. open_meteo_client
class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


CURRENT_OK = {"current": {"time": "2026-10-26T12:00", "temperature_2m": 25.0,
                          "relative_humidity_2m": 60.0, "wind_speed_10m": 7.2,
                          "precipitation": 0.0, "weather_code": 1}}
def _forecast_times(n: int = 24) -> list[str]:
    """Часы прогноза ОТ «сейчас» (UTC): иначе фильтры актуальных строк
    (valid_to >= now) отсекают прошедшие часы."""
    base = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return [(base + timedelta(hours=i)).isoformat(timespec="seconds")[:16]
            for i in range(n)]


FORECAST_OK = {"hourly": {
    "time": _forecast_times(),
    "temperature_2m": [20.0] * 24,
    "relative_humidity_2m": [65.0] * 24,
    "precipitation_probability": [80] * 24,
    "precipitation": [3.5] * 24,
    "weather_code": [61] * 24}}


@pytest.fixture(autouse=True)
def _clear_cache():
    omc.reset_cache()
    yield
    omc.reset_cache()


def test_client_fetch_current_success(monkeypatch):
    calls = []

    def fake_get(url, params=None, timeout=None, headers=None):
        calls.append((url, params, timeout, headers))
        return _Resp(CURRENT_OK)

    monkeypatch.setattr(omc.httpx, "get", fake_get)
    cur = omc.fetch_current(55.0, 37.0)
    assert cur is not None
    assert cur["temp_c"] == 25.0 and cur["humidity_pct"] == 60.0
    assert cur["precip_mm"] == 0.0
    assert cur["wind_ms"] == pytest.approx(2.0, abs=0.01)  # 7.2 км/ч -> м/с
    assert calls[0][2] == omc.TIMEOUT_SEC == 10.0
    assert "poliv-server/" in calls[0][3]["User-Agent"]


def test_client_fetch_forecast_success_and_normalize(monkeypatch):
    def fake_get(url, params=None, timeout=None, headers=None):
        return _Resp(FORECAST_OK)

    monkeypatch.setattr(omc.httpx, "get", fake_get)
    rows = omc.fetch_forecast(55.0, 37.0, hours=72)
    assert rows is not None and len(rows) == 24
    assert rows[0]["precip_probability_pct"] == 80
    assert rows[0]["precip_mm"] == 3.5
    assert rows[0]["condition_code"] == 61


def test_client_timeout_returns_none(monkeypatch):
    import httpx as _hx

    def boom(*a, **k):
        raise _hx.ConnectTimeout("timed out")

    monkeypatch.setattr(omc.httpx, "get", boom)
    assert omc.fetch_current(55.0, 37.0) is None
    assert omc.fetch_forecast(55.0, 37.0) is None


def test_client_http_5xx_returns_none(monkeypatch):
    monkeypatch.setattr(omc.httpx, "get",
                        lambda *a, **k: _Resp({}, status=503))
    assert omc.fetch_current(55.0, 37.0) is None


def test_client_http_4xx_returns_none(monkeypatch):
    monkeypatch.setattr(omc.httpx, "get",
                        lambda *a, **k: _Resp({}, status=404))
    assert omc.fetch_forecast(55.0, 37.0) is None


def test_client_cache_prevents_second_request(monkeypatch):
    calls = []

    def fake_get(url, params=None, timeout=None, headers=None):
        calls.append(url)
        return _Resp(CURRENT_OK)

    monkeypatch.setattr(omc.httpx, "get", fake_get)
    omc.fetch_current(55.0, 37.0)
    omc.fetch_current(55.0, 37.0)
    assert len(calls) == 1                      # второй раз — из кэша
    omc.fetch_current(44.0, 33.0)               # другие координаты — новый запрос
    assert len(calls) == 2


def test_client_none_coords_no_request(monkeypatch):
    def fail(*a, **k):
        raise AssertionError("сеть не должна дёргаться")

    monkeypatch.setattr(omc.httpx, "get", fail)
    assert omc.fetch_current(None, 37.0) is None
    assert omc.fetch_forecast(55.0, None) is None


# ==================================================== 2. CRUD правил
def test_create_and_list_rule(conn):
    rule = wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                                 "operator": "gt", "threshold": 5,
                                 "action": "skip"}, actor="admin")
    assert rule["id"] > 0 and rule["enabled"] == 1
    assert rule["scope"] == "global" and rule["adjustment_value"] == 0
    items = wx.list_rules(conn)
    assert [r["name"] for r in items] == ["Дождь"]


def test_update_rule_fields(conn):
    r = wx.create_rule(conn, {"name": "Жара", "condition": "high_temp",
                              "operator": "gt", "threshold": 30,
                              "action": "reduce_percent",
                              "adjustment_value": 30})
    upd = wx.update_rule(conn, r["id"], {"threshold": 32, "enabled": False})
    assert upd["threshold"] == 32 and upd["enabled"] == 0


def test_delete_rule(conn):
    r = wx.create_rule(conn, {"name": "Ветер", "condition": "wind",
                              "operator": "gt", "threshold": 10,
                              "action": "skip"})
    assert wx.delete_rule(conn, r["id"]) is True
    assert wx.delete_rule(conn, r["id"]) is False   # повторный delete — False
    assert wx.get_rule(conn, r["id"]) is None


def test_rule_validation_bad_condition(conn):
    with pytest.raises(wx.RuleValidationError):
        wx.create_rule(conn, {"name": "X", "condition": "blizzard",
                              "operator": "gt", "threshold": 1,
                              "action": "skip"})


def test_rule_validation_bad_operator_action_percent(conn):
    base = {"name": "X", "condition": "rain", "operator": "gt",
            "threshold": 1, "action": "skip"}
    with pytest.raises(wx.RuleValidationError):
        wx.create_rule(conn, {**base, "operator": "~="})
    with pytest.raises(wx.RuleValidationError):
        wx.create_rule(conn, {**base, "action": "multiply"})
    with pytest.raises(wx.RuleValidationError):
        wx.create_rule(conn, {**base, "action": "increase_percent",
                              "adjustment_value": 900})
    with pytest.raises(wx.RuleValidationError):
        wx.create_rule(conn, {**base, "threshold": "abc"})
    with pytest.raises(wx.RuleValidationError):
        wx.create_rule(conn, {**base, "name": ""})


def test_rule_scope_requires_existing_target(conn):
    with pytest.raises(wx.RuleValidationError):
        wx.create_rule(conn, {"name": "Z", "condition": "rain",
                              "operator": "gt", "threshold": 1,
                              "action": "skip", "scope": "zone",
                              "scope_id": 999})
    c = _mk_controller(conn)
    z = _mk_zone(conn, c["id"], 1)
    r = wx.create_rule(conn, {"name": "Z", "condition": "rain",
                              "operator": "gt", "threshold": 1,
                              "action": "skip", "scope": "zone",
                              "scope_id": z})
    assert r["scope_id"] == z


# ==================================================== 3. get_active_rules
def test_active_rules_priority_and_filters(conn):
    c = _mk_controller(conn)
    z = _mk_zone(conn, c["id"], 1)
    wx.create_rule(conn, {"name": "низкий приоритет", "condition": "rain",
                         "operator": "gt", "threshold": 1, "action": "skip",
                         "priority": 50})
    wx.create_rule(conn, {"name": "высокий приоритет", "condition": "wind",
                         "operator": "gt", "threshold": 1, "action": "skip",
                         "priority": 10})
    off = wx.create_rule(conn, {"name": "выкл", "condition": "rain",
                                "operator": "gt", "threshold": 1,
                                "action": "skip", "priority": 1,
                                "enabled": False})
    zrule = wx.create_rule(conn, {"name": "зонное", "condition": "rain",
                                  "operator": "gt", "threshold": 1,
                                  "action": "skip", "priority": 5,
                                  "scope": "zone", "scope_id": z})
    rules = wx.get_active_rules(conn, controller_id=c["id"], zone_ids=[z])
    names = [r["name"] for r in rules]
    assert off["name"] not in names
    assert names == ["зонное", "высокий приоритет", "низкий приоритет"]
    # без зоны — зонное правило не попадает
    names2 = [r["name"] for r in wx.get_active_rules(conn,
                                                     controller_id=c["id"],
                                                     zone_ids=[])]
    assert "зонное" not in names2


# ==================================================== 4. evaluate_rules
def test_evaluate_all_operators(conn):
    metrics = {"precip_mm": 5.0, "temp_c": 20.0, "humidity_pct": 80.0,
               "wind_ms": 3.0, "precip_probability_pct": 50.0}
    cases = [("gt", 4.9, True), ("gt", 5.0, False), ("gte", 5.0, True),
             ("lt", 5.1, True), ("lt", 5.0, False), ("lte", 5.0, True),
             ("eq", 5.0, True), ("eq", 4.0, False)]
    for op, thr, expect in cases:
        fired = wx.evaluate_rules(metrics, [{"condition": "rain",
                                             "operator": op, "threshold": thr,
                                             "action": "skip", "id": 1}])
        assert bool(fired) is expect, f"{op} {thr}"


def test_evaluate_missing_metric_never_fires(conn):
    fired = wx.evaluate_rules({"precip_mm": None},
                              [{"condition": "rain", "operator": "gt",
                                "threshold": 0, "action": "skip", "id": 1}])
    assert fired == []


def test_metrics_for_evaluation_prefers_forecast_max(conn):
    _add_obs(conn, precip_mm=0.0, temp_c=18.0, humidity=55.0, wind=1.0)
    _add_forecast(conn, 24, precip_mm=7.0, precip_prob=90)
    m = wx.metrics_for_evaluation(conn)
    assert m["precip_mm"] == 7.0                 # максимум по прогнозу
    assert m["precip_probability_pct"] == 90
    assert m["temp_c"] == 18.0                   # факт важнее прогноза


# ==================================================== 5. adjust_steps
def test_adjust_reduce_increase_clamp():
    steps = [{"duration_sec": 600, "active_zones": [1]}]
    r = wx.adjust_steps(steps, "reduce_percent", 30, 60, 14400)
    assert r[0]["duration_sec"] == 420
    assert steps[0]["duration_sec"] == 600       # исходник не изменён
    i = wx.adjust_steps(steps, "increase_percent", 50, 60, 14400)
    assert i[0]["duration_sec"] == 900
    lo = wx.adjust_steps([{"duration_sec": 100, "active_zones": [1]}],
                         "reduce_percent", 90, 60, 14400)
    assert lo[0]["duration_sec"] == 60           # кламп min_duration
    hi = wx.adjust_steps([{"duration_sec": 100, "active_zones": [1]}],
                         "increase_percent", 900, 60, 240)
    assert hi[0]["duration_sec"] == 240          # кламп max_duration


def test_adjust_skip_is_noop_on_steps():
    s = [{"duration_sec": 600, "active_zones": [1]}]
    assert wx.adjust_steps(s, "skip", 0, 60, 14400) == s


# ==================================================== 6. update_* с моком сети
def test_update_observations_and_forecasts_persist(conn, monkeypatch):
    _set_setting(conn, "weather.latitude", 55.0)
    _set_setting(conn, "weather.longitude", 37.0)

    # Build forecast inside this test, independent of module-import time.
    forecast = {"hourly": {**FORECAST_OK["hourly"], "time": _forecast_times()}}
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat(forecast["hourly"]["time"][0]).replace(tzinfo=timezone.utc)
    monkeypatch.setattr(wx, "datetime", FixedClock)
    def fake_get(url, params=None, timeout=None, headers=None):
        return _Resp(CURRENT_OK if "current" in params else forecast)

    monkeypatch.setattr(omc.httpx, "get", fake_get)
    obs = wx.update_observations(conn)
    assert obs is not None and obs["temp_c"] == 25.0
    row = wx.latest_observation(conn)
    assert row["precip_mm"] == 0.0 and row["precipitation_mm"] == 0.0
    n = wx.update_forecasts(conn)
    assert n == 24
    fc = wx.fresh_forecasts(conn)
    assert len(fc) == 24 and fc[0]["source"] == "open_meteo"


def test_update_without_coords_offline(conn, monkeypatch):
    def fail(*a, **k):
        raise AssertionError("без координат сеть не дёргается")

    monkeypatch.setattr(omc.httpx, "get", fail)
    assert wx.update_observations(conn) is None   # lat/lon = NULL
    assert wx.update_forecasts(conn) == 0
    # даже с координатами, но при сетевой ошибке — graceful
    _set_setting(conn, "weather.latitude", 55.0)
    _set_setting(conn, "weather.longitude", 37.0)
    import httpx as _hx

    def boom(*a, **k):
        raise _hx.ConnectError("offline")

    monkeypatch.setattr(omc.httpx, "get", boom)
    assert wx.update_observations(conn) is None
    assert wx.update_forecasts(conn) == 0


# ==================================================== 7. current_conditions
def test_current_conditions_statuses(conn):
    cond = wx.current_conditions(conn)           # пусто → stale/use_last
    assert cond["observation"] is None
    assert cond["status"] in ("use_last", "stale")
    _add_obs(conn, precip_mm=1.0, temp_c=22.0)
    cond = wx.current_conditions(conn)
    assert cond["fresh"] is True and cond["status"] == "ok"
    # старый факт без прогноза → не свежо, policy disable_corrections
    _add_obs(conn, precip_mm=1.0, ts=int(time.time()) - 10 * 3600)
    # latest_observation берёт самый новый — удалим свежие
    conn.execute("DELETE FROM weather_observations WHERE ts > ?",
                 (int(time.time()) - 5 * 3600,))
    conn.commit()
    _set_setting(conn, "weather.offline_policy", "disable_corrections")
    cond = wx.current_conditions(conn)
    assert cond["fresh"] is False and cond["status"] == "disabled"


# ==================================================== 8. интеграция с компилятором
def _compile(conn, ctrl, today=date(2026, 10, 26)):
    from server.app.core.schedule_compiler.compiler import compile_schedule
    # rain_delay (наследие Этапа 4, по умолчанию 24ч из миграции 0002) при
    # свежем наблюдении с дождём задержал бы и сегодняшние запуски — для
    # чистоты тестов именно ПОГОДНЫХ правил отключаем его.
    _set_setting(conn, "adjustment.rain_delay_hours", 0)
    # days_ahead=3: окно [today..today+3] = ровно 4 календарных дня.
    # today — понедельник 26.10.2026 → все дни будние (weekdays_mask),
    # а интервальные программы с понедельничным якорём тоже попадают в окно.
    return compile_schedule(conn, ctrl, today=today, days_ahead=3,
                            timezone_offset_min=0)


def _scenario(conn):
    c = _mk_controller(conn)
    z1 = _mk_zone(conn, c["id"], 1)
    z2 = _mk_zone(conn, c["id"], 2)
    p = _mk_program(conn)
    _link(conn, p, z1, 1)
    _link(conn, p, z2, 2)
    return c, z1, z2, p


def test_weather_skip_removes_runs_before_payload(conn):
    c, *_ = _scenario(conn)
    _add_obs(conn, precip_mm=10.0)
    wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                          "operator": "gt", "threshold": 5, "action": "skip"})
    comp = _compile(conn, c)
    assert comp.runs_count == 0
    wsk = [s for s in comp.skipped_runs if s["reason_code"] == "weather_skip"]
    assert len(wsk) == 4                          # 4 дня окна — все пропущены
    assert all(s["rule_id"] for s in wsk)
    assert comp.weather_applications and \
        comp.weather_applications[0]["action"] == "skip"


def test_weather_reduce_changes_durations_with_clamp(conn):
    c, *_ = _scenario(conn)
    _add_obs(conn, temp_c=35.0)
    wx.create_rule(conn, {"name": "Жара", "condition": "high_temp",
                          "operator": "gt", "threshold": 30,
                          "action": "reduce_percent", "adjustment_value": 30})
    comp = _compile(conn, c)
    assert comp.runs_count == 4
    for run in comp.payload["runs"]:
        for st in run["steps"]:
            assert st["duration_sec"] == 420      # 600*0.7 (10 мин база)
    _set_setting(conn, "schedule.min_duration_minutes", 8)
    wx.update_rule(conn, wx.list_rules(conn)[0]["id"],
                   {"adjustment_value": 90})     # 600*0.1=60сек < 8мин
    comp2 = _compile(conn, c)
    assert comp2.payload["runs"][0]["steps"][0]["duration_sec"] == 480


def test_weather_disabled_flags_skip_application(conn):
    c, *_ = _scenario(conn)
    _add_obs(conn, precip_mm=10.0)
    wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                          "operator": "gt", "threshold": 5, "action": "skip"})
    _set_setting(conn, "weather.enabled", False)
    assert _compile(conn, c).runs_count == 4
    _set_setting(conn, "weather.enabled", True)
    _set_setting(conn, "weather.rules_enabled", False)
    assert _compile(conn, c).runs_count == 4


def test_offline_policy_disable_corrections(conn):
    c, *_ = _scenario(conn)
    _add_obs(conn, precip_mm=10.0, ts=int(time.time()) - 100 * 3600)
    wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                          "operator": "gt", "threshold": 5, "action": "skip"})
    _set_setting(conn, "weather.offline_policy", "disable_corrections")
    assert wx.data_fresh(conn) is False
    assert _compile(conn, c).runs_count == 4      # поправки выключены


def test_offline_policy_use_last_applies_stale_data(conn):
    c, *_ = _scenario(conn)
    _add_obs(conn, precip_mm=10.0, ts=int(time.time()) - 100 * 3600)
    wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                          "operator": "gt", "threshold": 5, "action": "skip"})
    _set_setting(conn, "weather.offline_policy", "use_last")
    assert wx.data_fresh(conn) is False
    comp = _compile(conn, c)                       # старые данные применены
    assert comp.runs_count == 0


def test_zone_scope_prunes_only_that_zone(conn):
    c, z1, z2, _p = _scenario(conn)
    _add_obs(conn, precip_mm=10.0)
    wx.create_rule(conn, {"name": "Зона 2 под дождём", "condition": "rain",
                          "operator": "gt", "threshold": 5, "action": "skip",
                          "scope": "zone", "scope_id": z2})
    comp = _compile(conn, c)
    assert comp.runs_count == 4
    for run in comp.payload["runs"]:
        zones = {z for st in run["steps"] for z in st["active_zones"]}
        assert zones == {1}                        # зона 2 исключена


def test_running_watering_not_interrupted(conn):
    """Правила применяются только к новой машинограмме: активный полив
    (live-состояние контроллера) остаётся нетронутым."""
    c, *_ = _scenario(conn)
    with conn:
        conn.execute(
            "UPDATE controllers SET current_mode='watering', "
            "current_phase='water', active_zones_json='[1]' WHERE id=?",
            (c["id"],))
    _add_obs(conn, precip_mm=10.0)
    wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                          "operator": "gt", "threshold": 5, "action": "skip"})
    comp = _compile(conn, c)
    assert comp.runs_count == 0                    # новая машинограмма — пусто
    row = conn.execute("SELECT current_mode, current_phase, active_zones_json "
                       "FROM controllers WHERE id=?", (c["id"],)).fetchone()
    assert row["current_mode"] == "watering"       # текущий цикл не прерван
    assert json.loads(row["active_zones_json"]) == [1]


def test_record_applications_journal(conn):
    c, *_ = _scenario(conn)
    _add_obs(conn, precip_mm=10.0)
    r = wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                              "operator": "gt", "threshold": 5,
                              "action": "skip"})
    comp = _compile(conn, c)
    wx.record_applications(conn, comp.weather_applications,
                           schedule_version=7, before_hash="aaa",
                           after_hash="bbb")
    items, total = wx.list_applications(conn)
    assert total == 1
    a = items[0]
    assert a["rule_id"] == r["id"] and a["schedule_version"] == 7
    assert a["before_hash"] == "aaa" and a["after_hash"] == "bbb"
    assert a["details"]["action"] == "skip"


def test_schedule_service_persists_applications(conn):
    """compile_and_store пишет журнал с версией машинограммы."""
    from server.app.services.schedule_service import ScheduleService
    c, *_ = _scenario(conn)
    _add_obs(conn, precip_mm=10.0)
    wx.create_rule(conn, {"name": "Дождь", "condition": "rain",
                          "operator": "gt", "threshold": 5, "action": "skip"})
    tmp = Path(tempfile.mkdtemp(prefix="poliv-st7-svc-"))

    real_db = conn.execute("PRAGMA database_list").fetchone()[2]

    class _Cfg:
        db_path = real_db          # то же файл-БД, что и у соединения теста
        data_dir = tmp / "data"

    svc = ScheduleService(_Cfg(), conn)
    res = svc.compile_and_store(c["id"], source="manual")
    assert res["status"] == "compiled" and res["runs_count"] == 0
    items, total = wx.list_applications(conn)
    assert total >= 1
    assert items[0]["after_hash"] == res["schedule_hash"]
    svc.close()


# ==================================================== 9. REST API + ACL
from server.tests.test_stage3 import _Harness  # noqa: E402
from server.app.api.routes_weather import register_api_routes_weather  # noqa: E402


class _WxHarness(_Harness):
    def __init__(self):
        super().__init__()
        register_api_routes_weather(self.app, self.cfg, self.conn)

    def close(self):
        super().close()


@pytest.fixture()
def api():
    h = _WxHarness()
    yield h
    h.close()


def _rule_body(**over):
    b = {"name": "Дождь", "condition": "rain", "operator": "gt",
         "threshold": 5, "action": "skip", "scope": "global"}
    b.update(over)
    return b


def test_api_unauthorized_401(api):
    assert api.client.get("/api/v1/weather/current").status_code == 401
    assert api.client.post("/api/v1/weather/rules", json=_rule_body()
                           ).status_code == 401


def test_api_viewer_read_only(api):
    v = api.login_as("viewer")
    assert v.get("/api/v1/weather/current").status_code == 200
    assert v.get("/api/v1/weather/rules").status_code == 200
    assert v.get("/api/v1/weather/applications").status_code == 200
    assert v.post("/api/v1/weather/rules", json=_rule_body()).status_code == 403
    assert v.put("/api/v1/weather/rules/1", json={"threshold": 1}
                 ).status_code == 403
    assert v.delete("/api/v1/weather/rules/1").status_code == 403
    assert v.post("/api/v1/weather/rules/1/test",
                  json={"controller_id": 1}).status_code == 403


def test_api_operator_crud_admin_delete(api):
    o = api.login_as("operator")
    r = o.post("/api/v1/weather/rules", json=_rule_body(name="R1"))
    assert r.status_code == 201
    rid = r.json()["id"]
    upd = o.put(f"/api/v1/weather/rules/{rid}", json={"threshold": 9})
    assert upd.status_code == 200 and upd.json()["threshold"] == 9
    bad = o.post("/api/v1/weather/rules", json=_rule_body(condition="fog"))
    assert bad.status_code == 422
    assert o.delete(f"/api/v1/weather/rules/{rid}").status_code == 403
    a = api.login_as("admin")
    assert a.delete(f"/api/v1/weather/rules/{rid}").status_code == 200
    assert o.put(f"/api/v1/weather/rules/{rid}", json={"threshold": 1}
                 ).status_code == 404


def test_api_current_and_forecast_endpoints(api):
    v = api.login_as("viewer")
    cur = v.get("/api/v1/weather/current").json()
    assert "config" in cur and "offline_policy" in cur["config"]
    fc = v.get("/api/v1/weather/forecast?hours=48")
    assert fc.status_code == 200 and fc.json()["count"] == 0


def test_api_test_endpoint_dry_run_no_journal(api):
    o = api.login_as("operator")
    ctrl = api.controller("BOX-W-API")
    zz = api.catalog.create_zone({"controller_id": ctrl["id"],
                                  "zone_number": 1, "name": "З1",
                                  "base_duration_minutes": 10})
    p = api.catalog.create_program({"name": "Прога", "start_time": "06:00",
                                    "schedule_type": "weekdays",
                                    "weekdays": ["пн", "вт", "ср", "чт", "пт",
                                                 "сб", "вс"]})
    api.catalog.set_program_zones(p["id"], [zz["id"]])
    _add_obs(api.conn, precip_mm=12.0)
    r = o.post("/api/v1/weather/rules", json=_rule_body()).json()
    resp = o.post(f"/api/v1/weather/rules/{r['id']}/test",
                  json={"controller_id": ctrl["id"]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["would_apply"] is True
    assert body["skipped_runs"] and \
        all(s["reason_code"] == "weather_skip" for s in body["skipped_runs"])
    assert body["hash_before"] != body["hash_after"]
    _, total = wx.list_applications(api.conn)
    assert total == 0                              # dry-run ничего не пишет
    missing = o.post("/api/v1/weather/rules/999/test",
                     json={"controller_id": ctrl["id"]})
    assert missing.status_code == 404


def test_api_applications_pagination(api):
    v = api.login_as("viewer")
    for i in range(3):
        wx.record_applications(api.conn, [{
            "rule_id": None, "controller_id": None, "action": "skip",
            "adjustment_value": 0, "metric": "precip_mm", "metric_value": 9,
            "affected": [], "skipped": []}],
            schedule_version=i, before_hash="b", after_hash="a")
    page = v.get("/api/v1/weather/applications?page=1&page_size=2").json()
    assert page["total_items"] == 3 and len(page["items"]) == 2
    assert v.get("/api/v1/weather/applications?page=2&page_size=2"
                 ).json()["items"][0]["id"] < page["items"][-1]["id"]


def test_api_alias_paths_work(api):
    v = api.login_as("viewer")
    assert v.get("/api/weather/rules").status_code == 200


# ==================================================== 10. миграция 0009
def test_migration_0009_up_down_idempotent():
    from server.app.infra.db import _load_module, MIGRATIONS_DIR
    tmp = Path(tempfile.mkdtemp(prefix="poliv-st7-mig-"))
    conn = sqlite3.connect(tmp / "t.db")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE settings (key TEXT PRIMARY KEY,
        value_json TEXT, description TEXT, updated_at TEXT)""")
    mig = _load_module(MIGRATIONS_DIR / "0009_weather.py")
    mig.upgrade(conn)
    mig.upgrade(conn)                               # идемпотентность
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"weather_observations", "weather_forecasts", "weather_rules",
            "weather_rule_applications"} <= tables
    keys = {r["key"] for r in conn.execute(
        "SELECT key FROM settings WHERE key LIKE 'weather.%'")}
    assert len(keys) == 6
    conn.close()
    # downgrade — обратимый откат
    conn2 = sqlite3.connect(tmp / "t.db")
    mig.downgrade(conn2)
    tables2 = {r[0] for r in conn2.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert not ({"weather_observations", "weather_forecasts", "weather_rules",
                 "weather_rule_applications"} & tables2)
    assert conn2.execute("SELECT COUNT(*) FROM settings WHERE key LIKE "
                         "'weather.%'").fetchone()[0] == 0
    conn2.close()
