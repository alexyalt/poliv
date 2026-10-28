"""Сервис погодных корректировок (Этап 7).

Ответственности (промпт Этапа 7, Шаг 3):
- update_observations()/update_forecasts() — запрос Open-Meteo и сохранение
  факта/прогноза (graceful degradation: при ошибке сети ничего не падает);
- CRUD правил weather_rules (валидация enum-полей на уровне сервиса —
  CHECK в схеме страхует от порчи данных);
- get_active_rules(controller_id, zone_ids) — включённые правила по
  приоритету с учётом области действия;
- evaluate_rules(conditions, rules) -> [(rule, metric_value)] — какие
  условия сработали;
- adjust_steps(steps, action, value, min_sec, max_sec) — чистая функция
  изменения длительностей шагов (reduce/increase с клампом min/max);
- apply_to_candidates(...) — применение к запускам компилятора ДО сборки
  payload (skip помечает пропуск, reduce/increase меняет duration_sec);
- get_current_conditions() — сводка «сейчас» для UI + offline-статус;
- фоновый поток обновления (threading, интервал weather.update_interval_min;
  apscheduler в зависимостях отсутствует — осознанное отклонение, чтобы не
  добавлять новую зависимость на ходу).

Правила НЕ прерывают уже запущенный полив: они применяются только при
генерации новой машинограммы (см. core/schedule_compiler/compiler.py).
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from ..infra import open_meteo_client
from ..infra.db import connect_path
from ..infra.logging import get_logger

log = get_logger("poliv.weather")

# condition -> метрика наблюдения/прогноза, порог которой сравнивается
CONDITION_METRIC = {
    "rain": "precip_mm",
    "precip_probability": "precip_probability_pct",
    "high_humidity": "humidity_pct",
    "low_temp": "temp_c",
    "high_temp": "temp_c",
    "wind": "wind_ms",
}

RULE_CONDITIONS = tuple(CONDITION_METRIC)
RULE_OPERATORS = ("gt", "lt", "eq", "gte", "lte")
RULE_ACTIONS = ("skip", "reduce_percent", "increase_percent")
RULE_SCOPES = ("global", "controller", "zone")

FRESH_HOURS = 6   # данные старше — считаются «нет свежих» (offline_policy)


class RuleValidationError(ValueError):
    """Некорректное значение поля правила (API отдаёт 422)."""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _op_compare(op: str, value: float, threshold: float) -> bool:
    if op == "gt":
        return value > threshold
    if op == "lt":
        return value < threshold
    if op == "eq":
        return abs(value - threshold) < 1e-9
    if op == "gte":
        return value >= threshold
    if op == "lte":
        return value <= threshold
    return False


# --------------------------------------------------------------- настройки
def get_weather_setting(conn: sqlite3.Connection, key: str, default: Any) -> Any:
    row = conn.execute("SELECT value_json FROM settings WHERE key=?",
                       (key,)).fetchone()
    if row is None or row["value_json"] is None:
        return default
    try:
        return json.loads(row["value_json"])
    except (TypeError, ValueError):
        return default


def set_weather_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    with conn:
        conn.execute(
            """INSERT INTO settings(key, value_json, description, updated_at)
               VALUES (?, ?, 'Этап 7', datetime('now'))
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                   updated_at=datetime('now')""",
            (key, json.dumps(value)),
        )


def weather_config(conn: sqlite3.Connection) -> dict:
    """Снимок настроек weather.* (с дефолтами Артефакта 0.7 §17.7)."""
    return {
        "enabled": bool(get_weather_setting(conn, "weather.enabled", True)),
        "latitude": get_weather_setting(conn, "weather.latitude", 44.423062),
        "longitude": get_weather_setting(conn, "weather.longitude", 34.06452),
        "update_interval_min": int(get_weather_setting(
            conn, "weather.update_interval_min", 60) or 60),
        "offline_policy": str(get_weather_setting(
            conn, "weather.offline_policy", "use_last")),
        "rules_enabled": bool(get_weather_setting(conn, "weather.rules_enabled",
                                                  True)),
    }


# ------------------------------------------------------------------- CRUD
def validate_rule(data: dict) -> dict:
    """Нормализация/валидация полей правила; бросает RuleValidationError."""
    name = str(data.get("name") or "").strip()
    if not name or len(name) > 100:
        raise RuleValidationError("Название правила обязательно (до 100 символов)")
    condition = data.get("condition")
    if condition not in RULE_CONDITIONS:
        raise RuleValidationError(
            f"Условие должно быть одним из: {', '.join(RULE_CONDITIONS)}")
    operator = data.get("operator", "gt")
    if operator not in RULE_OPERATORS:
        raise RuleValidationError(f"Оператор должен быть одним из: "
                                  f"{', '.join(RULE_OPERATORS)}")
    action = data.get("action")
    if action not in RULE_ACTIONS:
        raise RuleValidationError(f"Действие должно быть одним из: "
                                  f"{', '.join(RULE_ACTIONS)}")
    try:
        threshold = float(data.get("threshold"))
    except (TypeError, ValueError):
        raise RuleValidationError("Порог — число")
    try:
        adjustment = int(data.get("adjustment_value") or 0)
    except (TypeError, ValueError):
        raise RuleValidationError("Корректировка — целое число процентов")
    if action != "skip" and not (0 <= adjustment <= 500):
        raise RuleValidationError("Проценты корректировки: диапазон 0..500")
    scope = data.get("scope", "global")
    if scope not in RULE_SCOPES:
        raise RuleValidationError(f"Область должна быть одной из: "
                                  f"{', '.join(RULE_SCOPES)}")
    scope_id = data.get("scope_id")
    if scope in ("controller", "zone"):
        try:
            scope_id = int(scope_id)
        except (TypeError, ValueError):
            raise RuleValidationError(f"Для области {scope} нужен scope_id")
        # существование контроллера/зоны проверяется в create/update (нужна conn)
    else:
        scope_id = None
    try:
        priority = int(data.get("priority") if data.get("priority") is not None
                       else 100)
    except (TypeError, ValueError):
        raise RuleValidationError("Приоритет — целое число")
    enabled = 1 if data.get("enabled", True) in (True, 1, "1", "true", "on") else 0
    return {"name": name, "enabled": enabled, "priority": priority,
            "condition": condition, "operator": operator, "threshold": threshold,
            "action": action, "adjustment_value": adjustment,
            "scope": scope, "scope_id": scope_id}


def create_rule(conn: sqlite3.Connection, data: dict,
                actor: Optional[str] = None) -> dict:
    clean = validate_rule(data)
    _require_scope_target(conn, clean)
    with conn:
        cur = conn.execute(
            """INSERT INTO weather_rules(name, enabled, priority, condition,
                   operator, threshold, action, adjustment_value, scope,
                   scope_id, created_by, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
            (clean["name"], clean["enabled"], clean["priority"],
             clean["condition"], clean["operator"], clean["threshold"],
             clean["action"], clean["adjustment_value"], clean["scope"],
             clean["scope_id"], actor, utcnow_iso()),
        )
    return get_rule(conn, int(cur.lastrowid))  # type: ignore[arg-type]


def update_rule(conn: sqlite3.Connection, rule_id: int, data: dict) -> dict:
    existing = get_rule(conn, rule_id)
    if existing is None:
        raise RuleValidationError("Правило не найдено")
    merged = {**existing, **{k: v for k, v in data.items() if v is not None
                              or k == "enabled"}}
    clean = validate_rule(merged)
    _require_scope_target(conn, clean)
    with conn:
        conn.execute(
            """UPDATE weather_rules SET name=?, enabled=?, priority=?,
                   condition=?, operator=?, threshold=?, action=?,
                   adjustment_value=?, scope=?, scope_id=?,
                   updated_at=datetime('now')
               WHERE id=?""",
            (clean["name"], clean["enabled"], clean["priority"],
             clean["condition"], clean["operator"], clean["threshold"],
             clean["action"], clean["adjustment_value"], clean["scope"],
             clean["scope_id"], rule_id),
        )
    return get_rule(conn, rule_id)


def delete_rule(conn: sqlite3.Connection, rule_id: int) -> bool:
    with conn:
        cur = conn.execute("DELETE FROM weather_rules WHERE id=?", (rule_id,))
    return cur.rowcount > 0


def get_rule(conn: sqlite3.Connection, rule_id: int) -> Optional[dict]:
    row = conn.execute("SELECT * FROM weather_rules WHERE id=?",
                       (rule_id,)).fetchone()
    return dict(row) if row else None


def list_rules(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM weather_rules ORDER BY priority, id").fetchall()]


def _require_scope_target(conn: sqlite3.Connection, clean: dict) -> None:
    if clean["scope"] == "controller":
        if not conn.execute("SELECT 1 FROM controllers WHERE id=? AND "
                            "deleted_at IS NULL", (clean["scope_id"],)).fetchone():
            raise RuleValidationError("Контроллер для области правила не найден")
    if clean["scope"] == "zone":
        if not conn.execute("SELECT 1 FROM zones WHERE id=? AND deleted_at IS NULL",
                            (clean["scope_id"],)).fetchone():
            raise RuleValidationError("Зона для области правила не найдена")


# ------------------------------------------------------------ получение данных
def update_observations(conn: sqlite3.Connection, *, lat=None, lon=None,
                        controller_id: Optional[int] = None) -> Optional[dict]:
    """Запросить текущую погоду и сохранить наблюдение. None при ошибке/офлайне."""
    cfgw = weather_config(conn)
    lat = cfgw["latitude"] if lat is None else lat
    lon = cfgw["longitude"] if lon is None else lon
    if lat is None or lon is None:
        log.info("WEATHER: координаты не заданы — обновление факта пропущено")
        return None
    cur = open_meteo_client.fetch_current(lat, lon)
    if cur is None:
        return None
    now_ts = int(time.time())
    observed_at = cur.get("observed_at") or utcnow_iso()
    precip = cur.get("precip_mm")
    with conn:
        conn.execute(
            """INSERT INTO weather_observations(controller_id, observed_at, ts,
                   temp_c, humidity_pct, precip_mm, precipitation_mm, wind_ms,
                   source, raw_json)
               VALUES (?,?,?,?,?,?,?,?, 'open_meteo', ?)""",
            (controller_id, observed_at, now_ts, cur.get("temp_c"),
             cur.get("humidity_pct"), precip, precip, cur.get("wind_ms"),
             json.dumps(cur.get("raw") or {}, ensure_ascii=False)[:8000]),
        )
    return cur


def update_forecasts(conn: sqlite3.Connection, *, lat=None, lon=None,
                     hours: int = 72,
                     controller_id: Optional[int] = None) -> int:
    """Сохранить почасовой прогноз; возвращает число записей (0 при ошибке)."""
    cfgw = weather_config(conn)
    lat = cfgw["latitude"] if lat is None else lat
    lon = cfgw["longitude"] if lon is None else lon
    if lat is None or lon is None:
        return 0
    rows = open_meteo_client.fetch_forecast(lat, lon, hours=hours)
    if not rows:
        return 0
    now_iso = utcnow_iso()
    inserted = 0
    with conn:
        for i, h in enumerate(rows):
            t_from = h.get("time") or now_iso
            if i + 1 < len(rows):
                nxt = rows[i + 1].get("time") or t_from
            else:
                # Последний час прогноза: период закрываем +1 часом,
                # valid_to NOT NULL (миграция 0009).
                try:
                    dt_from = datetime.fromisoformat(t_from)
                    nxt = (dt_from + timedelta(hours=1)
                           ).isoformat(timespec="seconds")
                except ValueError:
                    nxt = t_from
            conn.execute(
                """INSERT INTO weather_forecasts(controller_id, forecasted_at,
                       valid_from, valid_to, temp_c, humidity_pct,
                       precip_probability_pct, precip_mm, condition_code,
                       source, raw_json)
                   VALUES (?,?,?,?,?,?,?,?,?, 'open_meteo', NULL)""",
                (controller_id, now_iso, t_from, nxt, h.get("temp_c"),
                 h.get("humidity_pct"), h.get("precip_probability_pct"),
                 h.get("precip_mm"), h.get("condition_code")),
            )
            inserted += 1
    return inserted


def latest_observation(conn: sqlite3.Connection) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM weather_observations ORDER BY ts DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def fresh_forecasts(conn: sqlite3.Connection, hours_ahead: int = 72,
                    limit: int = 200) -> list[dict]:
    """Прогнозные строки, актуальные сейчас и вперёд (для evaluation/UI)."""
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = conn.execute(
        """SELECT * FROM weather_forecasts
           WHERE valid_to >= ? ORDER BY valid_from LIMIT ?""",
        (now_iso, int(limit))).fetchall()
    return [dict(r) for r in rows]


def data_fresh(conn: sqlite3.Connection, max_age_hours: int = FRESH_HOURS) -> bool:
    """Есть ли свежие данные (факт или прогноз) — критерий offline_policy."""
    obs = latest_observation(conn)
    horizon = time.time() - max_age_hours * 3600
    if obs and int(obs["ts"]) >= horizon:
        return True
    return bool(fresh_forecasts(conn, limit=1))


# ------------------------------------------------------------------ оценка
def current_conditions(conn: sqlite3.Connection) -> dict:
    """Сводка «сейчас» для UI: observation + forecast + offline-статус."""
    cfgw = weather_config(conn)
    obs = latest_observation(conn)
    fc = fresh_forecasts(conn, limit=72)
    fresh = data_fresh(conn)
    status = "ok" if fresh else "stale"
    if not fresh:
        status = ("use_last" if cfgw["offline_policy"] == "use_last"
                  else "disabled")
    return {
        "config": cfgw,
        "observation": obs,
        "forecast_count": len(fc),
        "forecast_preview": fc[:24],
        "fresh": fresh,
        "status": status,
    }


def get_active_rules(conn: sqlite3.Connection, controller_id: Optional[int] = None,
                     zone_ids: Optional[list[int]] = None) -> list[dict]:
    """Включённые правила, применимые к контроллеру/зонам, по приоритету."""
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM weather_rules WHERE enabled=1 ORDER BY priority, id"
    ).fetchall()]
    zset = set(zone_ids or [])
    out = []
    for r in rows:
        if r["scope"] == "global":
            out.append(r)
        elif r["scope"] == "controller" and controller_id is not None \
                and int(r["scope_id"]) == int(controller_id):
            out.append(r)
        elif r["scope"] == "zone" and zset and int(r["scope_id"] or 0) in zset:
            out.append(r)
    return out


def metrics_for_evaluation(conn: sqlite3.Connection) -> dict:
    """Метрики «на сейчас+прогноз»: максимум осадков/вероятности по окну,
    текущие temp/humidity/wind из последнего наблюдения (fallback — первая
    строка прогноза)."""
    m: dict[str, Optional[float]] = {
        "precip_mm": None, "precip_probability_pct": None,
        "temp_c": None, "humidity_pct": None, "wind_ms": None,
    }
    obs = latest_observation(conn)
    if obs:
        m["precip_mm"] = _num(obs.get("precip_mm") or obs.get("precipitation_mm"))
        m["temp_c"] = _num(obs.get("temp_c"))
        m["humidity_pct"] = _num(obs.get("humidity_pct"))
        m["wind_ms"] = _num(obs.get("wind_ms"))
    fc = fresh_forecasts(conn, limit=72)
    if fc:
        pmm = [_num(x.get("precip_mm")) for x in fc if _num(x.get("precip_mm"))]
        if pmm:
            m["precip_mm"] = max([v for v in pmm if v is not None]
                                 + ([m["precip_mm"]] if m["precip_mm"] else []))
        pp = [_num(x.get("precip_probability_pct")) for x in fc]
        pp = [v for v in pp if v is not None]
        if pp:
            m["precip_probability_pct"] = max(pp)
        if m["temp_c"] is None:
            m["temp_c"] = _num(fc[0].get("temp_c"))
        if m["humidity_pct"] is None:
            m["humidity_pct"] = _num(fc[0].get("humidity_pct"))
    return m


def _num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def evaluate_rules(metrics: dict, rules: list[dict]) -> list[dict]:
    """Какие правила сработали на данных metrics.

    Возвращает [{"rule": ..., "metric": имя, "value": X}] в порядке
    приоритета. Правило без значения метрики не срабатывает (нет данных).
    """
    fired = []
    for r in rules:
        metric = CONDITION_METRIC[r["condition"]]
        value = metrics.get(metric)
        if value is None:
            continue
        if _op_compare(r["operator"], value, float(r["threshold"])):
            fired.append({"rule": r, "metric": metric, "value": value})
    return fired


# ------------------------------------------------------- влияние на шаги/runs
def adjust_steps(steps: list[dict], action: str, percent: int,
                 min_sec: int, max_sec: int) -> list[dict]:
    """Чистая функция: reduce/increase длительностей water-шагов с клампом.

    skip не трогает шаги (пометка пропуска уровня run). soak тоже
    корректируется пропорционально (это часть того же цикла полива).
    Возвращает НОВЫЙ список steps (исходные dicts не изменяются).
    """
    if action == "skip":
        return steps
    factor = (1 - percent / 100.0) if action == "reduce_percent" \
        else (1 + percent / 100.0)
    lo, hi = int(min_sec), int(max_sec)
    out = []
    for s in steps:
        ns = dict(s)
        new_dur = max(lo, min(hi, round(int(s["duration_sec"]) * factor)))
        ns["duration_sec"] = new_dur
        out.append(ns)
    return out


def apply_to_candidates(conn: sqlite3.Connection, candidates: list[dict],
                        controller_id: int, *, min_sec: int, max_sec: int,
                        dry_run: bool = False,
                        rules_override: Optional[list[dict]] = None) -> tuple[list[dict], list[dict],
                                                         list[dict]]:
    """Применяет сработавшие правила к candidate-runs КОМПИЛЯТОРА.

    Возвращает (kept_candidates, skipped_runs, applications):
    - skip: все зоны правила затронуты — run исключается (reason_code
      weather_skip) либо ограничивается зоной (удаляются шаги зоны);
    - reduce/increase: duration_sec water-шагов меняется с клампом;
    - applications — записи для журнала weather_rule_applications
      (before/after длительности, затронутые номера зон).

    dry_run=True ничего не пишет в журнал (используется эндпоинтом test).
    """
    metrics = metrics_for_evaluation(conn)
    zone_ids = sorted({z for c in candidates for s in c["steps"]
                       for z in s.get("active_zones", [])})
    if rules_override is None:
        rules = get_active_rules(conn, controller_id=controller_id, zone_ids=zone_ids)
    else:
        # Explicit preview input, independent of persistent enabled flags.
        rules = [dict(r) for r in rules_override if
                 r["scope"] == "global" or
                 (r["scope"] == "controller" and r["scope_id"] == controller_id) or
                 (r["scope"] == "zone" and r["scope_id"] in {
                     row[0] for row in conn.execute(
                         "SELECT id FROM zones WHERE controller_id=? AND zone_number IN (%s)"
                         % (",".join("?" * len(zone_ids)) or "NULL"),
                         (controller_id, *zone_ids))})]
    fired = evaluate_rules(metrics, rules)
    if not fired:
        return candidates, [], []

    kept: list[dict] = []
    skipped_runs: list[dict] = []
    applications: list[dict] = []
    for item in fired:
        rule = item["rule"]
        action = rule["action"]
        value = int(rule["adjustment_value"] or 0)
        affected: list[dict] = []
        new_candidates: list[dict] = []
        for c in candidates:
            before_total = sum(s["duration_sec"] for s in c["steps"])
            if rule["scope"] == "zone":
                target_zone = conn.execute(
                    "SELECT zone_number FROM zones WHERE id=?",
                    (rule["scope_id"],)).fetchone()
                numbers = {target_zone["zone_number"]} if target_zone else set()
            else:
                numbers = None  # все зоны run'а
            if action == "skip":
                if numbers is None:
                    skipped_runs.append({
                        "date": c["date"].isoformat(),
                        "start_minute_local": c["start_minute"],
                        "program_id": c["program_id"],
                        "reason_code": "weather_skip",
                        "rule_id": rule["id"],
                    })
                    continue
                pruned_steps = [s for s in c["steps"]
                                if not numbers.intersection(s.get("active_zones",
                                                                  []))]
                if not pruned_steps:
                    skipped_runs.append({
                        "date": c["date"].isoformat(),
                        "start_minute_local": c["start_minute"],
                        "program_id": c["program_id"],
                        "reason_code": "weather_skip",
                        "rule_id": rule["id"],
                    })
                    continue
                nc = dict(c)
                nc["steps"] = pruned_steps
                new_candidates.append(nc)
                affected.append({"program_id": c["program_id"],
                                 "date": c["date"].isoformat(),
                                 "zones": sorted(numbers)})
            else:
                steps2 = adjust_steps(c["steps"], action, value, min_sec, max_sec)
                after_total = sum(s["duration_sec"] for s in steps2)
                if after_total != before_total:
                    affected.append({"program_id": c["program_id"],
                                     "date": c["date"].isoformat(),
                                     "before_sec": before_total,
                                     "after_sec": after_total,
                                     "zones": sorted({z for s in c["steps"]
                                                      for z in
                                                      s.get("active_zones", [])})})
                    nc = dict(c)
                    nc["steps"] = steps2
                    new_candidates.append(nc)
                else:
                    new_candidates.append(c)
        candidates = new_candidates
        if affected or any(s.get("rule_id") == rule["id"] for s in skipped_runs):
            applications.append({
                "rule_id": rule["id"],
                "controller_id": controller_id,
                "action": action,
                "adjustment_value": value,
                "metric": item["metric"], "metric_value": item["value"],
                "affected": affected,
                "skipped": [s for s in skipped_runs if s.get("rule_id") == rule["id"]],
            })
    return candidates, skipped_runs, applications


def record_applications(conn: sqlite3.Connection, applications: list[dict], *,
                        schedule_version: Optional[int],
                        before_hash: str, after_hash: str) -> None:
    """Журнал применений (weather_rule_applications) после persist-версии."""
    if not applications:
        return
    with conn:
        for a in applications:
            conn.execute(
                """INSERT INTO weather_rule_applications(rule_id,
                       schedule_version, controller_id, before_hash,
                       after_hash, affected_zones_json, details_json)
                   VALUES (?,?,?,?,?,?,?)""",
                (a["rule_id"], schedule_version, a["controller_id"],
                 before_hash, after_hash,
                 json.dumps(a["affected"], ensure_ascii=False),
                 json.dumps({k: v for k, v in a.items()
                             if k not in ("rule_id", "controller_id",
                                          "affected")},
                            ensure_ascii=False)),
            )


def list_applications(conn: sqlite3.Connection, limit: int = 100,
                      offset: int = 0) -> tuple[list[dict], int]:
    total = conn.execute(
        "SELECT COUNT(*) c FROM weather_rule_applications").fetchone()["c"]
    rows = conn.execute(
        """SELECT wra.*, wr.name AS rule_name FROM weather_rule_applications wra
           LEFT JOIN weather_rules wr ON wr.id = wra.rule_id
           ORDER BY wra.id DESC LIMIT ? OFFSET ?""", (int(limit), int(offset))
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["affected_zones"] = json.loads(d.pop("affected_zones_json") or "[]")
            d["details"] = json.loads(d.pop("details_json") or "{}")
        except (TypeError, ValueError):
            pass
        out.append(d)
    return out, int(total)


# ------------------------------------------------------------- фоновое обновление
class WeatherUpdater(threading.Thread):
    """Периодическое обновление факта/прогноза (как maintenance-поток MQTT).

    Интервал — weather.update_interval_min (читается каждый цикл, можно
    менять на лету). Ошибки сети не убивают поток. daemon=True — не мешает
    завершению тестов/сервера.
    """

    def __init__(self, db_path: str, *, initial_delay_sec: float = 5.0):
        super().__init__(daemon=True, name="poliv-weather-updater")
        self.db_path = db_path
        self._stop_event = threading.Event()
        self._initial_delay = initial_delay_sec

    def run(self) -> None:
        if self._stop_event.wait(self._initial_delay):
            return
        while not self._stop_event.is_set():
            interval_min = 60
            try:
                conn = connect_path(self.db_path)
                try:
                    cfgw = weather_config(conn)
                    interval_min = max(5, int(cfgw["update_interval_min"]))
                    if cfgw["enabled"]:
                        update_observations(conn)
                        update_forecasts(conn)
                    from .weather_outlook import update as update_outlook
                    update_outlook(conn)
                finally:
                    conn.close()
            except Exception:
                log.exception("WEATHER: цикл обновления упал — продолжаем")
            if self._stop_event.wait(interval_min * 60):
                return

    def stop(self) -> None:
        self._stop_event.set()


_updater: Optional[WeatherUpdater] = None
_updater_lock = threading.Lock()


def start_weather_updater(db_path: str) -> Optional[WeatherUpdater]:
    """Lifespan main.py: один фоновый поток на процесс."""
    global _updater
    with _updater_lock:
        if _updater is not None and _updater.is_alive():
            return _updater
        _updater = WeatherUpdater(str(db_path))
        _updater.start()
        return _updater


def stop_weather_updater() -> None:
    global _updater
    with _updater_lock:
        if _updater is not None:
            _updater.stop()
            _updater = None
