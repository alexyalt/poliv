"""Этап 8, блок 8.1: базовая аналитика (отчёты).

Источники данных (миграции 0005/0007/0008):
- watering_runs   — прогон-центрик: план/факт запуска, статусы
  planned/active/completed/skipped/aborted/failed/cancelled, source
  schedule/manual/service/test/emergency, water_sec, volume_liters;
- watering_events — фактические события контроллеров (started/finished/
  stopped) с объёмом воды — источник литров и аварий (reason_code);
- flow_daily      — дневные агрегаты расхода (переиспользуется методика
  FlowService.summary);
- notifications   — уведомления (аварии/предупреждения) — второй источник
  отчёта по инцидентам.

Все запросы — SQL-агрегации (SUM/COUNT/GROUP BY), один запрос на отчёт
(N+1 нет). Пустой/некорректный диапазон даёт пустой результат, а не ошибку;
date_from > date_to меняются местами (защита «пустых диапазонов»).

Диапазены from/to — даты YYYY-MM-DD (локальные, как в настройках проекта);
временные поля runs/events хранятся в unix-ts, поэтому диапазон переводится
в границы [from 00:00:00, to 23:59:59] через datetime-range-утилиту ниже.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

# ------------------------------------------------------------------ параметры
PRESETS = {"today": 0, "week": 6, "month": 29, "quarter": 91, "year": 364}


class RangeError(ValueError):
    """Некорректный диапазон/параметр отчёта → 422 на слое API."""


def resolve_range(date_from: Optional[str], date_to: Optional[str],
                  preset: Optional[str] = None,
                  now: Optional[datetime] = None) -> tuple[Optional[str], Optional[str]]:
    """Нормализовать параметры диапазона. preset (today/week/month/quarter/
    year) задаёт конец = сегодня, начало = сегодня минус N дней; явные from/to
    имеют приоритет над preset. Возвращает (date_from, date_to) в YYYY-MM-DD
    или (None, None) — без ограничений."""
    if preset and preset in PRESETS and not (date_from or date_to):
        today = (now or datetime.now()).date()
        start = today - timedelta(days=PRESETS[preset])
        return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    return _norm(date_from), _norm(date_to)


def _norm(value: Optional[str]) -> Optional[str]:
    """YYYY-MM-DD или None; мусорная строка → None (мягкая деградация —
    строгая валидация 422 выполняется слоем API)."""
    if value in (None, ""):
        return None
    v = str(value).strip()
    try:
        return datetime.strptime(v[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _ts_bounds(date_from: Optional[str], date_to: Optional[str]) -> tuple[int, int]:
    """Границы unix-диапазона по датам. Даты трактуются в UTC — та же
    каноническая таймзона суточных срезов, что и у flow_daily / _day_expr
    (см. FlowService.apply_event_to_daily), поэтому фильтр диапазона и
    группировка по дням согласованы между собой. При отсутствии границ —
    максимально широкий («всё время»), чтобы один и тот же SQL работал для
    произвольных фильтров."""
    lo = 0
    hi = 4_102_444_800  # 2100-01-01
    if date_from:
        lo = int(datetime.strptime(date_from, "%Y-%m-%d")
                 .replace(tzinfo=timezone.utc).timestamp())
    if date_to:
        hi = int((datetime.strptime(date_to, "%Y-%m-%d")
                  + timedelta(days=1) - timedelta(seconds=1))
                 .replace(tzinfo=timezone.utc).timestamp())
    if lo > hi:  # защита от перевёрнутого диапазона
        lo, hi = hi, lo
    return lo, hi


def _day_expr(ts_col: str) -> str:
    """Выражение SQL: unix-ts → дата YYYY-MM-DD.

    Методика совпадает с FlowService.apply_event_to_daily (Этап 6): UTC —
    единая каноническая таймзона проекта для суточных срезов (flow_daily
    тоже пишется по UTC-датам), поэтому отчёты по дням/месяцам согласованы
    с агрегатами расхода независимо от TZ окружения."""
    return f"strftime('%Y-%m-%d', {ts_col}, 'unixepoch')"


# ---------------------------------------------------------------------- отчёты
def report_by_zones(conn: sqlite3.Connection, date_from: Optional[str],
                    date_to: Optional[str]) -> list[dict[str, Any]]:
    """Сводка по зонам: прогоны (watering_runs.zones_json → номера зон),
    литры и аварии — из watering_events (watering_event_zones)."""
    lo, hi = _ts_bounds(date_from, date_to)
    # Прямое сопоставление run→zone по zones_json (номера зон) делаем в Python:
    # JSON в SQLite без json_each гарантированно недоступен во всех сборках —
    # берём прогоны одним запросом и распространяем на зоны (это не N+1:
    # два запроса на весь отчёт независимо от числа зон).
    runs = conn.execute(
        """SELECT wr.id, wr.controller_id, wr.program_id, wr.source, wr.status,
                  wr.reason_code, wr.water_sec, wr.volume_liters, wr.zones_json,
                  EXISTS(SELECT 1 FROM watering_events e
                         WHERE e.controller_id=wr.controller_id AND e.run_id=wr.run_id
                           AND e.status IN ('finished','stopped')) AS has_fact
           FROM watering_runs wr
           WHERE COALESCE(wr.actual_start_ts, wr.planned_start_ts, 0) BETWEEN ? AND ?""",
        (lo, hi)).fetchall()
    events = conn.execute(
        """SELECT ez.zone_id, e.status, e.reason_code, e.aborted,
                  CASE WHEN e.volume_liters IS NULL THEN COALESCE(ez.volume_liters,0)
                        WHEN COALESCE((SELECT SUM(COALESCE(z.volume_liters,0))
                                       FROM watering_event_zones z WHERE z.event_id=e.id),0)>0
                        THEN e.volume_liters * COALESCE(ez.volume_liters,0) /
                             (SELECT SUM(COALESCE(z.volume_liters,0))
                              FROM watering_event_zones z WHERE z.event_id=e.id)
                        ELSE e.volume_liters / MAX(1,(SELECT COUNT(*)
                              FROM watering_event_zones z WHERE z.event_id=e.id)) END AS volume_liters,
                  COALESCE(ez.water_sec, e.water_sec, 0) AS water_sec
           FROM watering_events e
           JOIN watering_event_zones ez ON ez.event_id = e.id
           WHERE e.start_ts BETWEEN ? AND ?""",
        (lo, hi)).fetchall()

    zones = conn.execute(
        """SELECT z.id, z.zone_number, z.name, z.controller_id, c.name AS cname
           FROM zones z JOIN controllers c ON c.id = z.controller_id
           ORDER BY c.name, z.zone_number""").fetchall()

    agg: dict[int, dict[str, Any]] = {
        r["id"]: {
            "zone_id": r["id"], "zone_number": r["zone_number"],
            "zone_name": r["name"], "controller_id": r["controller_id"],
            "controller_name": r["cname"],
            "runs_count": 0, "total_minutes": 0.0, "total_liters": 0.0,
            "avg_duration_min": 0.0, "skips_count": 0, "errors_count": 0,
        } for r in zones}

    # Зоны контроллера: номер → id (для разбора zones_json).
    num_to_id: dict[tuple[int, int], int] = {
        (r["controller_id"], r["zone_number"]): r["id"] for r in zones}

    import json as _json
    for run in runs:
        nums: list[int] = []
        try:
            nums = [int(n) for n in (_json.loads(run["zones_json"]) or [])]
        except (ValueError, TypeError):
            nums = []
        for n in nums:
            zid = num_to_id.get((run["controller_id"], n))
            a = agg.get(zid) if zid is not None else None
            if a is None:
                continue
            status = run["status"]
            if status in ("completed", "active"):
                a["runs_count"] += 1
                a["total_minutes"] += (run["water_sec"] or 0) / 60.0
                if run["volume_liters"] is not None and not run["has_fact"]:
                    a["total_liters"] += float(run["volume_liters"]) / max(1, len(nums))
            elif status == "skipped":
                a["skips_count"] += 1
            if status in ("aborted", "failed"):
                a["errors_count"] += 1

    for ev in events:
        a = agg.get(ev["zone_id"])
        if a is None:
            continue
        if ev["status"] in ("finished", "stopped"):
            a["total_liters"] += float(ev["volume_liters"] or 0)
        bad = (ev["reason_code"] in
               ("no_flow", "over_flow", "under_flow", "emergency_stop")
               or bool(ev["aborted"]))
        if bad and ev["status"] == "stopped":
            a["errors_count"] += 1

    result = []
    for a in agg.values():
        if a["runs_count"] > 0:
            a["avg_duration_min"] = round(a["total_minutes"] / a["runs_count"], 2)
        a["total_minutes"] = round(a["total_minutes"], 2)
        a["total_liters"] = round(a["total_liters"], 2)
        result.append(a)
    result.sort(key=lambda x: (x["controller_name"], x["zone_number"]))
    return result


def report_by_controllers(conn: sqlite3.Connection, date_from: Optional[str],
                          date_to: Optional[str]) -> list[dict[str, Any]]:
    """Сводка по контроллерам за диапазон (один GROUP BY по watering_runs +
    подзапрос событий для литров/аварий)."""
    lo, hi = _ts_bounds(date_from, date_to)
    # SQLite не поддерживает COUNT(*) FILTER (ANSI-фильтры появились только
    # в 3.30+ и не во всех сборках) — эквивалент через SUM(CASE WHEN ...).
    rows = conn.execute(
        """
        SELECT c.id AS controller_id, c.name AS controller_name, c.box_id,
               SUM(CASE WHEN wr.status IN ('completed','active')
                        THEN 1 ELSE 0 END) AS runs_count,
               COALESCE(SUM(CASE WHEN wr.status IN ('completed','active')
                                 THEN wr.water_sec ELSE 0 END), 0) / 60.0
                   AS total_minutes,
               SUM(CASE WHEN wr.status = 'skipped' THEN 1 ELSE 0 END)
                   AS skips_count,
               SUM(CASE WHEN wr.status IN ('aborted','failed')
                        THEN 1 ELSE 0 END) AS errors_count
        FROM controllers c
        LEFT JOIN watering_runs wr
               ON wr.controller_id = c.id
              AND COALESCE(wr.actual_start_ts, wr.planned_start_ts, 0)
                  BETWEEN ? AND ?
        GROUP BY c.id
        ORDER BY c.name""", (lo, hi)).fetchall()

    lmap = {r["controller_id"]: r for r in conn.execute(
        """SELECT controller_id,
                  COALESCE(SUM(COALESCE(volume_liters,0)),0) AS total_liters,
                  SUM(CASE WHEN reason_code IN
                          ('no_flow','over_flow','under_flow','emergency_stop')
                          OR aborted=1 THEN 1 ELSE 0 END) AS event_errors
           FROM watering_events
           WHERE status IN ('finished','stopped') AND start_ts BETWEEN ? AND ?
           GROUP BY controller_id""", (lo, hi)).fetchall()}

    result = []
    for r in rows:
        lr = lmap.get(r["controller_id"])
        runs_count = r["runs_count"] or 0
        total_minutes = round(r["total_minutes"] or 0.0, 2)
        result.append({
            "controller_id": r["controller_id"],
            "controller_name": r["controller_name"],
            "box_id": r["box_id"],
            "runs_count": runs_count,
            "total_minutes": total_minutes,
            "total_liters": round(float(lr["total_liters"]) if lr else 0.0, 2),
            "avg_duration_min": (round(total_minutes / runs_count, 2)
                                 if runs_count else 0.0),
            "skips_count": r["skips_count"] or 0,
            "errors_count": (r["errors_count"] or 0)
                            + int((lr["event_errors"] if lr else 0) or 0),
        })
    return result


def report_by_days(conn: sqlite3.Connection, date_from: Optional[str],
                   date_to: Optional[str]) -> list[dict[str, Any]]:
    """Дневная сводка: прогоны/минуты/литры/ошибки по дням (GROUP BY по дате)."""
    lo, hi = _ts_bounds(date_from, date_to)
    runs = conn.execute(
        f"""SELECT {_day_expr("COALESCE(wr.actual_start_ts, wr.planned_start_ts)")} AS day,
                   COUNT(*) AS runs_count,
                   SUM(wr.water_sec)/60.0 AS total_minutes,
                   SUM(CASE WHEN wr.status IN ('aborted','failed') THEN 1 ELSE 0 END)
                       AS errors_count
            FROM watering_runs wr
            WHERE wr.status IN ('completed','active','aborted','failed')
              AND COALESCE(wr.actual_start_ts, wr.planned_start_ts, 0)
                  BETWEEN ? AND ?
            GROUP BY day""", (lo, hi)).fetchall()
    ev = conn.execute(
        f"""SELECT {_day_expr("e.start_ts")} AS day,
                   COALESCE(SUM(COALESCE(e.volume_liters,0)),0) AS total_liters
            FROM watering_events e
            WHERE e.status IN ('finished','stopped')
              AND e.start_ts BETWEEN ? AND ?
            GROUP BY day""", (lo, hi)).fetchall()
    fmap = conn.execute(
        """SELECT date AS day, COALESCE(SUM(total_volume_l),0) AS flow_liters
           FROM flow_daily WHERE attribution='controller'
           GROUP BY date""").fetchall()

    days: dict[str, dict[str, Any]] = {}
    for r in runs:
        if r["day"] is None:
            continue
        d = days.setdefault(r["day"], _empty_day(r["day"]))
        d["runs_count"] += r["runs_count"]
        d["total_minutes"] += r["total_minutes"] or 0
        d["errors_count"] += r["errors_count"]
    lit_map: dict[str, float] = {}
    for r in ev:
        if r["day"]:
            lit_map[r["day"]] = lit_map.get(r["day"], 0.0) + float(r["total_liters"])
    for r in fmap:
        if r["day"]:
            lit_map.setdefault(r["day"], 0.0)
    for day in set(list(days.keys()) + list(lit_map.keys())):
        d = days.setdefault(day, _empty_day(day))
        d["total_liters"] = round(lit_map.get(day, 0.0), 2)
    result = sorted(days.values(), key=lambda x: x["day"])
    for d in result:
        d["total_minutes"] = round(d["total_minutes"], 2)
    return result


def _empty_day(day: str) -> dict[str, Any]:
    return {"day": day, "runs_count": 0, "total_minutes": 0.0,
            "total_liters": 0.0, "errors_count": 0}


def report_by_months(conn: sqlite3.Connection, date_from: Optional[str],
                     date_to: Optional[str]) -> list[dict[str, Any]]:
    """Месячная сводка (группировка substr(day,1,7))."""
    daily = report_by_days(conn, date_from, date_to)
    months: dict[str, dict[str, Any]] = {}
    for d in daily:
        m = d["day"][:7]
        mm = months.setdefault(m, {"month": m, "runs_count": 0,
                                   "total_minutes": 0.0, "total_liters": 0.0,
                                   "errors_count": 0})
        mm["runs_count"] += d["runs_count"]
        mm["total_minutes"] += d["total_minutes"]
        mm["total_liters"] += d["total_liters"]
        mm["errors_count"] += d["errors_count"]
    out = sorted(months.values(), key=lambda x: x["month"])
    for mm in out:
        mm["total_minutes"] = round(mm["total_minutes"], 2)
        mm["total_liters"] = round(mm["total_liters"], 2)
    return out


def report_by_programs(conn: sqlite3.Connection, date_from: Optional[str],
                       date_to: Optional[str]) -> list[dict[str, Any]]:
    """Сводка по программам (программа прогона; безпрограммные прогоны —
    строка program_id=NULL, «Без программы»)."""
    lo, hi = _ts_bounds(date_from, date_to)
    rows = conn.execute(
        """
        SELECT wr.program_id, COALESCE(p.name, 'Без программы') AS program_name,
               COUNT(*) AS runs_count,
               SUM(wr.water_sec)/60.0 AS total_minutes,
               SUM(CASE WHEN wr.status IN ('aborted','failed') THEN 1 ELSE 0 END)
                   AS errors_count,
               SUM(CASE WHEN wr.status = 'skipped' THEN 1 ELSE 0 END)
                   AS skips_count
        FROM watering_runs wr
        LEFT JOIN programs p ON p.id = wr.program_id
        WHERE wr.status IN ('completed','active','aborted','failed','skipped')
          AND COALESCE(wr.actual_start_ts, wr.planned_start_ts, 0)
              BETWEEN ? AND ?
        GROUP BY wr.program_id
        ORDER BY program_name""", (lo, hi)).fetchall()
    liters = {r["program_id"]: float(r["l"] or 0) for r in conn.execute(
        """SELECT wr.program_id, SUM(COALESCE(e.volume_liters, wr.volume_liters, 0)) AS l
           FROM watering_runs wr
           LEFT JOIN watering_events e ON e.run_id = wr.run_id
                              AND e.status IN ('finished','stopped')
           WHERE COALESCE(wr.actual_start_ts, wr.planned_start_ts, 0)
                 BETWEEN ? AND ?
           GROUP BY wr.program_id""", (lo, hi)).fetchall()}
    result = []
    for r in rows:
        runs_count = r["runs_count"] or 0
        total_minutes = round(r["total_minutes"] or 0.0, 2)
        result.append({
            "program_id": r["program_id"],
            "program_name": r["program_name"],
            "runs_count": runs_count,
            "total_minutes": total_minutes,
            "total_liters": round(liters.get(r["program_id"], 0.0), 2),
            "avg_duration_min": (round(total_minutes / runs_count, 2)
                                 if runs_count else 0.0),
            "skips_count": r["skips_count"] or 0,
            "errors_count": r["errors_count"] or 0,
        })
    return result


def report_by_incidents(conn: sqlite3.Connection, date_from: Optional[str],
                        date_to: Optional[str]) -> dict[str, Any]:
    """Аварии и пропуски: список инцидентов + счётчики по типам.

    Источники: watering_runs (skipped/aborted/failed), watering_events
    (reason_code аварий / aborted), notifications (severity warning+).
    """
    lo, hi = _ts_bounds(date_from, date_to)
    items: list[dict[str, Any]] = []

    for r in conn.execute(
        """SELECT wr.id, wr.controller_id, c.name AS controller_name,
                  wr.status, wr.reason_code, wr.source,
                  COALESCE(wr.actual_start_ts, wr.planned_start_ts) AS ts
           FROM watering_runs wr JOIN controllers c ON c.id = wr.controller_id
           WHERE wr.status IN ('skipped','aborted','failed')
             AND COALESCE(wr.actual_start_ts, wr.planned_start_ts, 0)
                 BETWEEN ? AND ?""", (lo, hi)).fetchall():
        kind = ("skip" if r["status"] == "skipped" else "error")
        items.append({"kind": kind, "source_table": "watering_runs",
                      "ref_id": r["id"], "controller_id": r["controller_id"],
                      "controller_name": r["controller_name"],
                      "reason": r["reason_code"] or r["status"],
                      "detail": f"run {r['source']}", "ts": r["ts"]})

    for r in conn.execute(
        """SELECT e.id, e.controller_id, c.name AS controller_name,
                  e.reason_code, e.status, e.start_ts AS ts
           FROM watering_events e JOIN controllers c ON c.id = e.controller_id
           WHERE e.start_ts BETWEEN ? AND ?
             AND (e.reason_code IN ('no_flow','over_flow','under_flow',
                                    'emergency_stop') OR e.aborted = 1)""",
            (lo, hi)).fetchall():
        items.append({"kind": "error", "source_table": "watering_events",
                      "ref_id": r["id"], "controller_id": r["controller_id"],
                      "controller_name": r["controller_name"],
                      "reason": r["reason_code"] or "emergency_stop",
                      "detail": f"event {r['status']}", "ts": r["ts"]})

    for r in conn.execute(
        """SELECT n.id, n.controller_id, c.name AS controller_name,
                  n.type, n.severity, n.message,
                  strftime('%s', n.created_at) AS ts
           FROM notifications n
           LEFT JOIN controllers c ON c.id = n.controller_id
           WHERE n.severity IN ('warning','error','critical')""").fetchall():
        ts = int(r["ts"] or 0)
        if not (lo <= ts <= hi):
            continue
        items.append({"kind": "notification", "source_table": "notifications",
                      "ref_id": r["id"], "controller_id": r["controller_id"],
                      "controller_name": r["controller_name"],
                      "reason": r["type"], "detail": r["message"],
                      "severity": r["severity"], "ts": ts})

    counters = {"skips": sum(1 for i in items if i["kind"] == "skip"),
                "errors": sum(1 for i in items if i["kind"] == "error"),
                "notifications": sum(1 for i in items
                                     if i["kind"] == "notification")}
    by_reason: dict[str, int] = {}
    for i in items:
        by_reason[i["reason"]] = by_reason.get(i["reason"], 0) + 1
    items.sort(key=lambda i: (i["ts"] or 0), reverse=True)
    return {"items": items, "counters": counters, "by_reason": by_reason}


# ------------------------------------------------------------------ имена
def report_by_missed(conn, date_from, date_to):
    """Individual planned skips/cancellations, with reason and affected zones."""
    lo, hi = _ts_bounds(date_from, date_to)
    return [dict(row) for row in conn.execute(
        """SELECT wr.id AS run_ref, wr.run_id, wr.controller_id,
                  c.name AS controller_name, wr.program_id, wr.status,
                  COALESCE(wr.reason_code,wr.status) AS reason,
                  wr.planned_start_ts, wr.zones_json
           FROM watering_runs wr JOIN controllers c ON c.id=wr.controller_id
           WHERE wr.status IN ('skipped','cancelled')
             AND wr.planned_start_ts BETWEEN ? AND ?
           ORDER BY wr.planned_start_ts DESC,wr.id DESC""", (lo,hi))]


REPORTS = {
    "zones": report_by_zones,
    "controllers": report_by_controllers,
    "days": report_by_days,
    "months": report_by_months,
    "programs": report_by_programs,
    "incidents": report_by_incidents,
    "missed": report_by_missed,
}

CSV_HEADERS = {
    "missed": ["run_ref", "run_id", "controller_id", "controller_name",
               "program_id", "status", "reason", "planned_start_ts", "zones_json"],
    "zones": ["zone_id", "zone_number", "zone_name", "controller_id",
              "controller_name", "runs_count", "total_minutes",
              "total_liters", "avg_duration_min", "skips_count",
              "errors_count"],
    "controllers": ["controller_id", "controller_name", "box_id",
                    "runs_count", "total_minutes", "total_liters",
                    "avg_duration_min", "skips_count", "errors_count"],
    "days": ["day", "runs_count", "total_minutes", "total_liters",
             "errors_count"],
    "months": ["month", "runs_count", "total_minutes", "total_liters",
               "errors_count"],
    "programs": ["program_id", "program_name", "runs_count", "total_minutes",
                 "total_liters", "avg_duration_min", "skips_count",
                 "errors_count"],
    "incidents": ["kind", "source_table", "ref_id", "controller_id",
                  "controller_name", "reason", "detail", "severity", "ts"],
}


def report_rows_for_csv(name: str, data: Any) -> list[list[Any]]:
    """Подготовить строки CSV для отчёта (инциденты — из вложенного items)."""
    if name == "incidents":
        items = data["items"]
    else:
        items = data
    header = CSV_HEADERS[name]
    out = []
    for row in items:
        out.append([row.get(k) for k in header])
    return out
