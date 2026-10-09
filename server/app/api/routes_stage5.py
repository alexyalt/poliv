"""REST API Этапа 5: view-модель операторского интерфейса (префикс /api).

Назначение (ТЗ Этап 5, п.1–2, п.5): дашборду и странице контроллера нужны
НЕ «сырые» live-поля, а готовые для отображения данные — остаток времени фазы/
запуска, последняя активность зоны, объём полива за сегодня, ошибки. Фронтенд
не должен пересчитывать timestamps сам (часовые пояса, целочисленные unix-секунды).

Контракт:
- GET /api/dashboard-view           — плитки контроллеров + сводка за сегодня;
- GET /api/controllers/{id}/view    — страница контроллера: live + зоны с
  активностью + последние прогоны + ошибки компиляции + отложенная машинограмма.

Оба ответа обновляются фронтендом опросом каждые 10 секунд (polling, без
WebSocket — ТЗ Этап 5, п.5). Роли/формат ошибок — как routes_stage3/_4
(viewer+; 401/403/404 в {"detail": ...}).

Данные берутся из существующих источников (новые таблицы НЕ вводятся):
- live-state контроллеров (миграция 0004) через live_view() Этапа 3;
- watering_runs (миграция 0005) — «объём за сегодня», история прогонов;
- schedule_compile_errors / pending_schedule (миграция 0007) — «ошибки»;
- zones (миграции 0001/0002) — карточки зон страницы контроллера.

«Сегодня» считается по локальной дате сервера (datetime.now()), что совпадает
с методикой компилятора (машинограммы строятся по локальным датам/минутам).
"""
from __future__ import annotations

import functools
import inspect
import json
import sqlite3
from datetime import datetime, time as dtime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .routes_stage3 import LIVE_SELECT, _user, live_view

ROLE_LEVELS = {"viewer": 0, "operator": 1, "admin": 2}


def _json_list(value) -> list:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _local_day_bounds() -> tuple[int, int]:
    """Unix-границы [начало суток, начало следующих) локального дня сервера."""
    now_local = datetime.now()
    start = datetime.combine(now_local.date(), dtime.min).timestamp()
    return int(start), int(start + 86400)


def _iso_utc(ts) -> str | None:
    """unix-секунды -> ISO8601 UTC (то же представление, что last_seen_at в БД)."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat(
            timespec="seconds")
    except (TypeError, ValueError, OSError):
        return None


def remaining_seconds(end_ts, now_ts: int) -> int | None:
    """Остаток времени по end_ts (unix); None если не задан; <=0 — просрочено."""
    if end_ts is None:
        return None
    return int(end_ts) - now_ts


def zone_activity(conn: sqlite3.Connection, controller_id: int,
                  now_ts: int) -> dict[int, dict]:
    """Активность зон контроллера: {номер зоны: {phase, remaining_sec, run_end_ts}}.

    Приоритет источника:
    1) активный прогон (watering_runs.status='active') с зонами текущего шага —
       зоны парсятся из details_json ({zones:[...]} или {zone:N}) либо из
       zones_json прогона; остаток — до run_end_ts;
    2) живое состояние контроллера (active_zones_json + phase_end_ts) —
       fallback, если таблица прогонов не заполнена (например, эмулятор без
       событий started).
    """
    out: dict[int, dict] = {}
    row = conn.execute(
        LIVE_SELECT + " WHERE id=? AND deleted_at IS NULL", (controller_id,)
    ).fetchone()
    if row is not None:
        d = dict(row)
        for z in _json_list(d.get("active_zones_json")):
            try:
                zn = int(z)
            except (TypeError, ValueError):
                continue
            out[zn] = {
                "phase": d.get("current_phase"),
                "remaining_sec": remaining_seconds(d.get("phase_end_ts"), now_ts),
                "run_end_ts": d.get("run_end_ts"),
            }
    run = conn.execute(
        """SELECT * FROM watering_runs
           WHERE controller_id=? AND status='active'
           ORDER BY actual_start_ts DESC, id DESC LIMIT 1""",
        (controller_id,),
    ).fetchone()
    if run is not None:
        rd = dict(run)
        zones: list[int] = []
        details = None
        if rd.get("details_json"):
            try:
                details = json.loads(rd["details_json"])
            except (TypeError, ValueError):
                details = None
        if isinstance(details, dict):
            zl = details.get("zones")
            if isinstance(zl, list):
                zones = zl
            elif details.get("zone") is not None:
                zones = [details["zone"]]
        if not zones:
            zones = _json_list(rd.get("zones_json"))
        rem = remaining_seconds(rd.get("run_end_ts"), now_ts)
        for z in zones:
            try:
                zn = int(z)
            except (TypeError, ValueError):
                continue
            cur = out.get(zn)
            if cur is None or cur.get("remaining_sec") is None:
                out[zn] = {
                    "phase": "water",
                    "remaining_sec": rem,
                    "run_end_ts": rd.get("run_end_ts"),
                }
    return out


def dashboard_tile(conn: sqlite3.Connection, row: sqlite3.Row,
                   now_ts: int, today_liters: float | None) -> dict:
    """Плитка дашборда (ТЗ Этап 5, п.1): статус, активная зона, остаток
    времени, последний контакт, объём за сегодня, ошибки."""
    v = live_view(row)
    d = dict(row)
    active = v["active_zones"]
    primary = v["primary_zone"] if v["primary_zone"] is not None else (
        active[0] if active else None)
    errors: list[str] = []
    if v["connection_status"] != "online":
        errors.append("Нет связи с контроллером")
    if not v["time_valid"]:
        errors.append("Время контроллера не синхронизировано")
    if v["emergency_lock_until_ts"] and v["emergency_lock_until_ts"] > now_ts:
        errors.append("Аварийная блокировка активна")
    ce = conn.execute(
        """SELECT error_json FROM schedule_compile_errors
           WHERE controller_id=? ORDER BY id DESC LIMIT 1""",
        (v["id"],),
    ).fetchone()
    if ce is not None:
        try:
            err = json.loads(ce["error_json"])
            errors.append("Расписание: " + str(err.get("message")
                          or err.get("code") or "ошибка компиляции"))
        except (TypeError, ValueError):
            errors.append("Расписание: ошибка компиляции (см. страницу контроллера)")
    pend = conn.execute(
        """SELECT COUNT(*) c FROM pending_schedule
           WHERE controller_id=? AND status='queued'""",
        (v["id"],),
    ).fetchone()["c"]
    if pend:
        errors.append(f"Машинограмма в очереди на отправку ({pend})")
    return {
        **v,
        "active_zone": primary,
        "phase_remaining_sec": remaining_seconds(v["phase_end_ts"], now_ts),
        "run_remaining_sec": remaining_seconds(v["run_end_ts"], now_ts),
        "pause_remaining_sec": remaining_seconds(v["pause_until_ts"], now_ts),
        "watered_today_liters": today_liters,
        "errors": errors,
    }


def liters_by_controller(conn: sqlite3.Connection,
                         day_start: int, day_end: int) -> dict[int, float]:
    """Объём за сегодня по контроллерам: сумма произведений
    (секунды полива зоны × ожидаемый расход lpm). Зоны без настроенного
    расхода в сумму не входят (данных для пересчёта нет)."""
    rows = conn.execute(
        """SELECT wr.controller_id cid, wr.zones_json, wr.details_json,
                  wr.water_sec, wr.status
           FROM watering_runs wr
           WHERE wr.status IN ('completed','aborted','active')
             AND COALESCE(wr.actual_start_ts, wr.planned_start_ts) >= ?
             AND COALESCE(wr.actual_start_ts, wr.planned_start_ts) < ?""",
        (day_start, day_end),
    ).fetchall()
    zone_lpm: dict[tuple[int, int], float] = {}
    for z in conn.execute(
            """SELECT controller_id, zone_number, expected_flow_lpm
               FROM zones WHERE deleted_at IS NULL
                 AND expected_flow_lpm IS NOT NULL AND expected_flow_lpm > 0"""
    ).fetchall():
        zone_lpm[(z["controller_id"], int(z["zone_number"]))] = float(
            z["expected_flow_lpm"])
    totals: dict[int, float] = {}
    for r in rows:
        zones = _json_list(r["zones_json"])
        if r["details_json"]:
            try:
                det = json.loads(r["details_json"])
                if isinstance(det, dict) and isinstance(det.get("zones"), list):
                    zones = det["zones"]
            except (TypeError, ValueError):
                pass
        sec = int(r["water_sec"] or 0)
        if not zones or sec <= 0:
            continue
        per_zone = sec / max(len(zones), 1)
        for z in zones:
            try:
                lpm = zone_lpm.get((r["cid"], int(z)))
            except (TypeError, ValueError):
                continue
            if lpm is None:
                continue
            totals[r["cid"]] = totals.get(r["cid"], 0.0) + per_zone * lpm / 60.0
    return {k: round(v, 1) for k, v in totals.items()}


def today_liters_for_controller(conn: sqlite3.Connection,
                                controller_id: int) -> tuple[float | None, dict]:
    """(литры за сегодня для контроллера, сводка «сегодня» по системе).

    Используется и view-API Этапа 5, и серверным рендером страницы
    контроллера (/controllers/{id}) — один источник расчёта объёма.
    """
    day_start, day_end = _local_day_bounds()
    liters = liters_by_controller(conn, day_start, day_end)
    row = conn.execute(
        """SELECT COALESCE(SUM(water_sec),0) sec, COUNT(*) cnt
           FROM watering_runs
           WHERE status IN ('completed','aborted','active')
             AND COALESCE(actual_start_ts, planned_start_ts) >= ?
             AND COALESCE(actual_start_ts, planned_start_ts) < ?""",
        (day_start, day_end),
    ).fetchone()
    summary = {"water_sec": int(row["sec"]), "runs": int(row["cnt"])}
    return liters.get(controller_id), summary


def register_api_routes_stage5(app: FastAPI, cfg, conn: sqlite3.Connection):

    def _check(request: Request, min_role: str = "viewer"):
        user = _user(request)
        if user is None:
            return JSONResponse({"detail": "Требуется вход в систему"}, status_code=401)
        if ROLE_LEVELS[user["role"]] < ROLE_LEVELS[min_role]:
            return JSONResponse(
                {"detail": f"Недостаточно прав: требуется роль {min_role}"},
                status_code=403)
        return None

    # ------------------------------------------------------ GET /api/dashboard-view
    async def dashboard_view(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        now_ts = int(datetime.now(timezone.utc).timestamp())
        day_start, day_end = _local_day_bounds()
        rows = conn.execute(
            LIVE_SELECT + " WHERE deleted_at IS NULL ORDER BY name").fetchall()
        liters = _liters_by_controller(day_start, day_end)
        tiles = [dashboard_tile(conn, r, now_ts, liters.get(dict(r)["id"]))
                 for r in rows]
        online = sum(1 for t in tiles if t["connection_status"] == "online")
        watering = [t for t in tiles
                    if t["mode"] in ("watering", "manual") or t["active_zones"]]
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "server_now_ts": now_ts,
            "summary": {
                "controllers_total": len(tiles),
                "controllers_online": online,
                "watering_now": len(watering),
                "today": _today_water_summary(),
            },
            "controllers": tiles,
        }

    # ------------------------------------------------ GET /api/controllers/{id}/view
    async def controller_view(request: Request, controller_id: int):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        row = conn.execute(
            LIVE_SELECT + " WHERE id=? AND deleted_at IS NULL",
            (controller_id,)).fetchone()
        if row is None:
            return JSONResponse({"detail": "Контроллер не найден"}, status_code=404)
        now_ts = int(datetime.now(timezone.utc).timestamp())
        day_start, day_end = _local_day_bounds()
        v = live_view(row)
        activity = zone_activity(conn, controller_id, now_ts)
        zones = []
        for z in conn.execute(
            """SELECT id, zone_number, name, enabled, icon, deleted_at,
                      base_duration_minutes, watering_adjustment_percent,
                      cycle_soak_enabled, expected_flow_lpm, notes
               FROM zones WHERE controller_id=?
               ORDER BY zone_number""", (controller_id,)).fetchall():
            zd = dict(z)
            act = activity.get(int(zd["zone_number"]))
            zd["active"] = act is not None
            zd["phase"] = act["phase"] if act else None
            zd["remaining_sec"] = act["remaining_sec"] if act else None
            zd["run_end_iso"] = _iso_utc(act["run_end_ts"]) if act else None
            zones.append(zd)
        runs = []
        for r in conn.execute(
            """SELECT wr.id, wr.run_id, wr.program_id, p.name AS program_name,
                      wr.source, wr.status, wr.reason_code,
                      wr.planned_start_ts, wr.actual_start_ts, wr.end_ts,
                      wr.water_sec, wr.zones_json, wr.schedule_version
               FROM watering_runs wr
               LEFT JOIN programs p ON p.id = wr.program_id
               WHERE wr.controller_id=?
               ORDER BY COALESCE(wr.actual_start_ts, wr.planned_start_ts) DESC,
                        wr.id DESC LIMIT 20""", (controller_id,)).fetchall():
            d = dict(r)
            d["zones"] = _json_list(d.pop("zones_json", None))
            d["planned_start_iso"] = _iso_utc(d.get("planned_start_ts"))
            d["actual_start_iso"] = _iso_utc(d.get("actual_start_ts"))
            d["end_iso"] = _iso_utc(d.get("end_ts"))
            runs.append(d)
        compile_error = None
        ce = conn.execute(
            """SELECT reason, error_json, ts FROM schedule_compile_errors
               WHERE controller_id=? ORDER BY id DESC LIMIT 1""",
            (controller_id,)).fetchone()
        if ce is not None:
            try:
                payload = json.loads(ce["error_json"])
            except (TypeError, ValueError):
                payload = {"message": ce["error_json"]}
            compile_error = {"reason": ce["reason"], "ts": ce["ts"], **payload} \
                if isinstance(payload, dict) else {"raw": payload, "ts": ce["ts"]}
        pending = conn.execute(
            """SELECT ps.id, ps.schedule_id, ps.status, ps.reason, ps.created_at,
                      cs.schedule_version
               FROM pending_schedule ps
               JOIN controller_schedules cs ON cs.id = ps.schedule_id
               WHERE ps.controller_id=? AND ps.status='queued'
               ORDER BY ps.id DESC LIMIT 5""", (controller_id,)).fetchall()
        rejection = conn.execute(
            """SELECT version, reason, ts FROM schedule_rejections
               WHERE controller_id=? ORDER BY id DESC LIMIT 1""",
            (controller_id,)).fetchone()
        liters = _liters_by_controller(day_start, day_end)
        return {
            "live": v,
            "now_ts": now_ts,
            "phase_remaining_sec": remaining_seconds(v["phase_end_ts"], now_ts),
            "run_remaining_sec": remaining_seconds(v["run_end_ts"], now_ts),
            "zones": zones,
            "runs": runs,
            "compile_error": compile_error,
            "pending_schedules": [dict(p) for p in pending],
            "last_rejection": dict(rejection) if rejection else None,
            "today_liters": liters.get(controller_id),
            "today_water_sec": _today_water_summary()["water_sec"],
        }

    # ------------------------------------------------------------ регистрация
    def api(method: str, path: str, handler):
        @functools.wraps(handler)
        async def endpoint(*args, **kwargs):
            try:
                return await handler(*args, **kwargs)
            except ValueError as exc:
                return JSONResponse({"detail": str(exc)}, status_code=422)

        endpoint.__name__ = f"{handler.__name__}_{method.lower()}"
        assert inspect.signature(endpoint).parameters.keys() == \
            inspect.signature(handler).parameters.keys()
        app.add_api_route(path, endpoint, methods=[method])

    # ВАЖНО про порядок: /api/dashboard-view — отдельный префикс;
    # /api/controllers/{id}/view не пересекается с /api/controllers/live
    # (тот зарегистрирован раньше в stage3 как точный путь).
    api("GET", "/api/dashboard-view", dashboard_view)
    api("GET", "/api/controllers/{controller_id}/view", controller_view)
