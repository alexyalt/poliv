"""REST API Этапа 7: погода и погодные правила (Артефакт 0.6 §weather).

Эндпоинты (промпт Этапа 7, Шаг 5):
- GET    /api/v1/weather/current            — сводка «сейчас» (+offline-статус);
- GET    /api/v1/weather/forecast           — прогноз на 72 часа;
- GET    /api/v1/weather/rules              — список правил;
- POST   /api/v1/weather/rules              — создать правило (operator+);
- PUT    /api/v1/weather/rules/{id}         — изменить (operator+);
- DELETE /api/v1/weather/rules/{id}         — удалить (admin);
- POST   /api/v1/weather/rules/{id}/test    — dry-run diff для следующей
  машинограммы контроллера (operator+);
- GET    /api/v1/weather/applications       — журнал применений (пагинация);
- POST   /api/v1/weather/refresh            — принудительное обновление
  факта/прогноза из Open-Meteo (operator+).

Дубли без префикса (/api/weather/*) зарегистрированы в стиле остального API
проекта (stage3..6 используют /api/...); канонический путь Артефакта 0.6 —
/api/v1/weather/*, оба ведут в один handler.

Роли: viewer — чтение; operator — чтение + CRUD правил + test + refresh;
admin — всё, включая удаление правил. 401/403/404/422 — как в stage3..6.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from ..infra.db import connect_path

from ..infra.logging import get_logger
from ..services import weather_service as wx
from .routes_stage3 import _user

log = get_logger("poliv.api7")

ROLE_LEVELS = {"viewer": 0, "operator": 1, "admin": 2}


def _check(request: Request, min_role: str = "viewer"):
    user = _user(request)
    if user is None:
        return JSONResponse({"detail": "Требуется вход в систему"},
                            status_code=401)
    if ROLE_LEVELS.get(user["role"], 0) < ROLE_LEVELS[min_role]:
        return JSONResponse(
            {"detail": f"Недостаточно прав: требуется роль {min_role}"},
            status_code=403)
    return None


def _int_param(request: Request, name: str, default: int,
               minimum: int, maximum: int) -> int:
    raw = request.query_params.get(name)
    try:
        value = int(raw) if raw not in (None, "") else default
    except ValueError:
        return default
    return max(minimum, min(value, maximum))


async def _body(request: Request) -> dict:
    try:
        data = await request.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def register_api_routes_weather(app: FastAPI, cfg, conn: sqlite3.Connection) -> None:
    """Регистрирует маршруты погоды (префикс /api/v1/weather + алиасы /api/weather)."""

    def api(method: str, path: str):
        def decorator(handler):
            app.add_api_route(path, handler, methods=[method])
            alias = "/api/weather" + path[len("/api/v1/weather"):]
            app.add_api_route(alias, handler, methods=[method], include_in_schema=False)
            return handler
        return decorator

    # ---------------------------------------------------------------- current
    @api("GET", "/api/v1/weather/outlook")
    async def outlook(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        from ..services.weather_outlook import view
        return view(conn)

    @api("GET", "/api/v1/weather/current")
    async def weather_current(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        return wx.current_conditions(conn)

    # --------------------------------------------------------------- forecast
    @api("GET", "/api/v1/weather/forecast")
    async def weather_forecast(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        hours = _int_param(request, "hours", 72, 1, 240)
        rows = wx.fresh_forecasts(conn, limit=hours)
        return {"hours": hours, "count": len(rows), "items": rows}

    # ------------------------------------------------------------------ rules
    @api("GET", "/api/v1/weather/rules")
    async def rules_list(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        return {"items": wx.list_rules(conn)}

    @api("POST", "/api/v1/weather/rules")
    async def rules_create(request: Request):
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        data = await _body(request)
        try:
            rule = wx.create_rule(conn, data, actor=_user(request)["username"])
        except wx.RuleValidationError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=422)
        return JSONResponse(rule, status_code=201)

    @api("PUT", "/api/v1/weather/rules/{rule_id}")
    async def rules_update(request: Request, rule_id: int):
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        data = await _body(request)
        try:
            rule = wx.update_rule(conn, rule_id, data)
        except wx.RuleValidationError as exc:
            msg = str(exc)
            code = 404 if msg == "Правило не найдено" else 422
            return JSONResponse({"detail": msg}, status_code=code)
        return rule

    @api("DELETE", "/api/v1/weather/rules/{rule_id}")
    async def rules_delete(request: Request, rule_id: int):
        denied = _check(request, "admin")
        if denied is not None:
            return denied
        if not wx.delete_rule(conn, rule_id):
            return JSONResponse({"detail": "Правило не найдено"},
                                status_code=404)
        return {"deleted": rule_id}

    # ------------------------------------------------------------------- test
    @api("POST", "/api/v1/weather/rules/{rule_id}/test")
    async def rules_test(request: Request, rule_id: int):
        """«Применить в сухую»: компиляция preview с правилом, diff до/после.

        Тело: {"controller_id": N}. Возвращает применённые действия и
        изменённые запуски; в weather_rule_applications НИЧЕГО не пишется.
        """
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        rule = wx.get_rule(conn, rule_id)
        if rule is None:
            return JSONResponse({"detail": "Правило не найдено"}, status_code=404)
        data = await _body(request)
        cid = data.get("controller_id")
        try:
            cid = int(cid)
        except (TypeError, ValueError):
            return JSONResponse({"detail": "Нужен controller_id (целое)"},
                                status_code=422)
        ctrl = conn.execute("SELECT * FROM controllers WHERE id=? AND "
                            "deleted_at IS NULL", (cid,)).fetchone()
        if ctrl is None:
            return JSONResponse({"detail": "Контроллер не найден"},
                                status_code=404)

        from ..core.schedule_compiler.compiler import compile_schedule
        from datetime import date as _date
        # baseline: текущая компиляция БЕЗ правил
        before = await run_in_threadpool(compile_schedule, conn, dict(ctrl), today=_date.today(), weather_rules=[])
        after = await run_in_threadpool(compile_schedule, conn, dict(ctrl), today=_date.today(), weather_rules=[rule])
        diff_runs = [s for s in after.skipped_runs
                     if s.get("reason_code") == "weather_skip"]
        fired = bool(after.weather_applications) or bool(diff_runs)
        return {
            "rule_id": rule_id,
            "would_apply": fired,
            "applications": after.weather_applications,
            "skipped_runs": diff_runs,
            "runs_before": before.runs_count,
            "runs_after": after.runs_count,
            "hash_before": before.schedule_hash,
            "hash_after": after.schedule_hash,
        }

    # ------------------------------------------------------------ applications
    @api("GET", "/api/v1/weather/applications")
    async def applications_list(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        page = _int_param(request, "page", 1, 1, 100000)
        page_size = _int_param(request, "page_size", 100, 1, 100)
        items, total = wx.list_applications(
            conn, limit=page_size, offset=(page - 1) * page_size)
        return {"items": items, "page": page, "page_size": page_size,
                "total_items": total}

    # ---------------------------------------------------------------- refresh
    @api("POST", "/api/v1/weather/refresh")
    async def weather_refresh(request: Request):
        """Принудительно обновить факт/прогноз из Open-Meteo (graceful)."""
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        db_path = conn.execute("PRAGMA database_list").fetchone()[2]

        def refresh():
            owned = connect_path(db_path)
            try:
                obs = wx.update_observations(owned)
                fc_rows = wx.update_forecasts(owned)
                from ..services.weather_outlook import update as update_outlook
                update_outlook(owned)
                return {"observation_saved": obs is not None,
                        "forecast_rows_saved": fc_rows,
                        "conditions": wx.current_conditions(owned)}
            finally:
                owned.close()

        return await run_in_threadpool(refresh)
