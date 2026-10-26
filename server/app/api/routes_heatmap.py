"""REST API Этапа 8 (блок 8.3): тепловая карта полива (Артефакт 0.6 §5.9).

Эндпоинты:
- GET /api/v1/heatmap?date=YYYY-MM-DD&interval_min=15&controller_ids=1,2
  — сетка «контроллеры → зоны × временные слоты» за сутки.

Параметры:
- date — дата (ISO); по умолчанию — сегодня;
- interval_min — 5 | 10 | 15 | 30 (по умолчанию 15);
- controller_ids — опциональный список id через запятую.

Ошибки: 401/403 — ACL (viewer=read); 422 — некорректная дата/интервал/id.

Дубли без префикса (/api/heatmap) зарегистрированы в стиле остального API
проекта (stage3..7, reports); канонический путь Артефакта 0.6 —
/api/v1/heatmap.

Производительность (DoD Этапа 8): сервис делает ровно 4 SQL-запроса и
собирает 50×16×96 ячеек арифметикой целых чисел — ответ отдаётся синхронно,
браузер обновляет страницу polling'ом (без WebSocket/SSE — запрет этапа).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ..infra.logging import get_logger
from ..services import heatmap_service as hm
from .routes_stage3 import _user

log = get_logger("poliv.api8.heatmap")

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


def register_api_routes_heatmap(app: FastAPI, cfg, conn: sqlite3.Connection) -> None:
    """Регистрирует маршруты тепловой карты (/api/v1/heatmap + алиас)."""

    def api(method: str, path: str):
        def decorator(handler):
            app.add_api_route(path, handler, methods=[method])
            alias = "/api/heatmap"
            app.add_api_route(alias, handler, methods=[method],
                              include_in_schema=False)
            return handler
        return decorator

    @api("GET", "/api/v1/heatmap")
    async def heatmap(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        qp = request.query_params

        raw_date = (qp.get("date") or "").strip()
        if not raw_date:
            date_str = datetime.now().strftime("%Y-%m-%d")
        else:
            try:
                d = datetime.strptime(raw_date, "%Y-%m-%d")
                date_str = d.strftime("%Y-%m-%d")
            except ValueError:
                return JSONResponse(
                    {"detail": "date должен быть в формате YYYY-MM-DD"},
                    status_code=422)

        raw_interval = (qp.get("interval_min") or "15").strip()
        try:
            interval_min = int(raw_interval)
        except ValueError:
            return JSONResponse(
                {"detail": "interval_min должен быть целым числом"},
                status_code=422)
        if interval_min not in hm.VALID_INTERVALS:
            return JSONResponse(
                {"detail": "interval_min должен быть одним из "
                           + ", ".join(map(str, hm.VALID_INTERVALS))},
                status_code=422)

        controller_ids = None
        raw_ids = (qp.get("controller_ids") or "").strip()
        if raw_ids:
            try:
                controller_ids = [int(x) for x in raw_ids.split(",") if x.strip()]
            except ValueError:
                return JSONResponse(
                    {"detail": "controller_ids — список целых id через запятую"},
                    status_code=422)
            if not controller_ids:
                controller_ids = None

        data = hm.build_heatmap(conn, date_str,
                                interval_min=interval_min,
                                controller_ids=controller_ids)
        return data
