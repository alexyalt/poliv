"""REST API Этапа 8 (блок 8.1): отчёты базовой аналитики.

Эндпоинты (Артефакт 0.6 §4.15, префикс /api/v1/reports — как /api/v1/weather
в Этапе 7):
- GET /api/v1/reports/zones?from=&to=[&preset=]        — сводка по зонам;
- GET /api/v1/reports/controllers?from=&to=            — по контроллерам;
- GET /api/v1/reports/days?from=&to=                   — дневная сводка;
- GET /api/v1/reports/months?from=&to=                 — месячная сводка;
- GET /api/v1/reports/programs?from=&to=               — по программам;
- GET /api/v1/reports/incidents?from=&to=              — аварии и пропуски;
- GET /api/v1/reports/{вид}.csv                        — CSV-выгрузка того же
  отчёта (заголовки стабильны даже при пустом результате).

Параметры диапазона: from/to (YYYY-MM-DD) либо preset=today|week|month|
quarter|year (явные from/to имеют приоритет). Некорректная дата → 422.
Пагинация: limit/offset применяются к инцидентам (списочный отчёт);
агрегатные отчёты (зоны/контроллеры/дни/месяцы/программы) возвращаются
полностью — их размер ограничен числом объектов в справочниках.

Роли: viewer/operator/admin — чтение (минимальная роль viewer, как во всех
read-эндпоинтах проекта). CSV собирается общей утилитой infra/csv_export
(_csv_safe — защита от formula injection, вынесена из routes_stage6).
Дубли без префикса /api/reports/* зарегистрированы в стиле остального API
(см. routes_weather), канонический путь Артефакта 0.6 — /api/v1/reports/*.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request
from starlette.concurrency import run_in_threadpool
from ..infra.db import connect_path

from fastapi.responses import JSONResponse

from ..infra.csv_export import csv_response
from ..infra.logging import get_logger
from ..services import archive_service as arcsvc
from ..services import report_service as rs
from ..services.archive_service import ArchiveService
from .routes_stage3 import _user

log = get_logger("poliv.api8.reports")

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


def _range(request: Request) -> tuple[Optional[str], Optional[str]] | JSONResponse:
    """Разобрать from/to/preset; некорректная дата → 422."""
    qp = request.query_params
    date_from = (qp.get("from") or "").strip() or None
    date_to = (qp.get("to") or "").strip() or None
    preset = (qp.get("preset") or "").strip() or None
    if preset and preset not in rs.PRESETS:
        raise rs.RangeError(f"Неизвестный preset: {preset}")
    nf = rs._norm(date_from) if date_from else None
    nt = rs._norm(date_to) if date_to else None
    if date_from and nf is None:
        raise rs.RangeError("Параметр from должен быть датой YYYY-MM-DD")
    if date_to and nt is None:
        raise rs.RangeError("Параметр to должен быть датой YYYY-MM-DD")
    return rs.resolve_range(nf, nt, preset)


def register_api_routes_reports(app: FastAPI, cfg, conn: sqlite3.Connection) -> None:
    """Регистрирует маршруты отчётов (/api/v1/reports/* + алиасы /api/reports/*)."""

    def api(method: str, path: str):
        def decorator(handler):
            app.add_api_route(path, handler, methods=[method])
            alias = "/api/reports" + path[len("/api/v1/reports"):]
            app.add_api_route(alias, handler, methods=[method],
                              include_in_schema=False)
            return handler
        return decorator

    def _run(name: str, request: Request) -> Any:
        """Общий каркас: ACL → диапазон → сервис → (пагинация для incidents)."""
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        try:
            date_from, date_to = _range(request)
        except rs.RangeError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=422)
        fn = rs.REPORTS[name]
        data = fn(conn, date_from, date_to)
        if name == "incidents":
            total = len(data["items"])
            limit = _int_param(request, "limit", 200, 1, 10000)
            offset = _int_param(request, "offset", 0, 0, 10 ** 9)
            data = dict(data, items=data["items"][offset:offset + limit],
                        total=total, limit=limit, offset=offset)
        return {"report": name, "from": date_from, "to": date_to, "data": data}

    # ---------------------------------------------------------- JSON-отчёты
    for _name in rs.REPORTS:
        def make_handler(name=_name):
            async def handler(request: Request):
                return await run_in_threadpool(_run, name, request)
            handler.__name__ = f"report_{name}"
            return handler

        api("GET", f"/api/v1/reports/{_name}")(make_handler())

        # ------------------------------------------------------ CSV-выгрузка
        def make_csv_handler(name=_name):
            async def csv_handler(request: Request):
                denied = _check(request, "viewer")
                if denied is not None:
                    return denied
                try:
                    date_from, date_to = _range(request)
                except rs.RangeError as exc:
                    return JSONResponse({"detail": str(exc)}, status_code=422)
                data = await run_in_threadpool(rs.REPORTS[name], conn, date_from, date_to)
                rows = rs.report_rows_for_csv(name, data)
                header = rs.CSV_HEADERS[name]
                stamp = f"{date_from or 'all'}_{date_to or 'all'}"
                return csv_response(rows, header,
                                    filename=f"report_{name}_{stamp}.csv")
            csv_handler.__name__ = f"report_{name}_csv"
            return csv_handler

        api("GET", f"/api/v1/reports/{_name}.csv")(make_csv_handler())

    # ------------------------------------------------ Архив (блок 8.2, ADR-14)
    # Вспомогательные эндпоинты для ручной проверки ротации без кода на сервере
    # (см. docs/ИНСТРУКЦИЯ_ЭТАП8.md §3). Канонический корень данных — cfg-путь
    # рядом с БД (data/), как в main.py при старте ArchiveDaemon.
    data_root = cfg.data_dir

    def _archive_service() -> ArchiveService:
        return ArchiveService(conn, data_root)

    archive_db_path = conn.execute("PRAGMA database_list").fetchone()[2]

    def _rotate_owned(entity_type, dry_run, target_date):
        owned = connect_path(archive_db_path)
        try:
            service = ArchiveService(owned, data_root)
            if entity_type is None:
                return service.rotate_all(dry_run=dry_run, target_date=target_date)
            return service.rotate(entity_type, dry_run=dry_run, target_date=target_date)
        finally:
            owned.close()

    @app.get("/api/v1/archive/config")
    async def archive_config(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        return arcsvc.archive_config(conn)

    @app.get("/api/v1/archive/exports")
    async def archive_exports(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        entity_type = request.query_params.get("entity_type") or None
        if entity_type is not None and entity_type not in arcsvc.ENTITY_TYPES:
            return JSONResponse({"detail": "неизвестный entity_type"},
                                status_code=422)
        limit = _int_param(request, "limit", 100, 1, 1000)
        items = _archive_service().list_exports(entity_type, limit=limit)
        return {"items": items, "total": len(items)}

    @app.post("/api/v1/archive/rotate")
    async def archive_rotate(request: Request):
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"detail": "Invalid JSON body"}, status_code=422)
        if not isinstance(body, dict) or set(body) - {"dry_run", "target_date", "entity_type"}:
            return JSONResponse({"detail": "Expected archive request object"}, status_code=422)
        dry_run = body.get("dry_run", False)
        if type(dry_run) is not bool:
            return JSONResponse({"detail": "dry_run must be a boolean"}, status_code=422)
        target_date = body.get("target_date")
        entity_type = body.get("entity_type")
        if target_date is not None:
            try:
                if not isinstance(target_date, str) or len(target_date) != 10:
                    raise ValueError("Expected ISO date")
                datetime.strptime(target_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                return JSONResponse(
                    {"detail": "target_date должен быть YYYY-MM-DD"},
                    status_code=422)
        if entity_type is not None and (not isinstance(entity_type, str) or
                                        entity_type not in arcsvc.ENTITY_TYPES):
            return JSONResponse({"detail": "Invalid entity_type"}, status_code=422)
        try:
            if entity_type is None:
                results = await run_in_threadpool(_rotate_owned, None, dry_run, target_date)
                return {"dry_run": dry_run, "results": results}
            if entity_type not in arcsvc.ENTITY_TYPES:
                return JSONResponse({"detail": "неизвестный entity_type"},
                                    status_code=422)
            res = await run_in_threadpool(_rotate_owned, entity_type, dry_run, target_date)
            return {"dry_run": dry_run, "result": res}
        except Exception as exc:
            log.exception("ARCHIVE: rotate через API упал")
            return JSONResponse({"detail": str(exc)}, status_code=500)
