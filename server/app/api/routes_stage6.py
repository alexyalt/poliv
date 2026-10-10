"""REST API Этапа 6: события, расход, уведомления, аварийные блокировки.

Эндпоинты (контракт Артефакта 0.6, адаптирован под проект):
- GET  /api/events                      — список событий (фильтры, пагинация);
- GET  /api/events/export.csv           — выгрузка CSV (до 10000 строк);
- GET  /api/flow/summary                — сводка расхода за период;
- GET  /api/flow/daily                  — дневные агрегаты;
- GET  /api/notifications               — уведомления (+unread_count);
- GET  /api/notifications/unread-count  — счётчик для шапки ( polling );
- POST /api/notifications/{id}/read     — отметить прочитанным;
- POST /api/notifications/read-all      — прочитать все;
- POST /api/controllers/{id}/clear-error — сброс аварийной блокировки (operator+).

Роли/ошибки — как в routes_stage3/4/5: viewer на чтение, operator на действия;
401/403/404 в {"detail": ...}. Команды clear_error в словаре COMMAND_NAMES
прошивки нет (Этап 3) — серверный сброс выполняется локально; если MQTT
подключён и контроллер онлайн, дополнительно шлётся ping как «разбудитель»
статуса (отсутствие dedicated-команды — осознанное отличие от рекомендации).
"""
from __future__ import annotations

import csv
import io
import sqlite3
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from ..infra.logging import get_logger
from ..services.flow_service import get_flow_service, get_setting
from ..services.notification_service import get_notification_service
from .routes_stage3 import _user

log = get_logger("poliv.api6")

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


def register_api_routes_stage6(app: FastAPI, cfg, conn: sqlite3.Connection) -> None:
    api = app.state.stage6_api_helper if hasattr(app.state, "stage6_api_helper") \
        else _plain_api(app)

    # ------------------------------------------------------------------ события
    @api("GET", "/api/events")
    async def events_list(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied

        page = _int_param(request, "page", 1, 1, 100000)
        page_size = _int_param(request, "page_size", 50, 1, 200)

        sql = "SELECT * FROM watering_events WHERE 1=1"
        params: list[Any] = []
        controller_id = request.query_params.get("controller_id")
        status = request.query_params.get("status")
        source = request.query_params.get("source")
        date_from = request.query_params.get("from")
        date_to = request.query_params.get("to")
        if controller_id:
            sql += " AND controller_id = ?"
            params.append(int(controller_id))
        if status:
            sql += " AND status = ?"
            params.append(status)
        if source:
            sql += " AND source = ?"
            params.append(source)
        try:
            if date_from:
                sql += " AND start_ts >= ?"
                params.append(int(date_from))
            if date_to:
                sql += " AND start_ts <= ?"
                params.append(int(date_to))
        except ValueError:
            return JSONResponse({"detail": "from/to — unix-секунды (целые)"},
                                status_code=422)

        total = conn.execute(
            sql.replace("SELECT *", "SELECT COUNT(*) c"), params
        ).fetchone()["c"]
        sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
        items = [dict(r) for r in conn.execute(
            sql, (*params, page_size, (page - 1) * page_size)).fetchall()]
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total_items": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
        }

    @api("GET", "/api/events/export.csv")
    async def events_csv(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied

        rows = conn.execute(
            """SELECT e.*, c.name AS controller_name, c.box_id
               FROM watering_events e JOIN controllers c ON c.id = e.controller_id
               ORDER BY e.id DESC LIMIT 10000"""
        ).fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        if rows:
            writer.writerow(rows[0].keys())
            for r in rows:
                writer.writerow(list(r))
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=events.csv"})

    # ------------------------------------------------------------------ расход
    @api("GET", "/api/flow/summary")
    async def flow_summary(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        svc = get_flow_service()
        if svc is None:
            return JSONResponse({"detail": "Сервис расхода недоступен"}, 503)
        return svc.summary(date_from=request.query_params.get("from"),
                           date_to=request.query_params.get("to"))

    @api("GET", "/api/flow/daily")
    async def flow_daily(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        sql = ("SELECT f.*, c.name AS controller_name, z.name AS zone_name "
               "FROM flow_daily f "
               "JOIN controllers c ON c.id = f.controller_id "
               # схема 0008: zone_id = -1 означает агрегат по контроллеру
               "LEFT JOIN zones z ON z.id = f.zone_id AND f.zone_id != -1 "
               "WHERE 1=1")
        params: list[Any] = []
        controller_id = request.query_params.get("controller_id")
        date_from = request.query_params.get("from")
        date_to = request.query_params.get("to")
        if controller_id:
            sql += " AND f.controller_id = ?"
            params.append(int(controller_id))
        if date_from:
            sql += " AND f.date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND f.date <= ?"
            params.append(date_to)
        sql += " ORDER BY f.date DESC, f.id DESC LIMIT 365"
        return {"items": [dict(r) for r in conn.execute(sql, params).fetchall()]}

    @api("GET", "/api/flow/settings")
    async def flow_settings(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        svc = get_flow_service()
        if svc is None:
            return JSONResponse({"detail": "Сервис расхода недоступен"}, 503)
        th = svc.thresholds()
        th["sound_enabled"] = bool(
            get_setting(conn, "ui.sound_notifications_enabled", True))
        return th

    # ------------------------------------------------------------------ уведомления
    @api("GET", "/api/notifications")
    async def notifications_list(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        svc = get_notification_service()
        if svc is None:
            return JSONResponse({"detail": "Сервис уведомлений недоступен"}, 503)
        unread = request.query_params.get("unread") == "1"
        severity = request.query_params.get("severity")
        limit = _int_param(request, "limit", 50, 1, 500)
        items = svc.list(unread_only=unread, severity=severity, limit=limit)
        return {"items": items, "unread_count": svc.unread_count(),
                "max_unread_critical_id": svc.max_unread_critical_id()}

    @api("GET", "/api/notifications/unread-count")
    async def notifications_unread(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        svc = get_notification_service()
        return {"unread_count": svc.unread_count() if svc else 0,
                "max_unread_critical_id": svc.max_unread_critical_id() if svc else 0,
                "sound_enabled": bool(
                    get_setting(conn, "ui.sound_notifications_enabled", True))}

    @api("POST", "/api/notifications/{notif_id}/read")
    async def notifications_read(request: Request, notif_id: int):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        svc = get_notification_service()
        if svc is None:
            return JSONResponse({"detail": "Сервис уведомлений недоступен"}, 503)
        return {"status": "ok" if svc.mark_read(notif_id) else "not_found"}

    @api("POST", "/api/notifications/read-all")
    async def notifications_read_all(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        svc = get_notification_service()
        if svc is None:
            return JSONResponse({"detail": "Сервис уведомлений недоступен"}, 503)
        return {"status": "ok", "marked": svc.mark_all_read()}

    # ------------------------------------------------------------------ сброс аварии
    @api("POST", "/api/controllers/{controller_id}/clear-error")
    async def clear_error(request: Request, controller_id: int):
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        user = _user(request)

        row = conn.execute(
            "SELECT id, box_id, emergency_lock_until_ts FROM controllers WHERE id=?",
            (controller_id,)).fetchone()
        if row is None:
            return JSONResponse({"detail": "Контроллер не найден"}, status_code=404)

        body: dict = {}
        try:
            raw = await request.body()
            if raw:
                import json as _json
                parsed = _json.loads(raw)
                if isinstance(parsed, dict):
                    body = parsed
        except Exception:
            body = {}
        reason = str(body.get("reason") or "ручной сброс аварии")[:200]

        svc = get_flow_service()
        cleared = False
        if svc is not None:
            cleared = svc.clear_error(controller_id)
        else:
            with conn:
                cur = conn.execute(
                    """UPDATE controllers SET emergency_lock_until_ts=NULL,
                           current_mode='idle', current_phase='idle',
                           updated_at=datetime('now')
                       WHERE id=? AND emergency_lock_until_ts IS NOT NULL""",
                    (controller_id,))
            cleared = cur.rowcount > 0

        # Уведомления об этой аварии помечаем разрешёнными/прочитанными.
        nsvc = get_notification_service()
        if nsvc is not None:
            with conn:
                conn.execute(
                    """UPDATE notifications
                       SET read_at=COALESCE(read_at, datetime('now')),
                           resolved_at=datetime('now')
                       WHERE controller_id=? AND type='flow_emergency'
                         AND resolved_at IS NULL""", (controller_id,))

        #MQTT: если клиент подключён — пингуем контроллер, чтобы он прислал
        # актуальный статус (специальной команды clear_error в прошивке нет).
        mqtt = getattr(app.state, "mqtt", None)
        if mqtt is not None and getattr(mqtt, "connected", False):
            try:
                mqtt.publish(f"poliv/{row['box_id']}/command", {
                    "protocol_version": "1.0", "ts": __import__("time").time(),
                    "command_id": "stage6-status-refresh",
                    "command": "ping", "params": {},
                }, qos=1)
            except Exception:
                log.exception("clear-error: не удалось отправить ping %s",
                              row["box_id"])

        app.state.auth.write_log(
            user["id"], user["username"], "controller.clear_error",
            "controller", controller_id, {"reason": reason,
                                          "cleared": cleared})
        return {"status": "ok", "controller_id": controller_id,
                "cleared": cleared}

    log.info("API Этап 6 зарегистрирован")


def _plain_api(app: FastAPI):
    """Регистратор маршрутов напрямую (без обёртки auth из stage5).

    Используется как api("GET", path) в виде декоратора над handler —
    двухуровневая фабрика (в исходной рекомендации был нерабочий вариант
    api(method, path, handler), несовместимый с синтаксисом декоратора).
    """
    def api(method: str, path: str):
        def decorator(handler):
            app.add_api_route(path, handler, methods=[method])
            return handler
        return decorator
    return api
