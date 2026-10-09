"""REST API Этапа 3: команды контроллерам и живое состояние (префикс /api).

Контракт — Артефакт 0.6 §4.5 (живое состояние) и задание Этап 3, Блок 5:
- POST /api/controllers/{id}/actions/{action} — команда контроллеру;
  action ∈ ping, zone_open, zone_close, stop_all, pause_controller,
  resume_controller, reboot (reboot — только admin);
  ответ: 202 {"status":"accepted","command_id":...} при финальном ack
  completed/ignored_duplicate; 409 controller_offline для офлайн-контроллера;
  422 — строгая валидация параметров; 503 mqtt_unavailable — брокер не подключён.
- GET  /api/controllers/{id}/live — view-модель живого состояния (минимальный
  состав полей миграции 0004);
- GET  /api/controllers/live — сводка по всем контроллерам для плиток дашборда
  (опрос фронтендом каждые 10 секунд).

Ошибки — в формате {"detail": ...}, как в routes_stage2.py.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ..infra.config import Config
from ..services.auth_service import SESSION_COOKIE, AuthService
from ..services.mqtt_command_service import (
    COMMAND_NAMES,
    ControllerOffline,
    MqttUnavailable,
    MqttCommandService,
    CommandError,
)

ROLE_LEVELS = {"viewer": 0, "operator": 1, "admin": 2}

# Действия Этапа 3 и минимально необходимые роли (reboot — только admin).
ACTION_ROLES: dict[str, str] = {name: "operator" for name in COMMAND_NAMES}
ACTION_ROLES["reboot"] = "admin"


def _user(request: Request) -> Optional[dict]:
    auth: AuthService = request.app.state.auth
    token = request.cookies.get(SESSION_COOKIE)
    user = auth.get_user_by_token(token) if token else None
    return dict(user) if user else None


def _json_list(value: Any) -> list:
    """active_zones_json/display_zones_json из БД -> список (пусто если NULL)."""
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def live_view(row: sqlite3.Row) -> dict:
    """View-модель живого состояния контроллера (Артефакт 0.6 §4.5, минимум Этапа 3).

    Поля — результат миграции 0004_controller_live_state; «последние события»
    и «предупреждения» появятся на Этапе 6 (таблицы событий ещё нет).
    """
    d = dict(row)
    return {
        "id": d["id"],
        "box_id": d["box_id"],
        "name": d["name"],
        "connection_status": d.get("connection_status") or "offline",
        "last_seen_at": d.get("last_seen_at"),
        "last_hello_at": d.get("last_hello_at"),
        "firmware_version": d.get("firmware_version"),
        "schedule_version": d.get("schedule_version") or 0,
        "schedule_hash": d.get("schedule_hash"),
        "time_valid": bool(d.get("time_valid")),
        "mode": d.get("current_mode"),
        "phase": d.get("current_phase"),
        "primary_zone": d.get("primary_zone"),
        "active_zones": _json_list(d.get("active_zones_json")),
        "display_zones": _json_list(d.get("display_zones_json")),
        "phase_end_ts": d.get("phase_end_ts"),
        "run_end_ts": d.get("run_end_ts"),
        "pause_until_ts": d.get("pause_until_ts"),
        "emergency_lock_until_ts": d.get("emergency_lock_until_ts"),
        "flow_enabled": bool(d.get("flow_enabled")),
        "flow_total_liters": d.get("flow_total_liters"),
        "instant_lpm": d.get("instant_lpm"),
        "service_mode_active": bool(d.get("service_mode_active")),
        "ip_address": d.get("ip_address"),
    }


LIVE_SELECT = """
    SELECT id, box_id, name, connection_status, last_seen_at, last_hello_at,
           firmware_version, schedule_version, schedule_hash, time_valid,
           current_mode, current_phase, primary_zone, active_zones_json,
           display_zones_json, phase_end_ts, run_end_ts, pause_until_ts,
           emergency_lock_until_ts, flow_enabled, flow_total_liters,
           instant_lpm, service_mode_active, ip_address
    FROM controllers
"""


def register_api_routes_stage3(
    app: FastAPI, cfg: Config, conn: sqlite3.Connection, cmd: MqttCommandService
):
    def _check(request: Request, min_role: str = "viewer"):
        user = _user(request)
        if user is None:
            return JSONResponse({"detail": "Требуется вход в систему"}, status_code=401)
        if ROLE_LEVELS[user["role"]] < ROLE_LEVELS[min_role]:
            return JSONResponse(
                {"detail": f"Недостаточно прав: требуется роль {min_role}"}, status_code=403
            )
        return None

    async def body(request: Request) -> dict:
        """Тело запроса: {} допустимо (ping), битый JSON -> 400 (как в stage2)."""
        raw = await request.body()
        if not raw.strip():
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise CommandError("Некорректный JSON тела запроса")
        if not isinstance(data, dict):
            raise CommandError("Тело запроса должно быть JSON-объектом")
        return data

    # ------------------------------------------------- POST actions/{action}
    async def controller_action(request: Request, controller_id: int, action: str):
        user = _user(request)
        denied = _check(request, ACTION_ROLES.get(action, "operator"))
        if denied is not None:
            return denied
        if action not in COMMAND_NAMES:
            return JSONResponse(
                {"detail": f"Неизвестное действие: {action}. "
                           f"Допустимы: {', '.join(sorted(COMMAND_NAMES))}"},
                status_code=422,
            )
        data = await body(request)
        params = data.get("params", {})
        source = data.get("source", "web")
        if source not in ("web", "api", "service", "system"):
            raise CommandError("source: допустимы web, api, service, system")
        result = cmd.send_command_for_controller(
            controller_id, action, params, source=source,
            user_id=user["id"], username=user["username"],
        )
        status = result["status"]
        # Финальный ack completed/ignored_duplicate -> 202 accepted (ТЗ Этапа 3).
        # rejected/error -> 502 (команда доставлена, контроллер отказал/ошибка),
        # timeout -> 504 (все повторы с тем же command_id исчерпаны).
        if status in ("completed", "ignored_duplicate"):
            code, api_status = 202, "accepted"
        elif status == "rejected":
            code, api_status = 502, "rejected"
        elif status == "error":
            code, api_status = 502, "error"
        else:
            code, api_status = 504, "timeout"
        return JSONResponse(
            {
                "status": api_status,
                "command_id": result["command_id"],
                "ack_status": status,
                "message": result.get("message"),
                "attempts": result.get("attempts"),
            },
            status_code=code,
        )

    # ------------------------------------------------- GET live (один/все)
    async def controller_live(request: Request, controller_id: int):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        row = conn.execute(LIVE_SELECT + " WHERE id=? AND deleted_at IS NULL",
                           (controller_id,)).fetchone()
        if row is None:
            return JSONResponse({"detail": "Контроллер не найден"}, status_code=404)
        return live_view(row)

    async def controllers_live_all(request: Request):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        rows = conn.execute(LIVE_SELECT + " WHERE deleted_at IS NULL ORDER BY name").fetchall()
        return [live_view(r) for r in rows]

    import functools
    import inspect

    def api(method: str, path: str, handler, wraps_check=True):
        """Регистрация маршрута; конвертация доменных ошибок в HTTP-ответы.

        Обёртка сохраняет сигнатуру (functools.wraps) — FastAPI корректно
        разбирает path-параметры (controller_id, action), как в routes_stage2.
        Проверка роли выполняется внутри обработчика (_check), чтобы 401/403
        возвращались до обращения к MQTT.
        """

        @functools.wraps(handler)
        async def endpoint(*args, **kwargs):
            try:
                return await handler(*args, **kwargs)
            except CommandError as exc:
                return JSONResponse({"detail": str(exc)}, status_code=422)
            except ControllerOffline:
                return JSONResponse({"detail": "controller_offline"}, status_code=409)
            except MqttUnavailable as exc:
                return JSONResponse({"detail": str(exc)}, status_code=503)

        endpoint.__name__ = f"{handler.__name__}_{method.lower()}"
        assert inspect.signature(endpoint).parameters.keys() == (
            inspect.signature(handler).parameters.keys()
        )
        app.add_api_route(path, endpoint, methods=[method])

    # ВАЖНО: /api/controllers/live регистрируется ДО /api/controllers/{id}/... —
    # иначе "live" воспринимался бы как controller_id (422 на разбор int).
    api("GET", "/api/controllers/live", controllers_live_all)
    api("GET", "/api/controllers/{controller_id}/live", controller_live)
    api("POST", "/api/controllers/{controller_id}/actions/{action}", controller_action)
