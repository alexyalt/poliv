"""REST API Этапа 2: CRUD справочников.

Префикс /api. Авторизация — та же сессионная cookie, что и в веб-интерфейсе.
Роли: admin — полный CRUD; operator — просмотр + без удаления; viewer — только чтение.
Ошибки валидации возвращаются как 422 {"detail": "..."} (текст на русском).
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ..infra.config import Config
from ..services.auth_service import SESSION_COOKIE, AuthService
from ..services.catalog_service import CatalogService, ValidationError
from ..services.user_service import UserService

ROLE_LEVELS = {"viewer": 0, "operator": 1, "admin": 2}


def _user(request: Request) -> Optional[dict]:
    auth: AuthService = request.app.state.auth
    token = request.cookies.get(SESSION_COOKIE)
    user = auth.get_user_by_token(token) if token else None
    return dict(user) if user else None


def register_api_routes(
    app: FastAPI, cfg: Config, conn: sqlite3.Connection, catalog: CatalogService, users: UserService
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

    def _actor(request: Request) -> str:
        u = _user(request)
        return u["username"] if u else "?"

    def _log(request: Request, action: str, obj_type: str, obj_id: Any, details: Any = None):
        app.state.auth.write_log(
            None, _actor(request), action, obj_type, obj_id,
            details if isinstance(details, dict) else None,
        )

    async def body(request: Request) -> dict:
        try:
            data = await request.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    import functools
    import inspect

    def api(method: str, path: str, handler, min_role: str = "admin"):
        """Регистрирует маршрут с проверкой сессии/роли поверх обработчика.

        Обёртка сохраняет сигнатуру обработчика (functools.wraps), поэтому
        FastAPI корректно разбирает path-параметры (controller_id, key, ...).
        """

        @functools.wraps(handler)
        async def endpoint(*args, **kwargs):
            request: Request = kwargs["request"]
            denied = _check(request, min_role)
            if denied is not None:
                return denied
            try:
                return await handler(*args, **kwargs)
            except ValidationError as exc:
                return JSONResponse({"detail": str(exc)}, status_code=422)

        # уникальное имя для operationId в OpenAPI (без этого схемы дублируются)
        endpoint.__name__ = f"{handler.__name__}_{method.lower()}"
        assert inspect.signature(endpoint).parameters.keys() == (
            inspect.signature(handler).parameters.keys()
        )
        app.add_api_route(path, endpoint, methods=[method])

    # ------------------------------------------------------------- controllers
    async def list_controllers(request: Request):
        return catalog.list_controllers()

    async def create_controller(request: Request):
        c = catalog.create_controller(await body(request))
        _log(request, "controller.created", "controller", c["id"], {"box_id": c["box_id"]})
        return JSONResponse(c, status_code=201)

    async def get_controller(request: Request, controller_id: int):
        c = catalog.get_controller(controller_id)
        if not c or c["deleted_at"]:
            return JSONResponse({"detail": "Контроллер не найден"}, status_code=404)
        return c

    async def update_controller(request: Request, controller_id: int):
        c = catalog.update_controller(controller_id, await body(request))
        _log(request, "controller.updated", "controller", controller_id)
        return c

    async def delete_controller(request: Request, controller_id: int):
        catalog.delete_controller(controller_id)
        _log(request, "controller.deleted", "controller", controller_id)
        return {"ok": True}

    api("GET", "/api/controllers", list_controllers, "viewer")
    api("POST", "/api/controllers", create_controller, "admin")
    api("GET", "/api/controllers/{controller_id}", get_controller, "viewer")
    api("PUT", "/api/controllers/{controller_id}", update_controller, "admin")
    api("DELETE", "/api/controllers/{controller_id}", delete_controller, "admin")

    # ------------------------------------------------------------------ zones
    async def list_zones(request: Request):
        cid = request.query_params.get("controller_id")
        include_deleted = request.query_params.get("include_deleted") == "1"
        return catalog.list_zones(int(cid) if cid and cid.isdigit() else None, include_deleted)

    async def create_zone(request: Request):
        z = catalog.create_zone(await body(request))
        _log(request, "zone.created", "zone", z["id"], {"number": z["zone_number"]})
        return JSONResponse(z, status_code=201)

    async def get_zone(request: Request, zone_id: int):
        z = catalog.get_zone(zone_id)
        if not z or z["deleted_at"]:
            return JSONResponse({"detail": "Зона не найдена"}, status_code=404)
        return z

    async def update_zone(request: Request, zone_id: int):
        z = catalog.update_zone(zone_id, await body(request))
        _log(request, "zone.updated", "zone", zone_id)
        return z

    async def delete_zone(request: Request, zone_id: int):
        catalog.delete_zone(zone_id)
        _log(request, "zone.disabled", "zone", zone_id)
        return {"ok": True}

    api("GET", "/api/zones", list_zones, "viewer")
    api("POST", "/api/zones", create_zone, "admin")
    api("GET", "/api/zones/{zone_id}", get_zone, "viewer")
    api("PUT", "/api/zones/{zone_id}", update_zone, "admin")
    api("DELETE", "/api/zones/{zone_id}", delete_zone, "admin")

    # --------------------------------------------------------------- programs
    async def list_programs(request: Request):
        return catalog.list_programs()

    async def create_program(request: Request):
        p = catalog.create_program(await body(request))
        _log(request, "program.created", "program", p["id"], {"name": p["name"]})
        return JSONResponse(p, status_code=201)

    async def get_program(request: Request, program_id: int):
        p = catalog.get_program(program_id)
        if not p:
            return JSONResponse({"detail": "Программа не найдена"}, status_code=404)
        return p

    async def update_program(request: Request, program_id: int):
        p = catalog.update_program(program_id, await body(request))
        _log(request, "program.updated", "program", program_id)
        return p

    async def delete_program(request: Request, program_id: int):
        catalog.delete_program(program_id)
        _log(request, "program.deleted", "program", program_id)
        return {"ok": True}

    async def set_program_zones(request: Request, program_id: int):
        data = await body(request)
        # ADR-12: принимаем либо плоский список zone_ids (последовательные),
        # либо zones = [{"zone_id": int, "parallel_group": optional[str]}, ...]
        if "zones" in data:
            spec = data.get("zones")
            field = "zones"
        else:
            spec = data.get("zone_ids")
            field = "zone_ids"
        if not isinstance(spec, list):
            raise ValidationError("Ожидается список zones или zone_ids")
        cleaned: list = []
        for item in spec:
            if isinstance(item, dict):
                cleaned.append(item)
            elif isinstance(item, (int, str)):
                cleaned.append(int(item))
            else:
                raise ValidationError(f"Некорректный элемент списка «{field}»")
        p = catalog.set_program_zones(program_id, cleaned)
        _log(request, "program.zones_set", "program", program_id, {"count": len(p["zones"])})
        return p

    api("GET", "/api/programs", list_programs, "viewer")
    api("POST", "/api/programs", create_program, "admin")
    api("GET", "/api/programs/{program_id}", get_program, "viewer")
    api("PUT", "/api/programs/{program_id}", update_program, "admin")
    api("DELETE", "/api/programs/{program_id}", delete_program, "admin")
    api("PUT", "/api/programs/{program_id}/zones", set_program_zones, "admin")

    # -------------------------------------------------------------- users CRUD
    async def list_users(request: Request):
        return users.list_users()

    async def create_user(request: Request):
        u, generated = users.create_user(await body(request), _actor(request))
        _log(request, "user.created", "user", u["id"], {"username": u["username"]})
        resp = dict(u)
        if generated:
            resp["generated_password"] = generated  # показывается один раз
        return JSONResponse(resp, status_code=201)

    async def update_user(request: Request, user_id: int):
        u = users.update_user(user_id, await body(request), _actor(request))
        _log(request, "user.updated", "user", user_id)
        return u

    async def reset_password(request: Request, user_id: int):
        new_pw = users.reset_password(user_id, _actor(request))
        _log(request, "user.password_reset", "user", user_id)
        return {"ok": True, "generated_password": new_pw}

    async def delete_user(request: Request, user_id: int):
        users.delete_user(user_id, _actor(request))
        _log(request, "user.deleted", "user", user_id)
        return {"ok": True}

    api("GET", "/api/users", list_users, "admin")
    api("POST", "/api/users", create_user, "admin")
    api("PUT", "/api/users/{user_id}", update_user, "admin")
    api("POST", "/api/users/{user_id}/reset_password", reset_password, "admin")
    api("DELETE", "/api/users/{user_id}", delete_user, "admin")

    # --------------------------------------------------------------- settings
    async def list_settings(request: Request):
        return catalog.list_settings()

    async def update_setting(request: Request, key: str):
        data = await body(request)
        catalog.set_setting(key, data.get("value"), _actor(request))
        _log(request, "setting.updated", "setting", key, {"value": data.get("value")})
        return {"ok": True}

    api("GET", "/api/settings", list_settings, "viewer")
    api("PUT", "/api/settings/{key}", update_setting, "admin")
