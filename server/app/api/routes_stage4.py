"""REST API Этапа 4: машинограммы (компиляция, просмотр, отправка).

Контракт — ТЗ Этап 4, п.3 + Артефакт 0.6 §4 (расширение):
- POST /api/controllers/{id}/schedules/compile   — компилировать и сохранить
  новую версию (operator+); тело опционально: {"send": true} — сразу отправить;
- GET  /api/controllers/{id}/schedules           — список версий (viewer+);
- GET  /api/controllers/{id}/schedules/preview   — компиляция без сохранения
  (оператор смотрит, что получится; версия не расходуется);
- GET  /api/controllers/{id}/schedules/current   — последняя сохранённая версия
  с полным payload машинограммы;
- GET  /api/schedules/{schedule_id}              — конкретная машинограмма;
- POST /api/schedules/{schedule_id}/send         — опубликовать в MQTT
  (учитывает apply_policy next_run — может вернуть deferred=true);
- GET  /api/controllers/{id}/watering-runs       — журнал прогонов (план/факт).

Ошибки: 401/403 роли; 404 отсутствие записи; 422 {detail, code:"schedule_conflict",
details:[...]} конфликт пересечения запусков (ScheduleConflict); 503 MQTT недоступен.
Формат ошибок и ролей — как routes_stage3 (_check/_user), чтобы фронтенд
Этапа 3 не менялся.
"""
from __future__ import annotations

import functools
import inspect
import json
import sqlite3
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ..core.schedule_compiler.compiler import ScheduleConflict
from ..services.auth_service import SESSION_COOKIE, AuthService
from ..services.schedule_service import ScheduleService

ROLE_LEVELS = {"viewer": 0, "operator": 1, "admin": 2}


def _user(request: Request) -> Optional[dict]:
    auth: AuthService = request.app.state.auth
    token = request.cookies.get(SESSION_COOKIE)
    user = auth.get_user_by_token(token) if token else None
    return dict(user) if user else None


def register_api_routes_stage4(
    app: FastAPI, cfg, conn: sqlite3.Connection, sched: ScheduleService
):
    def _check(request: Request, min_role: str = "viewer"):
        user = _user(request)
        if user is None:
            return JSONResponse({"detail": "Требуется вход в систему"}, status_code=401)
        if ROLE_LEVELS[user["role"]] < ROLE_LEVELS[min_role]:
            return JSONResponse(
                {"detail": f"Недостаточно прав: требуется роль {min_role}"},
                status_code=403)
        return None

    async def body(request: Request) -> dict:
        raw = await request.body()
        if not raw.strip():
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError("Некорректный JSON тела запроса")
        if not isinstance(data, dict):
            raise ValueError("Тело запроса должно быть JSON-объектом")
        return data

    # -------------------------------------------------------------- handlers
    async def compile_schedule(request: Request, controller_id: int):
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        user = _user(request)
        data = await body(request)
        send = bool(data.get("send", False))
        days_ahead = data.get("days_ahead")
        result = sched.compile_and_store(
            controller_id, source="manual", created_by=user["username"],
            days_ahead=int(days_ahead) if days_ahead else None)
        if send:
            sent = sched.send_schedule(result["id"])
            result["sent"] = sent.get("sent", False)
            result["deferred"] = sent.get("deferred", False)
            if sent.get("message"):
                result["message"] = sent["message"]
        return JSONResponse(result, status_code=201)

    async def preview_schedule(request: Request, controller_id: int):
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        return sched.preview(controller_id)

    async def list_schedules(request: Request, controller_id: int):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        ctrl = conn.execute(
            "SELECT id FROM controllers WHERE id=? AND deleted_at IS NULL",
            (controller_id,)).fetchone()
        if ctrl is None:
            return JSONResponse({"detail": "Контроллер не найден"}, status_code=404)
        limit = int(request.query_params.get("limit", 20) or 20)
        return sched.list_for_controller(controller_id, limit=max(1, min(limit, 200)))

    async def current_schedule(request: Request, controller_id: int):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        view = sched.current_for_controller(controller_id)
        if view is None:
            return JSONResponse({"detail": "Машинограмм ещё нет — выполните "
                                           "компиляцию"}, status_code=404)
        return view

    async def get_schedule(request: Request, schedule_id: int):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        view = sched.get(schedule_id)
        if view is None:
            return JSONResponse({"detail": "Машинограмма не найдена"}, status_code=404)
        return view

    async def send_schedule(request: Request, schedule_id: int):
        denied = _check(request, "operator")
        if denied is not None:
            return denied
        result = sched.send_schedule(schedule_id)
        if result.get("deferred"):
            return JSONResponse(result, status_code=202)
        return result

    async def watering_runs(request: Request, controller_id: int):
        denied = _check(request, "viewer")
        if denied is not None:
            return denied
        rows = conn.execute(
            """SELECT wr.*, p.name AS program_name FROM watering_runs wr
               LEFT JOIN programs p ON p.id = wr.program_id
               WHERE wr.controller_id=?
               ORDER BY wr.planned_start_ts DESC, wr.id DESC LIMIT 100""",
            (controller_id,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            for key in ("zones_json", "details_json"):
                v = d.get(key)
                if v:
                    try:
                        d[key.replace("_json", "")] = json.loads(v)
                    except (TypeError, ValueError):
                        d[key.replace("_json", "")] = None
                d.pop(key, None)
            out.append(d)
        return out

    # ------------------------------------------------------------ registration
    def api(method: str, path: str, handler):
        @functools.wraps(handler)
        async def endpoint(*args, **kwargs):
            try:
                return await handler(*args, **kwargs)
            except ScheduleConflict as exc:
                return JSONResponse(
                    {"detail": str(exc), "code": "schedule_conflict",
                     "conflicts": exc.details}, status_code=422)
            except ValueError as exc:
                msg = str(exc)
                if msg == "Контроллер не найден":
                    return JSONResponse({"detail": msg}, status_code=404)
                if msg == "Машинограмма не найдена":
                    return JSONResponse({"detail": msg}, status_code=404)
                return JSONResponse({"detail": msg}, status_code=422)
            except RuntimeError as exc:      # MQTT недоступен
                return JSONResponse({"detail": str(exc)}, status_code=503)

        endpoint.__name__ = f"{handler.__name__}_{method.lower()}"
        assert inspect.signature(endpoint).parameters.keys() == \
            inspect.signature(handler).parameters.keys()
        app.add_api_route(path, endpoint, methods=[method])

    # ВАЖНО про порядок: статические сегменты (preview/current) раньше {schedule_id}?
    # Они на разных префиксах (/api/controllers/... vs /api/schedules/...) —
    # конфликта нет. Внутри /api/controllers/{id}/schedules/* «preview» и
    # «current» — литералы, отдельного перехвата int-параметром нет.
    api("POST", "/api/controllers/{controller_id}/schedules/compile", compile_schedule)
    api("GET", "/api/controllers/{controller_id}/schedules/preview", preview_schedule)
    api("GET", "/api/controllers/{controller_id}/schedules", list_schedules)
    api("GET", "/api/controllers/{controller_id}/schedules/current", current_schedule)
    api("GET", "/api/controllers/{controller_id}/watering-runs", watering_runs)
    api("GET", "/api/schedules/{schedule_id}", get_schedule)
    api("POST", "/api/schedules/{schedule_id}/send", send_schedule)
