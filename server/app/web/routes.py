"""Веб-слой: страница входа, дашборд, разделы справочников Этапа 2."""
from __future__ import annotations

import sqlite3

from fastapi import Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles  # noqa: F401 (используется в main)
from fastapi.templating import Jinja2Templates

from ..infra.config import Config
from ..infra.logging import get_logger
from ..services.auth_service import SESSION_COOKIE, AuthService
from ..services.catalog_service import CatalogService, ValidationError
from ..services.user_service import UserService
from .errors import render_error

log = get_logger("poliv.web")

TEMPLATES_DIR = "templates"


def build_templates(app_dir: str) -> Jinja2Templates:
    return Jinja2Templates(directory=app_dir + "/" + TEMPLATES_DIR)


def get_current_user(
    request: Request, templates: Jinja2Templates
):
    """Зависимость: возвращает строку пользователя или редиректит на /login."""
    auth: AuthService = request.app.state.auth
    token = request.cookies.get(SESSION_COOKIE)
    user = auth.get_user_by_token(token) if token else None
    if user is None:
        raise _NotAuthed()
    return user


class _NotAuthed(Exception):
    pass


def register_web_routes(app, cfg: Config, conn: sqlite3.Connection, auth: AuthService):
    templates = build_templates(str(app.state.app_dir))
    catalog: CatalogService = app.state.catalog
    users: UserService = app.state.users

    # Версия приложения доступна во всех шаблонах (заголовок вкладки и логотип).
    templates.env.globals["app_version"] = getattr(app, "version", "0.0.0")

    def ctx(request: Request, **extra):
        # Новый синтаксис Starlette: request передаётся первым аргументом
        # TemplateResponse(request, name, context), поэтому request здесь не нужен.
        return dict(extra)

    def current_user(request: Request):
        token = request.cookies.get(SESSION_COOKIE)
        user = auth.get_user_by_token(token) if token else None
        return dict(user) if user else None

    def page(request: Request, name: str, status_code: int = 200, **extra):
        """Общая подготовка контекста страницы: пользователь + ошибка/flash."""
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        error = request.query_params.get("error")
        flash = request.query_params.get("ok")
        base = ctx(
            request,
            user=user,
            nav=name.split(".")[0] if "." in name else name,
            error=error,
            flash=flash,
            **extra,
        )
        return templates.TemplateResponse(request, name, base, status_code=status_code)

    def form_data(request_form) -> dict:
        return {k: v for k, v in request_form.items()}

    def _form_to_dict(form) -> dict:
        """Form -> dict. Повторяющиеся поля (чекбоксы weekdays/zone_ids)
        собираются в списки через multi_items(); одиночные — последнее значение."""
        d: dict = {}
        for k, v in form.multi_items():
            if k in d:
                cur = d[k]
                if isinstance(cur, list):
                    cur.append(v)
                else:
                    d[k] = [cur, v]
            else:
                d[k] = v
        return d

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request):
        token = request.cookies.get(SESSION_COOKIE)
        if token and auth.get_user_by_token(token):
            return RedirectResponse("/", status_code=302)
        return templates.TemplateResponse(request, "login.html", ctx(request, error=None))

    @app.post("/login")
    def login_submit(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
    ):
        ip = request.client.host if request.client else "?"
        ua = request.headers.get("user-agent", "")
        token, user, message = auth.login(username, password, ip, ua)
        if not token:
            return templates.TemplateResponse(
                request, "login.html", ctx(request, error=message), status_code=401
            )
        resp = RedirectResponse("/", status_code=302)
        resp.set_cookie(
            SESSION_COOKIE,
            token,
            httponly=True,
            samesite="lax",
            max_age=cfg.session_timeout_min * 60,
        )
        return resp

    @app.post("/logout")
    def logout(request: Request):
        token = request.cookies.get(SESSION_COOKIE)
        if token:
            auth.logout(token)
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie(SESSION_COOKIE)
        return resp

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request):
        return page(
            request,
            "dashboard.html",
            controllers=catalog.list_controllers(),
            programs=catalog.list_programs(),
            zones_count=len(catalog.list_zones()),
            logs=auth.recent_logs(20),
            stage="Этап 2. Базовые справочники и CRUD",
        )

    # ============================== Этап 2: контроллеры ====================
    def redirect(url: str, error: str = "", ok: str = "") -> RedirectResponse:
        from urllib.parse import quote

        if error:
            url += ("&" if "?" in url else "?") + "error=" + quote(error)
        if ok:
            url += ("&" if "?" in url else "?") + "ok=" + quote(ok)
        return RedirectResponse(url, status_code=302)

    def require_admin(user: dict):
        if user["role"] != "admin":
            return "Действие доступно только администратору"
        return None

    @app.get("/controllers")
    def controllers_page(request: Request):
        all_controllers = catalog.list_controllers(include_deleted=True)
        edit_id = request.query_params.get("edit")
        editing = None
        if edit_id and edit_id.isdigit():
            editing = next((c for c in all_controllers if c["id"] == int(edit_id)), None)
        return page(
            request,
            "controllers.html",
            controllers=all_controllers,
            editing=editing,
        )

    @app.post("/controllers/save")
    async def controllers_save(request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/controllers", error=err)
        form = await request.form()
        data = _form_to_dict(form)
        cid = data.pop("id", None)
        try:
            if cid and cid.isdigit():
                catalog.update_controller(int(cid), data)
                auth.write_log(user["id"], user["username"], "controller.updated",
                               "controller", cid)
            else:
                c = catalog.create_controller(data)
                auth.write_log(user["id"], user["username"], "controller.created",
                               "controller", c["id"], {"box_id": c["box_id"]})
        except ValidationError as exc:
            return redirect("/controllers", error=str(exc))
        return redirect("/controllers", ok="Контроллер сохранён")

    @app.post("/controllers/{controller_id}/delete")
    def controllers_delete(controller_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/controllers", error=err)
        try:
            catalog.delete_controller(controller_id)
            auth.write_log(user["id"], user["username"], "controller.deleted",
                           "controller", controller_id)
        except ValidationError as exc:
            return redirect("/controllers", error=str(exc))
        return redirect("/controllers", ok="Контроллер отключён (soft-delete)")

    @app.post("/controllers/{controller_id}/enable")
    def controllers_enable(controller_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/controllers", error=err)
        try:
            catalog.enable_controller(controller_id)
            auth.write_log(user["id"], user["username"], "controller.enabled",
                           "controller", controller_id)
        except ValidationError as exc:
            return redirect("/controllers", error=str(exc))
        return redirect("/controllers", ok="Контроллер включён")

    # ====================================== Этап 2: зоны ===================
    @app.get("/zones")
    def zones_page(request: Request):
        include_deleted = request.query_params.get("deleted") == "1"
        all_zones = catalog.list_zones(include_deleted=include_deleted)
        edit_id = request.query_params.get("edit")
        editing = None
        if edit_id and edit_id.isdigit():
            editing = next((z for z in all_zones if z["id"] == int(edit_id)), None)
        return page(
            request,
            "zones.html",
            zones=all_zones,
            controllers=catalog.list_controllers(),
            show_deleted=include_deleted,
            editing=editing,
            q=request.query_params.get("q", ""),
        )

    @app.post("/zones/save")
    async def zones_save(request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/zones", error=err)
        form = await request.form()
        data = _form_to_dict(form)
        # Чекбокс не отмечен -> поле не отправляется: считаем False (правка 5.6)
        data["enabled"] = form.get("enabled") == "on"
        data["cycle_soak_enabled"] = form.get("cycle_soak_enabled") == "on"
        zid = data.pop("id", None)
        try:
            if zid and zid.isdigit():
                catalog.update_zone(int(zid), data)
                auth.write_log(user["id"], user["username"], "zone.updated", "zone", zid)
            else:
                z = catalog.create_zone(data)
                auth.write_log(user["id"], user["username"], "zone.created", "zone",
                               z["id"], {"number": z["zone_number"]})
        except ValidationError as exc:
            return redirect("/zones", error=str(exc))
        return redirect("/zones", ok="Зона сохранена")

    @app.post("/zones/{zone_id}/delete")
    def zones_delete(zone_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/zones", error=err)
        try:
            catalog.delete_zone(zone_id)
            auth.write_log(user["id"], user["username"], "zone.disabled", "zone", zone_id)
        except ValidationError as exc:
            return redirect("/zones", error=str(exc))
        return redirect("/zones", ok="Зона отключена (зоны не удаляются — см. ТЗ)")

    @app.post("/zones/{zone_id}/enable")
    def zones_enable(zone_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/zones", error=err)
        try:
            catalog.enable_zone(zone_id)
            auth.write_log(user["id"], user["username"], "zone.enabled", "zone", zone_id)
        except ValidationError as exc:
            return redirect("/zones", error=str(exc))
        return redirect("/zones", ok="Зона включена")

    # ================================== Этап 2: программы ==================
    @app.get("/programs")
    def programs_page(request: Request):
        all_programs = catalog.list_programs()
        edit_id = request.query_params.get("edit")
        editing = None
        if edit_id and edit_id.isdigit():
            editing = next((p for p in all_programs if p["id"] == int(edit_id)), None)
        return page(
            request,
            "programs.html",
            programs=all_programs,
            editing=editing,
        )

    @app.get("/programs/{program_id}")
    def program_page(program_id: int, request: Request):
        prog = catalog.get_program(program_id)
        if not prog:
            return render_error(templates, request, 404)
        all_zones = catalog.list_zones()
        return page(
            request,
            "program_detail.html",
            prog=prog,
            editing_zones=bool(request.query_params.get("edit_zones")),
            all_zones=all_zones,
            selected=[z["zone_id"] for z in prog["zones"]],
            group_of={z["zone_id"]: (z.get("parallel_group") or "")
                      for z in prog["zones"]},
        )

    @app.post("/programs/save")
    async def programs_save(request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/programs", error=err)
        form = await request.form()
        data = _form_to_dict(form)
        data["enabled"] = form.get("enabled") == "on"
        pid = data.pop("id", None)
        try:
            if pid and pid.isdigit():
                catalog.update_program(int(pid), data)
                auth.write_log(user["id"], user["username"], "program.updated",
                               "program", pid)
            else:
                p = catalog.create_program(data)
                auth.write_log(user["id"], user["username"], "program.created",
                               "program", p["id"], {"name": p["name"]})
        except ValidationError as exc:
            return redirect("/programs", error=str(exc))
        return redirect("/programs", ok="Программа сохранена")

    @app.post("/programs/{program_id}/delete")
    def programs_delete(program_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/programs", error=err)
        try:
            catalog.delete_program(program_id)
            auth.write_log(user["id"], user["username"], "program.deleted",
                           "program", program_id)
        except ValidationError as exc:
            return redirect("/programs", error=str(exc))
        return redirect("/programs", ok="Программа удалена")

    @app.post("/programs/{program_id}/zones")
    async def programs_zones(program_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect(f"/programs/{program_id}", error=err)
        # Примечание: getlist() в Starlette MultiDictProxy возвращает только
        # ПЕРВОЕ значение ключа (наследие Django), поэтому повторяющиеся поля
        # формы читаем через multi_items().
        form = await request.form()
        raw = [v for k, v in form.multi_items() if k == "zone_ids"]
        groups = {
            str(v): (g.strip()[:30] if (g := form.get(f"pg_{v}")) else "")
            for v in raw
        }
        spec = [
            {"zone_id": int(r), "parallel_group": groups.get(str(r), "")}
            for r in raw if r.isdigit()
        ]
        # Правка 4.8: UI передаёт явный порядок выполнения (seq) и группы в JSON
        order_json = form.get("order_json")
        if order_json:
            import json as _json
            try:
                parsed = _json.loads(order_json)
                if isinstance(parsed, list) and parsed:
                    spec = [
                        {"zone_id": int(e["zone_id"]),
                         "parallel_group": (e.get("parallel_group") or "")}
                        for e in parsed if str(e.get("zone_id", "")).isdigit()
                    ]
            except (ValueError, TypeError, KeyError):
                pass  # некорректный JSON — остаёмся на последовательном разборе формы
        try:
            catalog.set_program_zones(program_id, spec)
            auth.write_log(user["id"], user["username"], "program.zones_set",
                           "program", program_id, {"zone_ids": raw})
        except ValidationError as exc:
            return redirect(f"/programs/{program_id}", error=str(exc))
        return redirect(f"/programs/{program_id}", ok="Состав зон программы обновлён")

    # =============================== Этап 2: пользователи ==================
    @app.get("/users")
    def users_page(request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        if user["role"] != "admin":
            return redirect("/", error="Раздел доступен только администратору")
        user_list = users.list_users()
        edit_id = request.query_params.get("edit")
        editing_user = None
        if edit_id and edit_id.isdigit():
            editing_user = next((u for u in user_list if u["id"] == int(edit_id)), None)
        return page(request, "users.html", user_list=user_list, editing_user=editing_user)

    @app.post("/users/save")
    async def users_save(request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/users", error=err)
        form = await request.form()
        data = _form_to_dict(form)
        data["enabled"] = form.get("enabled") == "on"  # правка 5.6
        uid = data.pop("id", None)
        try:
            if uid and uid.isdigit():
                # Правка 5.2: свой пароль при редактировании (пусто = без смены)
                pw = (data.pop("password", "") or "").strip()
                if pw:
                    data["password"] = pw
                users.update_user(int(uid), data, user["username"])
            else:
                _, generated = users.create_user(data, user["username"])
                if generated:
                    return redirect(
                        "/users",
                        ok=f"Пользователь создан, пароль: {generated} (сохраните, показывается один раз)",
                    )
        except ValidationError as exc:
            return redirect("/users", error=str(exc))
        return redirect("/users", ok="Пользователь сохранён")

    @app.post("/users/{user_id}/reset_password")
    def users_reset(user_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/users", error=err)
        try:
            new_pw = users.reset_password(user_id, user["username"])
        except ValidationError as exc:
            return redirect("/users", error=str(exc))
        return redirect("/users", ok=f"Новый пароль: {new_pw} (показывается один раз)")

    @app.post("/users/{user_id}/delete")
    def users_delete(user_id: int, request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/users", error=err)
        try:
            users.delete_user(user_id, user["username"])
        except ValidationError as exc:
            return redirect("/users", error=str(exc))
        return redirect("/users", ok="Пользователь удалён")

    # ================================ Этап 2: настройки =====================
    @app.get("/settings")
    def settings_page(request: Request):
        return page(request, "settings.html", settings=catalog.list_settings())

    @app.post("/settings/save")
    async def settings_save(request: Request):
        user = current_user(request)
        if user is None:
            return RedirectResponse("/login", status_code=302)
        err = require_admin(user)
        if err:
            return redirect("/settings", error=err)
        form = await request.form()
        try:
            for key, value in form.items():
                if key.startswith("setting:"):
                    catalog.set_setting(key[len("setting:"):], value, user["username"])
        except ValidationError as exc:
            return redirect("/settings", error=str(exc))
        auth.write_log(user["id"], user["username"], "settings.updated", "setting", None)
        return redirect("/settings", ok="Настройки сохранены")

    @app.exception_handler(_NotAuthed)
    async def _not_authed_handler(request: Request, exc: _NotAuthed):
        return RedirectResponse("/login", status_code=302)

    @app.exception_handler(404)
    async def not_found(request: Request, exc):
        return render_error(templates, request, 404)

    @app.exception_handler(500)
    async def server_error(request: Request, exc):
        # hotfix stage2 (блок 2): полный трейсбек пишем в logs/error.log ДО
        # отрисовки страницы — иначе 500 «немые» и причину найти невозможно.
        log.exception(
            "500 Internal Server Error: %s %s",
            request.method,
            request.url.path,
            exc_info=exc,
        )
        return render_error(templates, request, 500)

    return templates
