"""Веб-слой Этапа 1: страница входа, базовый шаблон, пустой дашборд."""
from __future__ import annotations

import sqlite3

from fastapi import Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles  # noqa: F401 (используется в main)
from fastapi.templating import Jinja2Templates

from ..infra.config import Config
from ..services.auth_service import SESSION_COOKIE, AuthService
from .errors import render_error

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

    def ctx(request: Request, **extra):
        base = {"request": request}
        base.update(extra)
        return base

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request):
        token = request.cookies.get(SESSION_COOKIE)
        if token and auth.get_user_by_token(token):
            return RedirectResponse("/", status_code=302)
        return templates.TemplateResponse("login.html", ctx(request, error=None))

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
                "login.html", ctx(request, error=message), status_code=401
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
        token = request.cookies.get(SESSION_COOKIE)
        user = auth.get_user_by_token(token) if token else None
        if user is None:
            return RedirectResponse("/login", status_code=302)
        controllers = [
            dict(r)
            for r in conn.execute(
                "SELECT c.*, (SELECT COUNT(*) FROM zones z WHERE z.controller_id=c.id) AS zone_count "
                "FROM controllers c ORDER BY c.name"
            ).fetchall()
        ]
        logs = auth.recent_logs(20)
        return templates.TemplateResponse(
            "dashboard.html",
            ctx(
                request,
                user=dict(user),
                controllers=controllers,
                logs=logs,
                stage="Этап 1. Базовый серверный каркас",
            ),
        )

    @app.exception_handler(_NotAuthed)
    async def _not_authed_handler(request: Request, exc: _NotAuthed):
        return RedirectResponse("/login", status_code=302)

    @app.exception_handler(404)
    async def not_found(request: Request, exc):
        return render_error(templates, request, 404)

    @app.exception_handler(500)
    async def server_error(request: Request, exc):
        return render_error(templates, request, 500)

    return templates
