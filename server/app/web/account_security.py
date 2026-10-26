"""Account recovery and same-origin checks for cookie-authenticated browsers."""
from contextlib import closing
from urllib.parse import urlsplit

from fastapi import Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.concurrency import run_in_threadpool

from ..infra.db import connect_path
from ..infra.security import hash_password, hash_token, verify_password
from ..services.auth_service import AuthService, SESSION_COOKIE


def register_account_security(app, cfg, db_path):
    """Keep temporary-password users on a self-service change path.

    Browser requests with Origin/Referer must match the application origin.
    Requests without those headers remain supported for existing API clients;
    Sec-Fetch-Site explicitly declaring cross-site is rejected.
    """
    def current(token):
        with closing(connect_path(db_path)) as conn:
            user = AuthService(conn, cfg).get_user_by_token(token, touch=False) if token else None
            return dict(user) if user else None

    @app.middleware('http')
    async def account_guard(request: Request, call_next):
        if request.method not in {'GET', 'HEAD', 'OPTIONS'}:
            origin = request.headers.get('origin') or request.headers.get('referer')
            actual = urlsplit(str(request.url))
            expected = (actual.scheme, actual.hostname, actual.port or (443 if actual.scheme == 'https' else 80))
            if origin:
                try:
                    parsed = urlsplit(origin)
                    supplied = (parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == 'https' else 80))
                except ValueError:
                    supplied = None
                if supplied != expected:
                    return JSONResponse({'detail': 'Untrusted request origin'}, status_code=403)
            elif request.headers.get('sec-fetch-site') == 'cross-site':
                return JSONResponse({'detail': 'Untrusted request origin'}, status_code=403)
        path = request.url.path
        if path not in {'/login', '/logout', '/account/password', '/health'} and not path.startswith('/static/'):
            user = await run_in_threadpool(current, request.cookies.get(SESSION_COOKIE))
            if user and user['must_change_password']:
                if path.startswith('/api/'):
                    return JSONResponse({'detail': 'Password change required',
                                         'code': 'password_change_required'}, status_code=403)
                return RedirectResponse('/account/password', status_code=303)
        return await call_next(request)

    def form(error=''):
        # Error strings are fixed internal messages, never user input.
        return HTMLResponse('''<!doctype html><html lang="ru"><meta charset="utf-8">
<title>Смена пароля — Полив</title><h1>Смена пароля</h1><p>''' + error + '''</p>
<form method="post" action="/account/password">
<label>Текущий пароль <input type="password" name="current_password" required autocomplete="current-password"></label><br>
<label>Новый пароль (от 8 символов) <input type="password" name="new_password" required minlength="8" autocomplete="new-password"></label><br>
<label>Повторите новый пароль <input type="password" name="confirm_password" required autocomplete="new-password"></label><br>
<button type="submit">Сохранить пароль</button></form>
<form method="post" action="/logout"><button type="submit">Выйти</button></form></html>''',
                            headers={'Cache-Control': 'no-store'})

    @app.get('/account/password', response_class=HTMLResponse)
    def password_page(request: Request):
        if not current(request.cookies.get(SESSION_COOKIE)):
            return RedirectResponse('/login', status_code=303)
        return form()

    @app.post('/account/password')
    def password_change(request: Request, current_password: str = Form(...),
                        new_password: str = Form(...), confirm_password: str = Form(...)):
        token = request.cookies.get(SESSION_COOKIE)
        with closing(connect_path(db_path)) as conn:
            user = AuthService(conn, cfg).get_user_by_token(token, touch=False) if token else None
            if user is None:
                return RedirectResponse('/login', status_code=303)
            if not verify_password(current_password, user['password_hash']):
                response = form('Текущий пароль неверен.')
                response.status_code = 422
                return response
            if (new_password != confirm_password or len(new_password) < 8 or
                    len(new_password.encode('utf-8')) > 72 or new_password == current_password):
                response = form('Проверьте совпадение и длину нового пароля; он должен отличаться от текущего.')
                response.status_code = 422
                return response
            with conn:
                conn.execute('UPDATE users SET password_hash=?, must_change_password=0 WHERE id=?',
                             (hash_password(new_password), user['id']))
                conn.execute('DELETE FROM sessions WHERE user_id=? AND token_hash<>?',
                             (user['id'], hash_token(token)))
            AuthService(conn, cfg).write_log(user['id'], user['username'], 'user.password_changed', 'user', user['id'])
        return RedirectResponse('/', status_code=303)
