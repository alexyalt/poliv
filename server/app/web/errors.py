"""Понятные страницы ошибок (Этап 1: «ошибки отображаются понятно»)."""
from __future__ import annotations

from fastapi.responses import HTMLResponse


def render_error(templates, request, code: int) -> HTMLResponse:
    titles = {
        403: ("Доступ запрещён", "У вашей роли нет прав на это действие."),
        404: ("Страница не найдена", "Проверьте адрес или вернитесь на главную."),
        500: ("Внутренняя ошибка сервера",
              "Ошибка записана в logs/error.log. Обратитесь к администратору."),
    }
    title, text = titles.get(code, ("Ошибка", f"Код ошибки: {code}"))
    return templates.TemplateResponse(
        request,
        f"errors/{code}.html" if code in titles and code != 500 else "errors/500.html",
        {"code": code, "title": title, "text": text},
        status_code=code,
    )
