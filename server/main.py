"""Точка входа сервера Автополива (Этап 1).

Запуск:
    python -m uvicorn server.main:app --host 0.0.0.0 --port 8080
или:
    python server/main.py

Порядок старта (критерии Этапа 1):
1. Загрузка и проверка целостности конфигурации.
2. Настройка логирования в файлы с ротацией.
3. Инициализация базы данных + миграции.
4. Создание первого администратора из конфига.
5. Загрузка тестового контроллера и тестовых данных.
6. Регистрация веб-маршрутов (вход/выход/дашборд/ошибки).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

# Единая версия приложения для FastAPI, заголовков страниц и логотипа.
# К базовой версии автоматически добавляется короткий хэш последнего коммита git
# (например, 0.2.0+fd22a77), чтобы по заголовку вкладки видно, какие изменения
# реально загружены на локальную машину. Если git недоступен (запуск из копии
# без .git, продакшен-сборка) — остаётся только базовая версия.
APP_VERSION_BASE = "0.2.0"


def _git_short_hash() -> str:
    """Короткий хэш HEAD; пустая строка, если определить не удалось."""
    import subprocess

    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(Path(__file__).resolve().parent.parent),
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return ""


APP_VERSION = f"{APP_VERSION_BASE}+{_git_short_hash()}" if _git_short_hash() else APP_VERSION_BASE

# Позволяем запускать и как пакет, и напрямую файлом.
ROOT = Path(__file__).resolve().parent
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from server.app.infra.config import ConfigError, load_config
from server.app.infra.db import init_db
from server.app.infra.logging import get_logger, setup_logging
from server.app.api.routes_stage2 import register_api_routes
from server.app.services.auth_service import AuthService
from server.app.services.catalog_service import CatalogService
from server.app.services.test_data import seed_test_data
from server.app.services.user_service import UserService
from server.app.web.routes import register_web_routes

log = get_logger("poliv.main")


def create_app(cfg: Config | None = None) -> FastAPI:
    if cfg is None:
        cfg = load_config()

    # Пути времени выполнения создаём при старте.
    for d in (cfg.data_dir, cfg.logs_dir, cfg.backups_dir):
        d.mkdir(parents=True, exist_ok=True)

    setup_logging(cfg)
    log.info("Конфигурация загружена и проверена: %s", ROOT.parent / "config")

    conn = init_db(cfg)
    auth = AuthService(conn, cfg)
    auth.ensure_admin()
    seed_test_data(conn, cfg)
    auth.cleanup_sessions()

    catalog = CatalogService(conn)
    users = UserService(conn, auth)

    app = FastAPI(title="Автополив", version=APP_VERSION)
    app.state.cfg = cfg
    app.state.db = conn
    app.state.auth = auth
    app.state.catalog = catalog
    app.state.users = users
    app.state.app_dir = str(ROOT / "app")

    app.mount("/static", StaticFiles(directory=app.state.app_dir + "/static"), name="static")
    templates = register_web_routes(app, cfg, conn, auth)
    register_api_routes(app, cfg, conn, catalog, users)

    @app.get("/health")
    def health():
        return {"status": "ok", "stage": 2}

    log.info("Сервер готов. Веб-интерфейс: http://%s:%d/", cfg.server_host, cfg.server_port)
    return app


try:
    app = create_app()
except ConfigError as exc:
    logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(message)s")
    logging.getLogger("poliv").error("Сервер не запущен: %s", exc)
    raise SystemExit(2)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=app.state.cfg.server_host, port=app.state.cfg.server_port)
