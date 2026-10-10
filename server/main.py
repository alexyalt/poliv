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
from contextlib import asynccontextmanager
from pathlib import Path

# Единая версия приложения для FastAPI, заголовков страниц и логотипа.
# К базовой версии автоматически добавляется короткий хэш последнего коммита git
# (например, 0.2.0+fd22a77), чтобы по заголовку вкладки видно, какие изменения
# реально загружены на локальную машину. Если git недоступен (запуск из копии
# без .git, продакшен-сборка) — остаётся только базовая версия.
APP_VERSION_BASE = "0.6.0"


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
from server.app.api.routes_stage3 import register_api_routes_stage3
from server.app.api.routes_stage4 import register_api_routes_stage4
from server.app.api.routes_stage5 import register_api_routes_stage5
from server.app.api.routes_stage6 import register_api_routes_stage6
from server.app.infra import mqtt_client as mqtt_infra
from server.app.services.auth_service import AuthService
from server.app.services.catalog_service import CatalogService
from server.app.services.event_service import init_event_service
from server.app.services.flow_service import init_flow_service
from server.app.services.mqtt_command_service import MqttCommandService
from server.app.services.notification_service import init_notification_service
from server.app.services.schedule_service import (
    ScheduleService, set_schedule_service)
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
    cmd_service = MqttCommandService(cfg, conn, db_path=str(cfg.db_path))
    # Этап 4: сервис машинограмм (компиляция + рассылка); MQTT подключается в lifespan.
    schedule_service = ScheduleService(cfg, conn, db_path=str(cfg.db_path))
    set_schedule_service(schedule_service)
    # Этап 6: события/расход/уведомления (singleton-сервисы; MQTT подключает
    # event/flow сервисы в lifespan, API/веб читают их через get_*()).
    event_service = init_event_service(conn)
    flow_service = init_flow_service(conn)
    notification_service = init_notification_service(conn)

    app = FastAPI(
        title="Автополив",
        version=APP_VERSION,
        lifespan=_lifespan_mqtt,   # Этап 3: старт/стоп MQTT-клиента (без эффектов при импорте)
    )
    app.state.cfg = cfg
    app.state.db = conn
    app.state.auth = auth
    app.state.catalog = catalog
    app.state.users = users
    app.state.command_service = cmd_service
    app.state.schedule_service = schedule_service
    # Этап 6: доступ к сервисам из lifespan/веб-маршрутов.
    app.state.event_service = event_service
    app.state.flow_service = flow_service
    app.state.notification_service = notification_service
    app.state.app_dir = str(ROOT / "app")

    app.mount("/static", StaticFiles(directory=app.state.app_dir + "/static"), name="static")
    templates = register_web_routes(app, cfg, conn, auth)
    # Порядок важен: routes_stage3 регистрирует GET /api/controllers/live ДО
    # обобщённого /api/controllers/{controller_id} из routes_stage2 (иначе
    # "live" парсится как controller_id → 422). Маршруты Этапа 4 — свои
    # префиксы (/api/controllers/{id}/schedules..., /api/schedules...),
    # регистрируются следом за stage3.
    register_api_routes_stage3(app, cfg, conn, cmd_service)
    register_api_routes_stage4(app, cfg, conn, schedule_service)
    # Этап 5: view-эндпоинты операторского интерфейса (/api/dashboard-view,
    # /api/controllers/{id}/view). Свой путь /api/dashboard-view;
    # /api/controllers/{id}/view не конфликтует с ранее зарегистрированными
    # точными путями stage3/stage4 (FastAPI отдаёт приоритет точным маршрутам).
    register_api_routes_stage5(app, cfg, conn)
    # Этап 6: REST API событий/расхода/уведомлений (/api/stage6/*).
    # Свой префикс /api/stage6 — не конфликтует с ранее зарегистрированными
    # маршрутами stage2..5.
    register_api_routes_stage6(app, cfg, conn)
    register_api_routes(app, cfg, conn, catalog, users)

    @app.get("/health")
    def health():
        return {"status": "ok", "stage": 6}

    log.info("Сервер готов. Веб-интерфейс: http://%s:%d/", cfg.server_host, cfg.server_port)
    return app


@asynccontextmanager
async def _lifespan_mqtt(app: FastAPI):
    """Lifespan-обёртка Этапа 3: MQTT-клиент стартует при запуске приложения и
    останавливается при завершении. Сервер НЕ падает, если брокер недоступен —
    paho продолжает попытки переподключения в своём фоновом потоке."""
    cfg = app.state.cfg
    try:
        inst = mqtt_infra.start_mqtt(
            cfg, db_path=str(cfg.db_path),
            command_service=app.state.command_service,
            schedule_service=getattr(app.state, "schedule_service", None),
            event_service=getattr(app.state, "event_service", None),
            flow_service=getattr(app.state, "flow_service", None),
        )
        app.state.mqtt = inst
    except Exception:
        log.exception("MQTT: не удалось запустить клиент — сервер работает без брокера")
    yield
    mqtt_infra.stop_mqtt()
    app.state.mqtt = None


try:
    app = create_app()
except ConfigError as exc:
    logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(message)s")
    logging.getLogger("poliv").error("Сервер не запущен: %s", exc)
    raise SystemExit(2)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=app.state.cfg.server_host, port=app.state.cfg.server_port)
