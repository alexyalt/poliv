"""Общая инфраструктура тестов (Этап 3).

Тесты Этапа 1–2 импортируют боевое приложение `server.main` (`from server.main
import app`), которое при импорте загружает конфигурацию. Секретный ключ и
пароль MQTT по правилам ТЗ хранятся ТОЛЬКО в config/config.local.toml, который
не попадает в репозиторий — поэтому без локального конфига импорт упал бы с
ConfigError. Для тестов создаём временный config.local.toml (в tmp_path-подобном
каталоге вне репозитория) до первого импорта приложения; если разработчик уже
настроил собственный config.local.toml — ничего не трогаем.

Файл gitignore-безопасен: сам ничего не пишет в рабочее дерево.
"""
from __future__ import annotations

import os
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_FILE = PROJECT_ROOT / "config" / "config.local.toml"

# Если POLIV_CONFIG_LOCAL уже задан — считаем, что окружение настроено.
_env_local = os.environ.get("POLIV_CONFIG_LOCAL")
_target = Path(_env_local) if _env_local else LOCAL_FILE

if not _target.exists():
    # Проверяем, что default-конфиг требует секрет (secret_key = CHANGE-ME...).
    with open(PROJECT_ROOT / "config" / "config.default.toml", "rb") as fh:
        _default = tomllib.load(fh)
    if _default.get("server", {}).get("secret_key") == "CHANGE-ME-IN-CONFIG-LOCAL":
        _target.parent.mkdir(parents=True, exist_ok=True)
        _target.write_text(
            "# Автогенерировано conftest.py для прогона тестов (вне коммита).\n"
            "[server]\n"
            'secret_key = "test-secret-key-not-for-production"\n\n'
            "[mqtt]\n"
            'password = ""\n',
            encoding="utf-8",
        )
        if not _env_local:
            os.environ["POLIV_CONFIG_LOCAL"] = str(_target)
