"""Общая инфраструктура тестов (Этап 3 + изоляция конфига).

Тесты импортируют боевое приложение `server.main` (`from server.main
import app`), которое при импорте загружает конфигурацию. Секретный ключ и
пароль MQTT по правилам ТЗ хранятся ТОЛЬКО в config/config.local.toml, который
не попадает в репозиторий — поэтому без локального конфига импорт упал бы с
ConfigError.

Правило изоляции (config_robustness_and_emulator_fix): pytest ВСЕГДА работает на
собственном тестовом конфиге во временном каталоге — локальный конфиг
разработчика (config/config.local.toml) НЕ читается. Это гарантирует зелёный
прогон независимо от состояния локального файла: битый TOML (например,
дублирующая секция [server]) у разработчика больше не роняет тесты. Исключение:
если POLIV_CONFIG_LOCAL задан явно извне — уважаем его и ничего не генерируем.

ENV-подмена выполняется на импорте conftest.py — ДО импорта приложения
(pytest гарантированно импортирует conftest раньше тестовых модулей).

Файл gitignore-безопасен: сам ничего не пишет в рабочее дерево.
"""
from __future__ import annotations

import atexit
import os
import shutil
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# --- Тестовый config.local.toml во временном каталоге ----------------------
_TEST_LOCAL_CONTENT = (
    "# Сгенерировано server/tests/conftest.py для прогона тестов.\n"
    "[server]\n"
    'secret_key = "test-secret-key-not-for-production"\n\n'
    "[mqtt]\n"
    'password = ""\n'
)

if not os.environ.get("POLIV_CONFIG_LOCAL"):
    # Локальный конфиг разработчика не читаем: всегда свой файл в temp-каталоге.
    _tmpdir = Path(tempfile.mkdtemp(prefix="poliv-tests-config-"))
    _local = _tmpdir / "config.local.toml"
    _local.write_text(_TEST_LOCAL_CONTENT, encoding="utf-8")
    os.environ["POLIV_CONFIG_LOCAL"] = str(_local)
    atexit.register(shutil.rmtree, _tmpdir, True)
