"""Загрузка и проверка целостности конфигурации (Этап 1).

Правила из ТЗ:
- config.default.toml хранит безопасные значения по умолчанию;
- config.local.toml хранит локальные и секретные значения и не попадает в репозиторий;
- сервер при старте проверяет целостность конфигурации.
"""
from __future__ import annotations

import copy
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # корень репозитория poliv/
CONFIG_DIR = PROJECT_ROOT / "config"
DEFAULT_FILE = CONFIG_DIR / "config.default.toml"
LOCAL_FILE = CONFIG_DIR / "config.local.toml"


class ConfigError(RuntimeError):
    """Ошибка конфигурации: сервер не должен стартовать с ней."""


def _deep_merge(base: dict, override: dict) -> dict:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


@dataclass
class Config:
    raw: dict[str, Any] = field(default_factory=dict)
    project_root: Path = field(default_factory=lambda: PROJECT_ROOT)

    # --- удобные свойства -------------------------------------------------
    @property
    def server_host(self) -> str:
        return self.raw["server"]["host"]

    @property
    def server_port(self) -> int:
        return int(self.raw["server"]["port"])

    @property
    def secret_key(self) -> str:
        return self.raw["server"]["secret_key"]

    @property
    def session_timeout_min(self) -> int:
        return int(self.raw["server"]["session_timeout_min"])

    @property
    def login_rate_limit(self) -> int:
        return int(self.raw["server"]["login_rate_limit"])

    @property
    def db_path(self) -> Path:
        return self._abs(self.raw["paths"]["db_file"])

    @property
    def data_dir(self) -> Path:
        return self._abs(self.raw["paths"]["data_dir"])

    @property
    def logs_dir(self) -> Path:
        return self._abs(self.raw["paths"]["logs_dir"])

    @property
    def backups_dir(self) -> Path:
        return self._abs(self.raw["paths"]["backups_dir"])

    @property
    def log_level(self) -> str:
        return str(self.raw["logging"]["level"]).upper()

    @property
    def log_max_bytes(self) -> int:
        return int(self.raw["logging"]["max_bytes"])

    @property
    def log_backup_count(self) -> int:
        return int(self.raw["logging"]["backup_count"])

    @property
    def admin_username(self) -> str:
        return self.raw["admin"]["username"]

    @property
    def admin_password(self) -> str:
        return self.raw["admin"].get("password", "")

    @property
    def admin_must_change_password(self) -> bool:
        return bool(self.raw["admin"].get("must_change_password", True))

    @property
    def test_data_enabled(self) -> bool:
        return bool(self.raw["test_data"].get("enabled", False))

    def _abs(self, value: str) -> Path:
        p = Path(value)
        return p if p.is_absolute() else self.project_root / p


def validate(cfg_raw: dict) -> list[str]:
    """Проверка целостности конфигурации. Возвращает список ошибок."""
    errors: list[str] = []

    required = {
        ("server", "host"): str,
        ("server", "port"): int,
        ("server", "secret_key"): str,
        ("server", "session_timeout_min"): int,
        ("paths", "db_file"): str,
        ("paths", "data_dir"): str,
        ("paths", "logs_dir"): str,
        ("logging", "level"): str,
        ("admin", "username"): str,
    }
    node: Any = cfg_raw
    for section, name in required.keys():
        if not isinstance(node.get(section) if isinstance(node, dict) else None, dict):
            errors.append(f"Конфигурация: отсутствует раздел [{section}]")

    for (section, name), typ in required.items():
        try:
            value = cfg_raw[section][name]
        except (KeyError, TypeError):
            continue  # уже отмечено выше
        if typ is int:
            if not isinstance(value, int) or isinstance(value, bool):
                errors.append(f"[{section}].{name}: ожидается целое число")
        elif not isinstance(value, str) or not value.strip():
            errors.append(f"[{section}].{name}: ожидается непустая строка")

    try:
        port = int(cfg_raw["server"]["port"])
        if not (1 <= port <= 65535):
            errors.append("[server].port: должен быть в диапазоне 1..65535")
    except (KeyError, TypeError, ValueError):
        pass

    level = str(cfg_raw.get("logging", {}).get("level", "")).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        errors.append("[logging].level: допустимы DEBUG/INFO/WARNING/ERROR")

    if cfg_raw.get("server", {}).get("secret_key") == "CHANGE-ME-IN-CONFIG-LOCAL":
        errors.append(
            "[server].secret_key: задайте собственный ключ в config/config.local.toml"
        )
    return errors


def load_config(
    default_file: Path = DEFAULT_FILE,
    local_file: Path | None = LOCAL_FILE,
) -> Config:
    if not default_file.exists():
        raise ConfigError(f"Не найден файл конфигурации: {default_file}")
    with open(default_file, "rb") as fh:
        merged = tomllib.load(fh)

    if local_file is None:
        local_file = LOCAL_FILE
    env_local = os.environ.get("POLIV_CONFIG_LOCAL")
    if env_local:
        local_file = Path(env_local)
    if local_file.exists():
        with open(local_file, "rb") as fh:
            merged = _deep_merge(merged, tomllib.load(fh))

    errors = validate(merged)
    if errors:
        raise ConfigError(
            "Конфигурация не прошла проверку целостности:\n- "
            + "\n- ".join(errors)
        )
    return Config(raw=merged)
