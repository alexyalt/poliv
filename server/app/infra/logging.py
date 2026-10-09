"""Логирование в файлы с ротацией (Этап 1).

Правила из ТЗ:
- логи пишутся в файлы (logs/app.log);
- ошибки пишутся отдельно (logs/error.log);
- ротация логов по размеру.
"""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from .config import Config

_FMT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"
_configured = False


def setup_logging(cfg: Config) -> None:
    global _configured
    if _configured:
        return
    cfg.logs_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    formatter = logging.Formatter(_FMT, datefmt=_DATEFMT)

    def add_file(name: str, level: int) -> None:
        handler = RotatingFileHandler(
            cfg.logs_dir / name,
            maxBytes=cfg.log_max_bytes,
            backupCount=cfg.log_backup_count,
            encoding="utf-8",
        )
        handler.setLevel(level)
        handler.setFormatter(formatter)
        root.addHandler(handler)

    add_file("app.log", logging.DEBUG)
    add_file("error.log", logging.ERROR)
    # Этап 3: MQTT-обмен логируется в отдельный файл с той же ротацией.
    # Логгер "poliv.mqtt" пишет и в app.log (наследование root), и в mqtt.log.
    mqtt_handler = RotatingFileHandler(
        cfg.logs_dir / "mqtt.log",
        maxBytes=cfg.log_max_bytes,
        backupCount=cfg.log_backup_count,
        encoding="utf-8",
    )
    mqtt_handler.setLevel(logging.DEBUG)
    mqtt_handler.setFormatter(formatter)
    mqtt_logger = logging.getLogger("poliv.mqtt")
    mqtt_logger.addHandler(mqtt_handler)
    # paho internally uses its own logger — route it into poliv.mqtt too.
    paho_logger = logging.getLogger("paho")
    paho_logger.handlers.clear()
    paho_logger.addHandler(mqtt_handler)
    paho_logger.propagate = False

    console = logging.StreamHandler()
    console.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    console.setFormatter(formatter)
    root.addHandler(console)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
