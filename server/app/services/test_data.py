"""Тестовые данные Этапа 1: тестовый контроллер и зоны (Этап 1).

На Этапе 2 эти данные будут управляться через CRUD-справочники.
"""
from __future__ import annotations

import sqlite3

from ..infra.config import Config
from ..infra.logging import get_logger
from ..infra.security import utcnow_iso

log = get_logger("poliv.testdata")


def seed_test_data(conn: sqlite3.Connection, cfg: Config) -> None:
    if not cfg.test_data_enabled:
        return
    td = cfg.raw["test_data"]
    box_id = td["box_id"]
    now = utcnow_iso()

    with conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO controllers(
                   name, box_id, description, enabled, connection_status,
                   status, created_at, updated_at)
               VALUES (?, ?, ?, 0, 'offline', 'idle', ?, ?)""",
            (
                td["controller_name"],
                box_id,
                "Тестовый контроллер для проверки каркаса (Этап 1). "
                "Заменить реальным устройством или эмулятором на Этапе 3.",
                now,
                now,
            ),
        )
        if cur.rowcount == 0:
            return  # уже существует — не дублируем зоны
        controller_id = cur.lastrowid
        for n in range(1, int(td["zones"]) + 1):
            conn.execute(
                """INSERT INTO zones(controller_id, zone_number, name, enabled,
                                     base_duration_minutes, created_at, updated_at)
                   VALUES (?, ?, ?, 1, 10, ?, ?)""",
                (controller_id, n, f"Тестовая зона {n}", now, now),
            )
    log.info(
        "Загружены тестовые данные: контроллер %s (%d зон)", box_id, int(td["zones"])
    )
