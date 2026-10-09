"""Миграция 0006_stage4_parallel_enabled — разрешение параллельной работы зон (Этап 4).

Назначение (ТЗ Этап 4, п.1: компилятор формирует параллельные шаги ADR-12;
Артефакт 0.5 §3.4: опция машинограммы options.parallel_enabled; Артефакт 0.7
§17.3: настройка controller.parallel_enabled_default):
- controllers.parallel_enabled INTEGER DEFAULT 0 — флаг «разрешить параллельную
  работу зон» на конкретном контроллере. По умолчанию 0: зоны программы с
  одинаковым parallel_group компилируются в последовательные шаги (ADR-12:
  параллель — только явным group И разрешением на контроллере);
- settings controller.parallel_enabled_default = false — значение по умолчанию
  для новых контроллеров (список настроек ТЗ §17.3).

Правило процесса (AGENTS.md): применённые миграции 0001–0005 не изменяются,
схема расширяется только этой миграцией; всё идемпотентно (_has_column,
INSERT OR IGNORE).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

ADD_COLUMNS = [
    ("controllers", "parallel_enabled", "INTEGER NOT NULL DEFAULT 0"),
]

SETTING_DEFAULTS = [
    ("controller.parallel_enabled_default", "false",
     "Разрешать параллельную работу зон по умолчанию для новых контроллеров "
     "(ADR-12)"),
]


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    # Индекс [1], а не ["name"]: row_factory у соединения может быть кортежным.
    return any(
        c[1] == column
        for c in conn.execute(f"PRAGMA table_info({table})").fetchall()
    )


def upgrade(conn: sqlite3.Connection) -> None:
    for table, name, decl in ADD_COLUMNS:
        if not _has_column(conn, table, name):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
    now = datetime.now(timezone.utc).isoformat()
    for key, value_json, desc in SETTING_DEFAULTS:
        conn.execute(
            """INSERT OR IGNORE INTO settings(key, value_json, description,
                                              updated_at, updated_by)
               VALUES (?, ?, ?, ?, 'system')""",
            (key, value_json, desc, now),
        )
