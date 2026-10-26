"""Миграция 0010_stage8 — Этап 8, блок 8.2: файловый архив и ротация БД.

ADR-14: бесконечный файловый архив (data/archive/...), оперативная БД —
ограниченный срез (archive.db_retention_days).

Таблица archive_exports — журнал выгрузок (Артефакт 0.7 §17.9):
- entity_type   events | flow | notifications | weather | logs;
- date          дата среза (YYYY-MM-DD, UTC — каноническая таймзона проекта);
- file_path     путь файла относительно корня данных (data/archive/...);
- row_count     число выгруженных строк;
- file_hash     sha256 содержимого файла (целостность архива);
- created_at    момент выгрузки.

Настройки archive.* (Артефакт 0.7 §17.9, правила §23):
- archive.enabled            — включать ли фоновый ArchiveDaemon;
- archive.db_retention_days  — глубина оперативного среза БД (дней);
- archive.path               — подкаталог в корне данных для архива;
- archive.events_format      — csv | json (формат файлов выгрузки).

Миграция идемпотентна (CREATE TABLE IF NOT EXISTS / INSERT OR IGNORE) и
ОБРАТИМА: downgrade удаляет журнал и настройки archive.* (сами файлы
архива на диске не трогаются — они и есть долговременное хранилище).
"""
from __future__ import annotations

import json
import sqlite3

# Значения по умолчанию — Артефакт 0.7 §17.9 (Этап 8).
DEFAULT_ARCHIVE_SETTINGS = {
    "archive.enabled": True,
    "archive.db_retention_days": 365,
    "archive.path": "archive",
    "archive.events_format": "csv",
}

SQL = [
    """
    CREATE TABLE IF NOT EXISTS archive_exports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_type TEXT NOT NULL
            CHECK (entity_type IN ('events','flow','notifications',
                                   'weather','logs')),
        date TEXT NOT NULL,               -- дата среза YYYY-MM-DD (UTC)
        file_path TEXT NOT NULL,          -- путь к файлу (относительно data/)
        row_count INTEGER NOT NULL DEFAULT 0,
        file_hash TEXT,                   -- sha256 содержимого
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ae_type_date"
    " ON archive_exports(entity_type, date)",
]


def upgrade(conn: sqlite3.Connection) -> None:
    for stmt in SQL:
        conn.execute(stmt)
    for key, value in DEFAULT_ARCHIVE_SETTINGS.items():
        conn.execute(
            """INSERT OR IGNORE INTO settings (key, value_json, description, updated_at)
               VALUES (?, ?, ?, datetime('now'))""",
            (key, json.dumps(value), f"Этап 8: {key}"),
        )


def downgrade(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS archive_exports")
    conn.execute("DELETE FROM settings WHERE key LIKE 'archive.%'")
