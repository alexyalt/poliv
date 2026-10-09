"""Миграция 0004_controller_live_state — живые поля состояния контроллеров (Этап 3).

Назначение (Артефакт 0.5 §4, Артефакт 0.6 §4.5): таблицы controllers не хватало
полей «живого» состояния, которые сервер пишет из MQTT hello/status/lwt/flow и из
которых собирается view-модель GET /api/controllers/{id}/live.

Правило процесса (AGENTS.md): применённые миграции 0001–0003 НЕ трогаются —
схема расширяется ТОЛЬКО новой миграцией. Всё идемпотентно через _has_column.

Состав (ALTER controllers ADD):
- connection_status TEXT DEFAULT 'offline'  (в 0001 колонка уже есть на новых БД —
  добавляется только если отсутствует; индекс создаётся всегда idempotentно);
- last_seen_at / last_hello_at / firmware_version / schedule_version / schedule_hash /
  time_valid / current_mode / current_phase / primary_zone / active_zones_json /
  display_zones_json / phase_end_ts / run_end_ts / pause_until_ts /
  emergency_lock_until_ts / flow_enabled / flow_total_liters / instant_lpm /
  service_mode_active / ip_address;
- индексы: idx_controllers_conn_status (connection_status),
  idx_controllers_last_seen (last_seen_at) — по ним работает офлайн-опрос раз в минуту.

После применения: PRAGMA table_info(controllers) содержит все перечисленные колонки.
"""
from __future__ import annotations

import sqlite3

# (колонка, объявление типа и значения по умолчанию)
ADD_COLUMNS: list[tuple[str, str]] = [
    ("connection_status", "TEXT DEFAULT 'offline'"),
    ("last_seen_at", "TEXT"),
    ("last_hello_at", "TEXT"),
    ("firmware_version", "TEXT"),
    ("schedule_version", "INTEGER DEFAULT 0"),
    ("schedule_hash", "TEXT"),
    ("time_valid", "INTEGER DEFAULT 0"),
    ("current_mode", "TEXT"),
    ("current_phase", "TEXT"),
    ("primary_zone", "INTEGER"),
    ("active_zones_json", "TEXT"),
    ("display_zones_json", "TEXT"),
    ("phase_end_ts", "INTEGER"),
    ("run_end_ts", "INTEGER"),
    ("pause_until_ts", "INTEGER"),
    ("emergency_lock_until_ts", "INTEGER"),
    ("flow_enabled", "INTEGER DEFAULT 0"),
    ("flow_total_liters", "REAL"),
    ("instant_lpm", "REAL"),
    ("service_mode_active", "INTEGER DEFAULT 0"),
    ("ip_address", "TEXT"),
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_controllers_conn_status ON controllers(connection_status)",
    "CREATE INDEX IF NOT EXISTS idx_controllers_last_seen ON controllers(last_seen_at)",
]


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    # Индекс [1], а не ["name"]: row_factory у соединения может быть кортежным.
    return any(
        c[1] == column
        for c in conn.execute(f"PRAGMA table_info({table})").fetchall()
    )


def upgrade(conn: sqlite3.Connection) -> None:
    for name, decl in ADD_COLUMNS:
        if not _has_column(conn, "controllers", name):
            conn.execute(f"ALTER TABLE controllers ADD COLUMN {name} {decl}")
    for stmt in INDEXES:
        conn.execute(stmt)
