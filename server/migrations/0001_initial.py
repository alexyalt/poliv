"""Миграция 0001_initial — базовые таблицы Этапа 1.

Состав (по артефакту 0.7, минимально necessary для запуска и входа):
- schema_migrations — учёт применённых миграций;
- users            — пользователи и роли;
- sessions         — активные сессии;
- logs             — журнал действий;
- settings         — таблица настроек (настройки не хардкодятся);
- controllers      — контроллеры (для тестового контроллера);
- zones            — зоны полива.

Полный состав таблиц справочников и исполнения добавляется следующими
миграциями на Этапах 2+ (см. docs/03_data_model.md).
"""
from __future__ import annotations

import sqlite3

SQL = [
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version    TEXT PRIMARY KEY,
        applied_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        username             TEXT NOT NULL UNIQUE,
        password_hash        TEXT NOT NULL,
        role                 TEXT NOT NULL CHECK (role IN ('admin','operator','viewer')),
        enabled              INTEGER NOT NULL DEFAULT 1,
        must_change_password INTEGER NOT NULL DEFAULT 0,
        last_login_at        TEXT,
        created_at           TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id      INTEGER NOT NULL REFERENCES users(id),
        token_hash   TEXT NOT NULL UNIQUE,
        created_at   TEXT NOT NULL,
        expires_at   TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        ip           TEXT,
        user_agent   TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)",
    """
    CREATE TABLE IF NOT EXISTS logs (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        ts           TEXT NOT NULL,
        user_id      INTEGER,
        username     TEXT,
        action       TEXT NOT NULL,
        object_type  TEXT,
        object_id    TEXT,
        details_json TEXT,
        ip           TEXT,
        source       TEXT NOT NULL DEFAULT 'web'
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts)",
    "CREATE INDEX IF NOT EXISTS idx_logs_user ON logs(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_logs_action ON logs(action)",
    """
    CREATE TABLE IF NOT EXISTS settings (
        key         TEXT PRIMARY KEY,
        value_json  TEXT NOT NULL,
        description TEXT,
        updated_at  TEXT,
        updated_by  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS controllers (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        name              TEXT NOT NULL,
        box_id            TEXT NOT NULL UNIQUE,
        description       TEXT,
        enabled           INTEGER NOT NULL DEFAULT 1,
        provisioning_state TEXT NOT NULL DEFAULT 'new',
        connection_status TEXT NOT NULL DEFAULT 'offline',
        status            TEXT NOT NULL DEFAULT 'idle',
        status_reason     TEXT,
        firmware_version  TEXT,
        schedule_version  INTEGER,
        schedule_hash     TEXT,
        last_seen_at      TEXT,
        last_hello_at     TEXT,
        time_valid        INTEGER NOT NULL DEFAULT 0,
        timezone_offset_min INTEGER,
        ip_address        TEXT,
        mac_address       TEXT,
        created_at        TEXT NOT NULL,
        updated_at        TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_controllers_box ON controllers(box_id)",
    "CREATE INDEX IF NOT EXISTS idx_controllers_enabled ON controllers(enabled)",
    """
    CREATE TABLE IF NOT EXISTS zones (
        id                        INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id             INTEGER NOT NULL REFERENCES controllers(id),
        zone_number               INTEGER NOT NULL CHECK (zone_number BETWEEN 1 AND 16),
        name                      TEXT NOT NULL,
        enabled                   INTEGER NOT NULL DEFAULT 1,
        icon                      TEXT,
        image_path                TEXT,
        image_thumb_path          TEXT,
        notes                     TEXT,
        base_duration_minutes     INTEGER NOT NULL DEFAULT 10,
        watering_adjustment_percent INTEGER NOT NULL DEFAULT 100,
        max_runtime_minutes       INTEGER,
        cycle_soak_enabled        INTEGER NOT NULL DEFAULT 0,
        cycle_minutes             INTEGER,
        soak_minutes              INTEGER,
        soak_after_last           INTEGER,
        expected_flow_lpm         REAL,
        flow_tolerance_percent    INTEGER,
        learn_flow_enabled        INTEGER NOT NULL DEFAULT 0,
        created_at                TEXT NOT NULL,
        updated_at                TEXT NOT NULL,
        UNIQUE (controller_id, zone_number)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_zones_controller ON zones(controller_id)",
]


def upgrade(conn: sqlite3.Connection) -> None:
    for statement in SQL:
        conn.execute(statement)
