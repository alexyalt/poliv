"""Миграция 0008_stage6 — Этап 6: события полива, расход, аварии, уведомления.

Назначение (по qwen_chat/stage6_recomendations, адаптировано под фактическую
структуру проекта):
- watering_events        — приём событий из poliv/{box_id}/event с дедупликацией
  по event_uid (ТЗ §9.2); run_id — TEXT (как в watering_runs, миграция 0007);
- watering_event_zones   — связь событий с зонами (параллельные группы);
- flow_daily             — дневные агрегаты расхода (источник истины — события);
- notifications          — уведомления info/warning/error/critical с дедупликацией;
- пороги потока и флаг звука — в таблицу settings (ключ/value_json, миграция 0001).

Колонки controllers.flow_total_liters / instant_lpm / emergency_lock_until_ts /
flow_enabled добавляться НЕ требуются — они уже созданы миграцией 0004.

Правило процесса (AGENTS.md): применённые миграции 0001–0007 не изменяются,
схема расширяется только этой миграцией; всё идемпотентно.
"""
from __future__ import annotations

import json
import sqlite3

DEFAULT_FLOW_SETTINGS = {
    "flow.enabled_default": False,          # датчик ставится опционально (ТЗ §9)
    "flow.no_flow_delay_sec": 10,
    "flow.overflow_percent": 150,
    "flow.underflow_percent": 70,
    "flow.stable_delay_sec": 15,
    "flow.emergency_lock_min": 30,
    "flow.min_samples_for_expected": 3,
}

DEFAULT_NOTIFICATION_SETTINGS = {
    "ui.sound_notifications_enabled": True,
}

SQL = [
    """
    CREATE TABLE IF NOT EXISTS watering_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_uid TEXT NOT NULL UNIQUE,
        started_event_uid TEXT,
        run_id TEXT,
        controller_id INTEGER NOT NULL REFERENCES controllers(id),
        primary_zone_id INTEGER,
        parallel INTEGER NOT NULL DEFAULT 0,
        source TEXT NOT NULL,
        status TEXT NOT NULL,
        start_ts INTEGER NOT NULL,
        end_ts INTEGER,
        water_sec INTEGER NOT NULL DEFAULT 0,
        wall_sec INTEGER,
        volume_liters REAL,
        expected_flow_lpm REAL,
        reason_code TEXT,
        aborted INTEGER,
        schedule_version INTEGER,
        buffered INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        archived_at TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_events_controller ON watering_events(controller_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_start ON watering_events(start_ts)",
    "CREATE INDEX IF NOT EXISTS idx_events_status ON watering_events(status)",
    """
    CREATE TABLE IF NOT EXISTS watering_event_zones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER NOT NULL REFERENCES watering_events(id),
        zone_id INTEGER NOT NULL,
        water_sec INTEGER,
        volume_liters REAL,
        expected_flow_lpm REAL,
        UNIQUE(event_id, zone_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS flow_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        controller_id INTEGER NOT NULL,
        zone_id INTEGER NOT NULL DEFAULT -1,   -- -1 = агрегат по контроллеру (SQLite не допускает выражения в UNIQUE)
        attribution TEXT NOT NULL DEFAULT 'controller',
        total_volume_l REAL NOT NULL DEFAULT 0,
        watering_count INTEGER NOT NULL DEFAULT 0,
        water_sec INTEGER NOT NULL DEFAULT 0,
        anomaly_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(date, controller_id, zone_id, attribution)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_flow_daily_date ON flow_daily(date)",
    """
    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        severity TEXT NOT NULL CHECK
            (severity IN ('info','warning','error','critical')),
        controller_id INTEGER,
        zone_id INTEGER,
        run_id TEXT,
        event_id INTEGER,
        message TEXT NOT NULL,
        details_json TEXT,
        dedupe_key TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        read_at TEXT,
        resolved_at TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_notifications_created ON notifications(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_notifications_severity ON notifications(severity)",
    "CREATE INDEX IF NOT EXISTS idx_notifications_read ON notifications(read_at)",
    "CREATE INDEX IF NOT EXISTS idx_notifications_dedupe ON notifications(dedupe_key)",
]


def upgrade(conn: sqlite3.Connection) -> None:
    for stmt in SQL:
        conn.execute(stmt)
    for key, value in {**DEFAULT_FLOW_SETTINGS, **DEFAULT_NOTIFICATION_SETTINGS}.items():
        conn.execute(
            """INSERT OR IGNORE INTO settings (key, value_json, description, updated_at)
               VALUES (?, ?, ?, datetime('now'))""",
            (key, json.dumps(value), f"Этап 6: {key}"),
        )
