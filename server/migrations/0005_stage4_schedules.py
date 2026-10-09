"""Миграция 0005_stage4_schedules — машинограммы и базовое исполнение (Этап 4).

Назначение (ТЗ Этап 4, Артефакт 0.7 §8.1, Артефакт 0.5 §3):
- controller_schedules — метаданные скомпилированных машинограмм: монотонная
  версия на контроллер, хеш содержимого, период действия, статус жизненного
  цикла (draft/compiled/sent/acknowledged/failed), источник компиляции, путь к
  JSON-файлу машинограммы (содержимое хранится файлом, в БД — метаданные);
- watering_runs — журнал запусков полива (план/факт/завершение, источник,
  версия машинограммы) — «программа стартует, зона открывается» видно в БД;
- settings: ключ apply_policy — политика применения новой версии расписания
  во время активного прогона (ТЗ Этап 4, п. 4).

Правило процесса (AGENTS.md): применённые миграции 0001–0004 не изменяются,
схема расширяется только этой миграцией; всё идемпотентно.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SQL = [
    """
    CREATE TABLE IF NOT EXISTS controller_schedules (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id       INTEGER NOT NULL REFERENCES controllers(id),
        schedule_version    INTEGER NOT NULL,      -- монотонная версия на контроллер (§3.10)
        valid_from_date     TEXT NOT NULL,         -- локальная дата начала действия
        valid_to_date       TEXT NOT NULL,         -- локальная дата окончания
        timezone_offset_min INTEGER NOT NULL DEFAULT 0,
        source              TEXT NOT NULL CHECK
            (source IN ('auto','manual','weather','normalization','offline_export')),
        status              TEXT NOT NULL CHECK
            (status IN ('draft','compiled','sent','acknowledged','failed',
                        'offline_export','imported_offline')),
        schedule_hash       TEXT NOT NULL,         -- sha256 канонического содержимого
        payload_path        TEXT,                  -- data/schedules/{box_id}/v<N>.json
        runs_count          INTEGER NOT NULL DEFAULT 0,
        warnings_json       TEXT,                  -- предупреждения компиляции
        errors_json         TEXT,                  -- ошибки/конфликты компиляции
        error_message       TEXT,
        created_by          TEXT,
        created_at          TEXT NOT NULL,
        sent_at             TEXT,
        acknowledged_at     TEXT,
        UNIQUE (controller_id, schedule_version)   -- ограничение ТЗ §8.1
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sched_controller ON controller_schedules(controller_id)",
    "CREATE INDEX IF NOT EXISTS idx_sched_status ON controller_schedules(status)",
    "CREATE INDEX IF NOT EXISTS idx_sched_dates ON controller_schedules(valid_from_date, valid_to_date)",
    """
    CREATE TABLE IF NOT EXISTS watering_runs (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id      INTEGER NOT NULL REFERENCES controllers(id),
        box_id             TEXT NOT NULL,
        run_id             TEXT NOT NULL UNIQUE,   -- uuid запуска (поле машинограммы run_id)
        program_id         INTEGER REFERENCES programs(id),
        schedule_id        INTEGER REFERENCES controller_schedules(id),
        schedule_version   INTEGER,                -- версия машинограммы-источника
        source             TEXT NOT NULL CHECK
            (source IN ('schedule','manual','service','test','emergency','offline_import')),
        status             TEXT NOT NULL CHECK
            (status IN ('planned','active','completed','skipped','aborted','failed')),
        reason_code        TEXT,                   -- rain_delay / zone_locked / disabled / ...
        planned_start_ts   INTEGER,                -- плановый старт (unix)
        actual_start_ts    INTEGER,                -- фактический старт (unix)
        end_ts             INTEGER,                -- завершение (unix)
        water_sec          INTEGER NOT NULL DEFAULT 0,
        zones_json         TEXT,                   -- участвовавшие зоны (номера)
        details_json       TEXT,
        created_at         TEXT NOT NULL,
        updated_at         TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_wr_controller ON watering_runs(controller_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_wr_planned ON watering_runs(planned_start_ts)",
]

# Новые настройки этапа (значения по умолчанию задаёт администратор в UI).
SETTING_DEFAULTS = [
    ("schedule.apply_policy", '"next_run"',
     "Применение новой версии расписания: next_run (безопасная — после "
     "завершения текущего прогона) / immediate"),
    ("schedule.min_duration_minutes", "1", "Минимальная длительность шага полива"),
    ("schedule.max_duration_minutes", "240", "Максимальная длительность шага полива"),
]


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(
        c[1] == column
        for c in conn.execute(f"PRAGMA table_info({table})").fetchall()
    )


def upgrade(conn: sqlite3.Connection) -> None:
    for statement in SQL:
        conn.execute(statement)
    now = datetime.now(timezone.utc).isoformat()
    for key, value_json, desc in SETTING_DEFAULTS:
        conn.execute(
            """INSERT OR IGNORE INTO settings(key, value_json, description,
                                              updated_at, updated_by)
               VALUES (?, ?, ?, ?, 'system')""",
            (key, value_json, desc, now),
        )
