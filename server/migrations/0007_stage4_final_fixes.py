"""Миграция 0007_stage4_final_fixes — закрытие замечаний аудитора (Этап 4 final).

Назначение (замечания P1-2, P1-3, P1-5):
- pending_schedule — очередь отложенной доставки машинограмм: публикация
  новой версии во время активного прогона (apply_policy=next_run) сохраняется
  в БД и переживает перезапуск сервера; при завершении прогона (event
  finished/stopped) последняя queued-версия отправляется автоматически;
- schedule_compile_errors — автокомпиляция после CRUD программ/зон/настроек:
  ошибки компиляции (конфликт) сохраняются и видны UI («настройки сохранены,
  расписание НЕ применено»), сами настройки при этом применяются;
- schedule_rejections — история отказов контроллера (schedule_ack rejected)
  с причиной; версия controllers.schedule_version при отказе не меняется.

Правило процесса (AGENTS.md): применённые миграции 0001–0006 не изменяются,
схема расширяется только этой миграцией; всё идемпотентно.
"""
from __future__ import annotations

import sqlite3

SQL = [
    """
    CREATE TABLE IF NOT EXISTS pending_schedule (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id  INTEGER NOT NULL REFERENCES controllers(id),
        schedule_id    INTEGER NOT NULL REFERENCES controller_schedules(id),
        status         TEXT NOT NULL CHECK
            (status IN ('queued','sent','cancelled','failed')),
        reason         TEXT,                       -- active_run_next_run_policy / ...
        created_at     TEXT NOT NULL,
        updated_at     TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pending_ctrl "
    "ON pending_schedule(controller_id, status)",
    """
    CREATE TABLE IF NOT EXISTS schedule_compile_errors (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id  INTEGER NOT NULL REFERENCES controllers(id),
        reason         TEXT,                       -- program.updated / zone.deleted / ...
        error_json     TEXT NOT NULL,              -- {code, message, conflicts:[...]}
        ts             TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sce_ctrl ON schedule_compile_errors(controller_id, ts)",
    """
    CREATE TABLE IF NOT EXISTS schedule_rejections (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id  INTEGER NOT NULL REFERENCES controllers(id),
        version        INTEGER NOT NULL,           -- отвергнутая версия машинограммы
        reason         TEXT,                       -- текст из schedule_ack
        ts             TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rej_ctrl ON schedule_rejections(controller_id, ts)",
    # P2-8: статус 'cancelled' — отмена устаревших planned-записей при
    # применении новой версии машинограммы (replaced_by_newer_schedule).
    # SQLite не умеет ALTER CHECK — пересоздаём таблицу (идемпотентно:
    # выполняется только если cancelled ещё нет в определении).
]

_CANCELLED_CHECK_SQL = [
    """CREATE TABLE watering_runs__new (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id      INTEGER NOT NULL REFERENCES controllers(id),
        box_id             TEXT NOT NULL,
        run_id             TEXT NOT NULL UNIQUE,
        program_id         INTEGER REFERENCES programs(id),
        schedule_id        INTEGER REFERENCES controller_schedules(id),
        schedule_version   INTEGER,
        source             TEXT NOT NULL CHECK
            (source IN ('schedule','manual','service','test','emergency','offline_import')),
        status             TEXT NOT NULL CHECK
            (status IN ('planned','active','completed','skipped',
                        'aborted','failed','cancelled')),
        reason_code        TEXT,
        planned_start_ts   INTEGER,
        actual_start_ts    INTEGER,
        end_ts             INTEGER,
        water_sec          INTEGER NOT NULL DEFAULT 0,
        volume_liters      REAL,
        zones_json         TEXT,
        details_json       TEXT,
        created_at         TEXT NOT NULL,
        updated_at         TEXT NOT NULL
    )""",
    """INSERT INTO watering_runs__new (
           id, controller_id, box_id, run_id, program_id, schedule_id,
           schedule_version, source, status, reason_code, planned_start_ts,
           actual_start_ts, end_ts, water_sec, zones_json, details_json,
           created_at, updated_at)
       SELECT id, controller_id, box_id, run_id, program_id, schedule_id,
              schedule_version, source, status, reason_code, planned_start_ts,
              actual_start_ts, end_ts, water_sec, zones_json, details_json,
              created_at, updated_at
       FROM watering_runs""",
    "DROP TABLE watering_runs",
    "ALTER TABLE watering_runs__new RENAME TO watering_runs",
    "CREATE INDEX IF NOT EXISTS idx_wr_controller ON watering_runs(controller_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_wr_planned ON watering_runs(planned_start_ts)",
]


def upgrade(conn: sqlite3.Connection) -> None:
    for stmt in SQL:
        conn.execute(stmt)
    # Пересоздание watering_runs только при необходимости (идемпотентность):
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='watering_runs'"
    ).fetchone()
    if row and "cancelled" not in (row["sql"] or ""):
        for stmt in _CANCELLED_CHECK_SQL:
            conn.execute(stmt)
