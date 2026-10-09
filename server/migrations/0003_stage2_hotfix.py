"""Миграция 0003_stage2_hotfix — устранение разрыва схемы на «старых» БД (hotfix Этапа 2).

Причина (WorkBook, запись 14): на боевых БД, созданных ДО правок before_stage_3,
миграция 0002 уже была применена и повторнo не запускается, поэтому её новые
правки (ADDITIONS parallel_group, удаление season_start/season_end) на таких базах
не выполнились. Править применённую миграцию 0002 ЗАПРЕЩЕНО — изменения
применённых миграций переписывают историю и ломают идемпотентность прогонов.
Эта миграция доводит схему старых БД до ожидаемого кодом состояния.

Состав (всё идемпотентно, на новых БД — no-op):
- program_zones: ADD COLUMN parallel_group TEXT (ADR-12), если колонки нет;
- zones / programs: DROP COLUMN season_start / season_end (SQLite >= 3.35),
  если легаси-колонки ещё остались в схеме.

После применения: PRAGMA table_info(program_zones) содержит parallel_group,
а в zones/programs колонок сезона нет.
"""
from __future__ import annotations

import sqlite3

# Легаси-колонки сезонности (упразднена правками v1/v2 Этапа 2, п. 3.5/3.10, 4.1/4.5).
LEGACY_DROP_COLUMNS = {
    "zones": ("season_start", "season_end"),
    "programs": ("season_start", "season_end"),
}


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    # Индекс [1], а не ["name"]: row_factory у соединения может быть кортежным
    # (например, в тестах/утилитах без sqlite3.Row) — тогда dict-доступ падает.
    return any(
        c[1] == column
        for c in conn.execute(f"PRAGMA table_info({table})").fetchall()
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def upgrade(conn: sqlite3.Connection) -> None:
    # 1) ADR-12: группа параллельности для связей программы и зон.
    #    На новых БД колонка уже есть (добавлена 0002) — пропускаем.
    if _table_exists(conn, "program_zones") and not _has_column(
        conn, "program_zones", "parallel_group"
    ):
        conn.execute("ALTER TABLE program_zones ADD COLUMN parallel_group TEXT")

    # 2) Сезонность упразднена: удаляем легаси-колонки из старых БД.
    #    ALTER TABLE ... DROP COLUMN доступен в SQLite >= 3.35; на более старых
    #    версиях колонки остаются физически (код их не читает и не пишет),
    #    миграция при этом не падает. На новых БД колонок нет — no-op.
    if sqlite3.sqlite_version_info < (3, 35):
        return
    for table, columns in LEGACY_DROP_COLUMNS.items():
        if not _table_exists(conn, table):
            continue
        for column in columns:
            if _has_column(conn, table, column):
                conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
