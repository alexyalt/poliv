"""Миграция 0002_stage2_catalogs — справочники и программы (Этап 2).

Состав:
- controllers: признак soft-disable (deleted_at) + поле модели устройства;
- zones: блокировки зон (на уровне таблицы,
  по ТЗ Этапа 2 «будущие блокировки зон хотя бы на уровне таблицы»);
  сезонности у зон НЕТ (правки v1/v2 Этапа 2: «убрать дату сезона вообще»,
  п. 3.5/3.10 — поля season_start/season_end из схемы удалены);
- programs: программы полива (тип расписания: дни недели или интервал,
  время старта, включение/отключение; сезонности нет — п. 4.1/4.5);
- program_zones: порядок зон внутри программы (нумерация с 1);
- zone_locks: журнал блокировок (включая ручные поливы как будущие блокировки);
- settings: ключи корректировок полива (значения задаёт администратор через UI).

Правила из ТЗ:
- зоны не удаляются, только отключаются (soft-disable через deleted_at);
- номер зоны уникален в пределах контроллера (уникальность распространяется
  и на занятые номера удалённых зон — UNIQUE(controller_id, zone_number));
- программа может быть без зон и содержать зоны разных контроллеров.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SQL = [
    """
    CREATE TABLE IF NOT EXISTS programs (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        name           TEXT NOT NULL UNIQUE,
        description    TEXT,
        enabled        INTEGER NOT NULL DEFAULT 1,
        schedule_type  TEXT NOT NULL CHECK (schedule_type IN ('weekdays','interval')),
        weekdays_mask  TEXT NOT NULL DEFAULT '0000000',  -- пн..вс, '1' = день активен
        interval_days  INTEGER,                          -- для schedule_type='interval'
        start_time     TEXT NOT NULL DEFAULT '06:00',    -- HH:MM локального времени
        created_at     TEXT NOT NULL,
        updated_at     TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS program_zones (
        program_id  INTEGER NOT NULL REFERENCES programs(id) ON DELETE CASCADE,
        zone_id     INTEGER NOT NULL REFERENCES zones(id),
        seq         INTEGER NOT NULL,   -- порядок зоны в программе (1..)
        duration_override_minutes INTEGER,  -- NULL = базовая длительность зоны
        parallel_group TEXT,            -- ADR-12: группы с одинаковым именем стартуют
                                        -- параллельно; NULL = последовательный запуск
        PRIMARY KEY (program_id, seq),
        UNIQUE (program_id, zone_id)
    )
    """,
    # Правки v1/v2 Этапа 2 (п. 3.5/3.10, 4.1/4.5): сезонность убрана из системы —
    # колонок season_start/season_end в новых БД нет; в старых они удаляются
    # функцией _drop_legacy_season_columns() ниже.
    "CREATE INDEX IF NOT EXISTS idx_pz_zone ON program_zones(zone_id)",
    """
    CREATE TABLE IF NOT EXISTS zone_locks (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        zone_id     INTEGER NOT NULL REFERENCES zones(id),
        kind        TEXT NOT NULL CHECK (kind IN ('manual_watering','maintenance','other')),
        reason      TEXT,
        locked_by   TEXT,
        locked_at   TEXT NOT NULL,
        unlock_at   TEXT,
        active      INTEGER NOT NULL DEFAULT 1
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_zl_zone ON zone_locks(zone_id, active)",
]

# columns, which may be absent in old DBs: (table, column, ddl)
ADDITIONS = [
    ("controllers", "model", "TEXT"),
    # soft-disable: NULL = активная запись, ISO-дата = «удалена» (не удаляется физически)
    ("controllers", "deleted_at", "TEXT"),
    ("zones", "deleted_at", "TEXT"),
    # Правки v1/v2 Этапа 2: сезонность (season_start/season_end) НЕ добавляем —
    # дата сезона убрана из системы по требованию (п. 3.5/3.10, 4.1/4.5).
    # ADR-12: параллельные зоны (для уже существующих БД, созданных до правки)
    ("program_zones", "parallel_group", "TEXT"),
]

# Легаси-колонки, которые нужно удалить из старых БД (сезонность упразднена).
LEGACY_DROP_COLUMNS = {
    "zones": ("season_start", "season_end"),
    "programs": ("season_start", "season_end"),
}


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(
        c["name"] == column
        for c in conn.execute(f"PRAGMA table_info({table})").fetchall()
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _drop_legacy_season_columns(conn: sqlite3.Connection) -> None:
    """Удаляет season_start/season_end из старых БД (правка «убрать дату сезона»).

    SQLite >= 3.35 поддерживает ALTER TABLE ... DROP COLUMN; на более старых
    версиях колонки остаются физически (код их больше не читает и не пишет —
    без влияния на работу), поэтому миграция не падает.
    """
    try:
        if sqlite3.sqlite_version_info < (3, 35):
            return
    except AttributeError:  # pragma: no cover
        return
    for table, columns in LEGACY_DROP_COLUMNS.items():
        if not _table_exists(conn, table):
            continue
        for column in columns:
            if _has_column(conn, table, column):
                conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")


def upgrade(conn: sqlite3.Connection) -> None:
    now = datetime.now(timezone.utc).isoformat()
    for statement in SQL:
        conn.execute(statement)
    for table, column, ddl in ADDITIONS:
        if not _has_column(conn, table, column):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    _drop_legacy_season_columns(conn)
    # Настройки корректировок: значения по умолчанию задаются здесь,
    # дальше управляются администратором из раздела «Настройки».
    for key, value_json, desc in [
        ("adjustment.rain_delay_hours", "24", "Задержка полива после дождя, часы"),
        ("adjustment.temp_factor_min", "0.5", "Множитель длительности при жаре/холоде, мин."),
        ("adjustment.temp_factor_max", "2.0", "Максимальный множитель длительности"),
        ("adjustment.soak_default_enabled", "false", "Cycle&soak по умолчанию для новых зон"),
    ]:
        conn.execute(
            """INSERT OR IGNORE INTO settings(key, value_json, description, updated_at, updated_by)
               VALUES (?, ?, ?, ?, 'system')""",
            (key, value_json, desc, now),
        )
