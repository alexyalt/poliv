"""Инициализация SQLite и механизм миграций (Этап 1).

Правила из ТЗ:
- SQLite, режим WAL, внешние ключи включены;
- каждая миграция имеет номер и описание (.py + .md);
- миграции применяются последовательно;
- перед первой миграцией на новой базе создаётся пустая база в data/.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .config import PROJECT_ROOT, Config
from .logging import get_logger

log = get_logger("poliv.db")

MIGRATIONS_DIR = PROJECT_ROOT / "server" / "migrations"


def connect(cfg: Config) -> sqlite3.Connection:
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cfg.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if cfg.raw.get("database", {}).get("wal", True):
        conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r["name"] for r in rows}


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if table not in _table_names(conn):
        return False
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(c["name"] == column for c in cols)


def migrate(conn: sqlite3.Connection) -> list[str]:
    """Применяет отсутствующие миграции. Возвращает список применённых."""
    applied_before = {
        r["version"]
        for r in (
            conn.execute("SELECT version FROM schema_migrations").fetchall()
            if "schema_migrations" in _table_names(conn)
            else []
        )
    }

    files = sorted(MIGRATIONS_DIR.glob("0*.py"))
    applied: list[str] = []
    for path in files:
        version = path.stem  # например 0001_initial
        if version in applied_before:
            continue
        log.info("Применяю миграцию %s", version)
        module = _load_module(path)
        with conn:  # транзакция
            module.upgrade(conn)
            if "schema_migrations" in _table_names(conn):
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (version, datetime.now(timezone.utc).isoformat()),
                )
        applied.append(version)
        log.info("Миграция %s применена", version)
    return applied


def _load_module(path: Path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(f"migration_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def init_db(cfg: Config) -> sqlite3.Connection:
    """Открывает базу, прогоняет миграции, проверяет целостность схемы."""
    conn = connect(cfg)
    applied = migrate(conn)
    if applied:
        log.info("База %s: применено миграций: %d", cfg.db_path, len(applied))
    result = conn.execute("PRAGMA integrity_check").fetchone()
    if result[0] != "ok":
        raise RuntimeError(f"Проверка целостности БД не пройдена: {result[0]}")
    return conn
