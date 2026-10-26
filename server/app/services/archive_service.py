"""Immutable archive batches with typed, checksummed companion manifests.

New exports use YYYY-MM-DD_<batch>.csv|json under data_root/archive.path/type.
Source deletion and journal insertion share one write transaction; durable files
precede deletion. Legacy daily files stay readable, with incomplete relations
explicitly reported. See docs/adr/ADR-18-stage8-stabilization.md.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from ..infra.db import connect_path
from ..infra.logging import get_logger
from .flow_service import get_setting
from . import archive_integrity as integrity

log = get_logger("poliv.archive")

ENTITY_TYPES = ("events", "flow", "notifications", "weather", "logs")

DEFAULTS = {
    "archive.enabled": True,
    "archive.db_retention_days": 365,
    "archive.path": "archive",
    "archive.events_format": "csv",
}


# --------------------------------------------------------------- настройки
def archive_config(conn: sqlite3.Connection) -> dict[str, Any]:
    """Настройки archive.* из таблицы settings (миграция 0010) со
    значениями по умолчанию."""
    return {
        "enabled": bool(get_setting(conn, "archive.enabled",
                                    DEFAULTS["archive.enabled"])),
        "retention_days": max(1, int(get_setting(
            conn, "archive.db_retention_days",
            DEFAULTS["archive.db_retention_days"]))),
        "path": str(get_setting(conn, "archive.path",
                                DEFAULTS["archive.path"])),
        "events_format": str(get_setting(
            conn, "archive.events_format",
            DEFAULTS["archive.events_format"])),
    }


def utc_date(days_ago: int = 0) -> str:
    d = datetime.now(timezone.utc).date() - timedelta(days=days_ago)
    return d.strftime("%Y-%m-%d")


def _day_bounds(target_date: str) -> tuple[int, int]:
    """unix-границы UTC-суток target_date (для числовых ts-полей)."""
    lo = int(datetime.strptime(target_date, "%Y-%m-%d")
             .replace(tzinfo=timezone.utc).timestamp())
    return lo, lo + 86400 - 1


# ------------------------------------------------------------- выборки
def _fetch_rows(conn: sqlite3.Connection, entity_type: str,
                cutoff_ts: int, cutoff_iso: str,
                target_date: Optional[str]) -> list[sqlite3.Row]:
    """Строки entity_type, подлежащие выгрузке.

    target_date=None — всё старше cutoff (стандартный режим ротации);
    target_date=YYYY-MM-DD — только строки этой даты (ручная выгрузка /
    тесты; cutoff тогда игнорируется).
    """
    if target_date:
        lo, hi = _day_bounds(target_date)
    else:
        lo, hi = None, None

    if entity_type == "events":
        q = "SELECT * FROM watering_events WHERE "
        if target_date:
            q += "start_ts BETWEEN ? AND ?"
            args: tuple = (lo, hi)
        else:
            q += "start_ts < ?"
            args = (cutoff_ts,)
        return conn.execute(q + " ORDER BY start_ts, id LIMIT 1000", args).fetchall()
    if entity_type == "flow":
        q = "SELECT * FROM flow_daily WHERE "
        if target_date:
            q += "date = ?"
            args = (target_date,)
        else:
            q += "date < ?"
            args = (datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
                    .strftime("%Y-%m-%d"),)
        return conn.execute(q + " ORDER BY date, controller_id, zone_id LIMIT 1000",
                            args).fetchall()
    if entity_type == "notifications":
        q = "SELECT * FROM notifications WHERE "
        if target_date:
            q += "date(created_at) = ?"
            args = (target_date,)
        else:
            q += "created_at < ?"
            args = (cutoff_iso,)
        return conn.execute(q + " ORDER BY created_at, id LIMIT 1000", args).fetchall()
    if entity_type == "weather":
        q = "SELECT * FROM weather_observations WHERE "
        if target_date:
            q += "ts BETWEEN ? AND ?"
            args = (lo, hi)
        else:
            q += "ts < ?"
            args = (cutoff_ts,)
        return conn.execute(q + " ORDER BY ts, id LIMIT 1000", args).fetchall()
    if entity_type == "logs":
        q = "SELECT * FROM logs WHERE "
        if target_date:
            q += "date(ts) = ?"
            args = (target_date,)
        else:
            q += "ts < ?"
            args = (cutoff_iso,)
        return conn.execute(q + " ORDER BY ts, id LIMIT 1000", args).fetchall()
    raise ValueError(f"Неизвестный тип архивации: {entity_type}")


_DELETE_SQL = {
    "events": "DELETE FROM watering_events WHERE id IN ({})",
    "flow": "DELETE FROM flow_daily WHERE id IN ({})",
    "notifications": "DELETE FROM notifications WHERE id IN ({})",
    "weather": "DELETE FROM weather_observations WHERE id IN ({})",
    "logs": "DELETE FROM logs WHERE id IN ({})",
}

_INSERT_SQL = {
    # Обратный импорт (restore_export): те же колонки, что SELECT *.
    "events": ("watering_events",
               "INSERT OR IGNORE INTO watering_events({cols}) VALUES ({ph})"),
    "notifications": ("notifications",
                      "INSERT OR IGNORE INTO notifications({cols}) VALUES ({ph})"),
    "logs": ("logs",
             "INSERT OR IGNORE INTO logs({cols}) VALUES ({ph})"),
}


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _serialize(rows: list[dict[str, Any]], fmt: str) -> tuple[str, list[str]]:
    """Содержимое файла + заголовок колонок. fmt: csv | json."""
    header = list(rows[0].keys()) if rows else []
    if fmt == "json":
        return json.dumps(rows, ensure_ascii=False, default=str), header
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if header:
        w.writerow(header)
        for r in rows:
            w.writerow([r.get(c) for c in header])
    return buf.getvalue(), header


# ------------------------------------------------------------------ ядро
class ArchiveService:
    """Ротация оперативной БД в файловый архив (ADR-14)."""

    def __init__(self, conn: sqlite3.Connection, data_root: str | Path):
        self.conn = conn
        self.data_root = Path(data_root)

    # ---- выгрузка -------------------------------------------------------
    def rotate(self, entity_type: str, *, target_date: Optional[str] = None,
               dry_run: bool = False) -> dict[str, Any]:
        """Write an immutable v2 batch before deleting its exact source rows.

        BEGIN IMMEDIATE serializes rotation across processes. On any failure the
        source rows and journal roll back; completed unregistered files are copies
        only and must never be auto-restored. See ADR-18 for recovery rules.
        """
        if entity_type not in ENTITY_TYPES:
            raise ValueError(f"Unknown entity_type: {entity_type}")
        with integrity.transaction(self.conn) as conn:
            cfg = archive_config(conn)
            cutoff = datetime.now(timezone.utc) - timedelta(days=cfg["retention_days"])
            rows = [_dict(r) for r in _fetch_rows(
                conn, entity_type, int(cutoff.timestamp()),
                cutoff.strftime("%Y-%m-%d %H:%M:%S"), target_date)]
            file_date = utc_date()
            result = {"entity_type": entity_type, "date": target_date or file_date,
                      "row_count": len(rows), "dry_run": dry_run,
                      "file_path": None, "file_hash": None}
            if dry_run or not rows:
                return result
            fmt = cfg["events_format"] if entity_type == "events" else "csv"
            if fmt not in ("csv", "json"):
                raise ValueError("Unknown archive format")
            content, _ = _serialize(rows, fmt)
            directory = self.data_root / cfg["path"] / entity_type
            directory.mkdir(parents=True, exist_ok=True)
            batch_id = uuid.uuid4().hex
            path = directory / f"{file_date}_{batch_id}.{fmt}"
            data = content.encode("utf-8")
            children = []
            ids = [r["id"] for r in rows]
            if entity_type == "events":
                for offset in range(0, len(ids), 500):
                    part = ids[offset:offset + 500]
                    ph = ",".join("?" * len(part))
                    children.extend(_dict(r) for r in conn.execute(
                        f"SELECT * FROM watering_event_zones WHERE event_id IN ({ph}) ORDER BY id",
                        part).fetchall())
            manifest = {"schema_version": 2, "batch_id": batch_id,
                        "entity_type": entity_type, "file_hash": integrity.digest(data),
                        "row_count": len(rows), "target_date": target_date,
                        "nulls": [[k for k, v in row.items() if v is None] for row in rows],
                        "event_zones": children}
            meta = json.dumps(manifest, ensure_ascii=False, allow_nan=False).encode("utf-8")
            integrity.durable_write(path, data)
            integrity.durable_write(path.with_suffix(path.suffix + ".manifest.json"), meta)
            # Files and their directory entries are durable before deletion.
            for offset in range(0, len(ids), 500):
                part = ids[offset:offset + 500]
                ph = ",".join("?" * len(part))
                if entity_type == "events":
                    conn.execute(f"DELETE FROM watering_event_zones WHERE event_id IN ({ph})", part)
                conn.execute(_DELETE_SQL[entity_type].format(ph), part)
            conn.execute(
                """INSERT INTO archive_exports(entity_type,date,file_path,row_count,
                       file_hash,created_at,format_version,manifest_hash)
                   VALUES (?,?,?,?,?,datetime('now'),2,?)""",
                (entity_type, file_date, str(path.resolve()), len(rows),
                 integrity.digest(data), integrity.digest(meta)))
            result.update(file_path=str(path.resolve()), file_hash=integrity.digest(data))
            return result

    def rotate_all(self, *, dry_run: bool = False,
                   target_date: Optional[str] = None) -> list[dict[str, Any]]:
        """Пройти по всем типам сущностей. Штатный вызов из ArchiveDaemon —
        dry_run=False, target_date=None (всё старше среза)."""
        out = []
        for et in ENTITY_TYPES:
            try:
                for _ in range(100):
                    batch = self.rotate(et, target_date=target_date, dry_run=dry_run)
                    out.append(batch)
                    if dry_run or batch["row_count"] < 1000:
                        break
            except Exception as exc:  # один тип не роняет весь цикл
                log.exception("ARCHIVE: rotate(%s) упал", et)
                out.append({"entity_type": et, "error": str(exc)})
        return out

    # ---- журнал ---------------------------------------------------------
    def list_exports(self, entity_type: Optional[str] = None,
                     limit: int = 100) -> list[dict[str, Any]]:
        q = ("SELECT * FROM archive_exports"
             + (" WHERE entity_type=?" if entity_type else "")
             + " ORDER BY created_at DESC, id DESC LIMIT ?")
        args: tuple = ((entity_type, limit) if entity_type else (limit,))
        return [_dict(r) for r in self.conn.execute(q, args).fetchall()]

    # ---- восстановление ---------------------------------------------------
    def restore_export(self, export_id: int) -> dict[str, Any]:
        """Validate v2 checksums, types and relations before transactional restore."""
        with integrity.transaction(self.conn) as conn:
            row = conn.execute("SELECT * FROM archive_exports WHERE id=?", (export_id,)).fetchone()
            if row is None:
                raise KeyError(f"Export {export_id} not found")
            entity = row["entity_type"]
            if entity not in integrity.TABLES:
                raise ValueError(f"Unknown archive entity: {entity}")
            records, manifest = integrity.read_records(conn, row)
            restored = integrity.restore_records(conn, entity, records, manifest)
            return {"export_id": export_id, "entity_type": entity, "restored": restored,
                    "legacy_incomplete": row["format_version"] == 1}


def _dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


# ---------------------------------------------------- фоновый демон (ADR-14)
class ArchiveDaemon(threading.Thread):
    """Daemon-поток: раз в сутки в 03:00 локального времени — rotate_all().

    Аналог WeatherUpdater (Этап 7): own-соединение к БД, ошибки сети/БД не
    убивают поток, stop() через Event. Проверка расписания — каждые 60 с;
    если час >= 03 и сегодняшняя ротация ещё не выполнялась — запускаем
    (флаг последней даты ротации хранится в памяти процесса).
    """

    RUN_HOUR = 3

    def __init__(self, db_path: str, data_root: str | Path, *,
                 check_interval_sec: float = 60.0,
                 initial_delay_sec: float = 5.0):
        super().__init__(daemon=True, name="poliv-archive-daemon")
        self.db_path = str(db_path)
        self.data_root = str(data_root)
        self._stopev = threading.Event()
        self._check_interval = check_interval_sec
        self._initial_delay = initial_delay_sec
        self.last_run_date: Optional[str] = None

    def run(self) -> None:
        if self._stopev.wait(self._initial_delay):
            return
        while not self._stopev.is_set():
            try:
                self.tick_once()
            except Exception:
                log.exception("ARCHIVE: цикл демона упал — продолжаем")
            if self._stopev.wait(self._check_interval):
                return

    def tick_once(self) -> Optional[list[dict[str, Any]]]:
        """Один шаг планировщика: если наступило 03:00 сегодня и ещё не
        запускали — выполнить rotate_all(). Вынесено для тестируемости."""
        now = datetime.now()  # локальное время ОС — как в ТЗ (§ «03:00 локального»)
        today = now.strftime("%Y-%m-%d")
        if now.hour < self.RUN_HOUR or self.last_run_date == today:
            return None
        conn = connect_path(self.db_path)
        try:
            cfg = archive_config(conn)
            if not cfg["enabled"]:
                self.last_run_date = today
                return None
            svc = ArchiveService(conn, self.data_root)
            results = svc.rotate_all()
            self.last_run_date = today
            return results
        finally:
            conn.close()

    def stop(self) -> None:
        self._stopev.set()


_daemon: Optional[ArchiveDaemon] = None
_daemon_lock = threading.Lock()


def start_archive_daemon(db_path: str | Path,
                         data_root: str | Path) -> Optional[ArchiveDaemon]:
    """Lifespan main.py: один фоновый поток на процесс (как погода)."""
    global _daemon
    with _daemon_lock:
        if _daemon is not None and _daemon.is_alive():
            return _daemon
        _daemon = ArchiveDaemon(str(db_path), str(data_root))
        _daemon.start()
        return _daemon


def stop_archive_daemon() -> None:
    global _daemon
    with _daemon_lock:
        if _daemon is not None:
            _daemon.stop()
            _daemon = None
