"""Этап 8, блок 8.2: файловый архив и ротация оперативной БД (ADR-14).

Правила архивации (Артефакт 0.7 §23):
- строки старше archive.db_retention_days выгружаются в файлы
  {data_root}/{archive.path}/{entity_type}/YYYY-MM-DD.csv|json — по одному
  файлу на тип сущности за дату выгрузки; если файл за эту дату уже есть,
  пачка дописывается без повторного заголовка CSV;
- после успешной записи файла строки удаляются из БД (одна транзакция);
- журнал выгрузок — таблица archive_exports (миграция 0010): путь,
  число строк, sha256 файла;
- dry_run=True ничего не пишет и не удаляет (только подсчёт);
- restore_export — обратный импорт из файла архива (реализован для
  events/notifications/logs; flow/weather — заглушка NotImplementedError).

Фоновая задача ArchiveDaemon — daemon-поток (аналог WeatherUpdater,
Этап 7): раз в сутки в 03:00 локального времени вызывает rotate_all().

Таймзоны: датировка файлов — UTC-дата (каноническая суточная зона проекта,
совпадает с flow_daily и report_service._day_expr).
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from ..infra.logging import get_logger
from .flow_service import get_setting

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
        return conn.execute(q + " ORDER BY start_ts, id", args).fetchall()
    if entity_type == "flow":
        q = "SELECT * FROM flow_daily WHERE "
        if target_date:
            q += "date = ?"
            args = (target_date,)
        else:
            q += "date < ?"
            args = (datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
                    .strftime("%Y-%m-%d"),)
        return conn.execute(q + " ORDER BY date, controller_id, zone_id",
                            args).fetchall()
    if entity_type == "notifications":
        q = "SELECT * FROM notifications WHERE "
        if target_date:
            q += "date(created_at) = ?"
            args = (target_date,)
        else:
            q += "created_at < ?"
            args = (cutoff_iso,)
        return conn.execute(q + " ORDER BY created_at, id", args).fetchall()
    if entity_type == "weather":
        q = "SELECT * FROM weather_observations WHERE "
        if target_date:
            q += "ts BETWEEN ? AND ?"
            args = (lo, hi)
        else:
            q += "ts < ?"
            args = (cutoff_ts,)
        return conn.execute(q + " ORDER BY ts, id", args).fetchall()
    if entity_type == "logs":
        q = "SELECT * FROM logs WHERE "
        if target_date:
            q += "date(ts) = ?"
            args = (target_date,)
        else:
            q += "ts < ?"
            args = (cutoff_iso,)
        return conn.execute(q + " ORDER BY ts, id", args).fetchall()
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
        """Выгрузить строки entity_type в файл архива и удалить из БД.

        target_date=None — штатный режим: всё старше retention cutoff.
        target_date=YYYY-MM-DD — выгрузить конкретные сутки (ружная
        операция/тесты). Возвращает {"entity_type","date","file_path",
        "row_count","file_hash","dry_run"}; при 0 строк файл НЕ создаётся
        (кроме dry_run) и журнал не пишется — это делает повторный rotate
        за ту же дату идемпотентным.
        """
        if entity_type not in ENTITY_TYPES:
            raise ValueError(f"Неизвестный тип архивации: {entity_type}")
        cfg = archive_config(self.conn)
        now = datetime.now(timezone.utc)
        cutoff_ts = int((now - timedelta(days=cfg["retention_days"]))
                        .timestamp())
        cutoff_iso = datetime.fromtimestamp(cutoff_ts,
                                            tz=timezone.utc).strftime(
                                                "%Y-%m-%d %H:%M:%S")
        rows = [_dict(r) for r in _fetch_rows(
            self.conn, entity_type, cutoff_ts, cutoff_iso, target_date)]
        # Имя файла — по дню ВЫГРУЗКИ (ADR-14: одна пачка на сутки работы
        # архиватора), а не по target_date: несколько ручных rotate за
        # разные сутки пишутся в один файл дня (CSV дописывается без
        # повторного заголовка). В колонке date журнала — target_date.
        file_date = utc_date()
        result: dict[str, Any] = {
            "entity_type": entity_type, "date": target_date or file_date,
            "row_count": len(rows), "dry_run": dry_run,
            "file_path": None, "file_hash": None,
        }
        if dry_run:
            return result
        if not rows:
            return result  # идемпотентность: нечего выгружать — пусто

        fmt = cfg["events_format"] if entity_type == "events" else "csv"
        content, _hdr = _serialize(rows, fmt)
        rel_dir = self.data_root / cfg["path"] / entity_type
        rel_dir.mkdir(parents=True, exist_ok=True)
        ext = "json" if fmt == "json" else "csv"
        fpath = rel_dir / f"{file_date}.{ext}"
        appending = fpath.exists() and fmt != "json"
        with open(fpath, "a", encoding="utf-8") as fh:
            if appending:
                # дописываем пачку без повторного заголовка CSV
                body, _ = _serialize(rows, "csv")
                payload = body.split("\n", 1)[1] if "\n" in body else ""
            else:
                payload = content
            fh.write(payload)
        # hash — по содержимому конкретной выгрузки (при дописывании в
        # существующий файл дня — hash дописанной пачки).
        file_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

        # Удаление выгруженного — в одной транзакции с журналом.
        ids = [r["id"] for r in rows]
        with self.conn:
            placeholders = ",".join("?" * len(ids))
            sql = _DELETE_SQL[entity_type].format(placeholders)
            # Каскад watering_event_zones — через явное удаление (в 0008 нет
            # ON DELETE CASCADE; FK включены — сначала дочерние строки).
            if entity_type == "events":
                self.conn.execute(
                    "DELETE FROM watering_event_zones WHERE event_id IN (%s)"
                    % placeholders, ids)
            self.conn.execute(sql, ids)
            self.conn.execute(
                """INSERT INTO archive_exports(entity_type, date, file_path,
                       row_count, file_hash, created_at)
                   VALUES (?,?,?,?,?,datetime('now'))""",
                (entity_type, file_date, str(fpath), len(rows), file_hash))
        log.info("ARCHIVE: %s за %s — %d строк → %s", entity_type,
                 file_date, len(rows), fpath)
        result["file_path"] = str(fpath)
        result["file_hash"] = file_hash
        return result

    def rotate_all(self, *, dry_run: bool = False,
                   target_date: Optional[str] = None) -> list[dict[str, Any]]:
        """Пройти по всем типам сущностей. Штатный вызов из ArchiveDaemon —
        dry_run=False, target_date=None (всё старше среза)."""
        out = []
        for et in ENTITY_TYPES:
            try:
                out.append(self.rotate(et, target_date=target_date,
                                       dry_run=dry_run))
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
        """Обратный импорт строк из файла выгрузки в оперативную БД.

        Реализовано для events/notifications/logs (INSERT OR IGNORE по id —
        повторный импорт безопасен). Для flow/weather на этом этапе —
        заглушка NotImplementedError (осмысленное отложенное: эти данные
        восстанавливаются пересчётом/источником).
        """
        row = self.conn.execute(
            "SELECT * FROM archive_exports WHERE id=?",
            (export_id,)).fetchone()
        if row is None:
            raise KeyError(f"Выгрузка {export_id} не найдена")
        entity = row["entity_type"]
        if entity not in _INSERT_SQL:
            raise NotImplementedError(
                f"restore_export для '{entity}' отложен (Этап 8: только "
                "events/notifications/logs)")
        path = Path(row["file_path"])
        if not path.exists():
            raise FileNotFoundError(str(path))
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".json":
            records = json.loads(text)
        else:
            records = list(csv.DictReader(io.StringIO(text)))
        tmpl = _INSERT_SQL[entity][1]
        restored = 0
        with self.conn:
            for rec in records:
                cols = list(rec.keys())
                before = self.conn.total_changes
                self.conn.execute(
                    tmpl.format(cols=",".join(cols),
                                ph=",".join("?" * len(cols))),
                    tuple(rec[c] for c in cols))
                if self.conn.total_changes > before:
                    restored += 1
        log.info("ARCHIVE: restore #%d (%s) — %d записей", export_id,
                 entity, restored)
        return {"export_id": export_id, "entity_type": entity,
                "restored": restored}


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
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
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
