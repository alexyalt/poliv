"""Этап 6: уведомления с уровнями info/warning/error/critical и дедупликацией.

Паттерны проекта: singleton + init/get/set (как schedule_service); журнал —
таблица logs (source=mqtt/system); роли проверяются на уровне API/веб-маршрутов.

NotificationService.create():
- severity вне списка → 'info' (мягкая деградация вместо исключения);
- dedupe_key: повторное уведомление с тем же ключом НЕ создаётся (возвращается
  id существующего) — защита от лавины одинаковых аварий;
- zone_number разрешается в zone_id по controller_id (zones, миграция 0002).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from ..infra.logging import get_logger

log = get_logger("poliv.notify")

SEVERITIES = ("info", "warning", "error", "critical")


def write_mqtt_log(conn: sqlite3.Connection, action: str, box_id: str,
                  details: dict, source: str = "mqtt") -> None:
    """Журнал системы (таблица logs) — тот же формат, что MqttServerClient.write_log."""
    try:
        with conn:
            conn.execute(
                "INSERT INTO logs(ts, username, action, object_type, object_id,"
                " details_json, source)"
                " VALUES (datetime('now'), NULL, ?, 'controller', ?, ?, ?)",
                (action, box_id, json.dumps(details, ensure_ascii=False), source))
    except Exception:
        log.exception("NOTIFY: не удалось записать журнал (action=%s)", action)


class NotificationService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ------------------------------------------------------------------ создание
    def create(
        self,
        type: str,
        severity: str,
        message: str,
        controller_id: Optional[int] = None,
        zone_id: Optional[int] = None,
        zone_number: Optional[int] = None,
        run_id: Optional[str] = None,
        event_id: Optional[int] = None,
        dedupe_key: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> Optional[int]:
        if severity not in SEVERITIES:
            severity = "info"
        if not message:
            return None

        if dedupe_key:
            exists = self.conn.execute(
                "SELECT id FROM notifications WHERE dedupe_key=?",
                (dedupe_key,)).fetchone()
            if exists:
                return int(exists["id"])

        if zone_id is None and zone_number is not None \
                and controller_id is not None:
            row = self.conn.execute(
                "SELECT id FROM zones WHERE controller_id=? AND zone_number=?",
                (controller_id, int(zone_number))).fetchone()
            if row:
                zone_id = int(row["id"])

        try:
            with self.conn:
                cur = self.conn.execute(
                    """INSERT INTO notifications(
                           type, severity, controller_id, zone_id, run_id,
                           event_id, message, details_json, dedupe_key)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (type, severity, controller_id, zone_id,
                     str(run_id) if run_id is not None else None,
                     event_id, message,
                     json.dumps(details, ensure_ascii=False) if details else None,
                     dedupe_key))
            notif_id = int(cur.lastrowid)
            log.info("NOTIFY: [%s] %s (id=%s)", severity, message, notif_id)
            return notif_id
        except Exception:
            log.exception("NOTIFY: ошибка создания уведомления")
            return None

    # ------------------------------------------------------------------ чтение
    def list(self, unread_only: bool = False, severity: Optional[str] = None,
             limit: int = 50, controller_id: Optional[int] = None) -> list[dict]:
        sql = ("SELECT n.*, c.name AS controller_name, c.box_id "
               "FROM notifications n "
               "LEFT JOIN controllers c ON c.id = n.controller_id WHERE 1=1")
        params: list[Any] = []
        if unread_only:
            sql += " AND n.read_at IS NULL"
        if severity:
            sql += " AND n.severity = ?"
            params.append(severity)
        if controller_id is not None:
            sql += " AND n.controller_id = ?"
            params.append(controller_id)
        sql += " ORDER BY n.id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 500)))
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def unread_count(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) c FROM notifications WHERE read_at IS NULL"
        ).fetchone()
        return int(row["c"]) if row else 0

    def max_unread_critical_id(self) -> int:
        """Для звукового виджета: id самого свежего непрочитанного critical."""
        row = self.conn.execute(
            """SELECT COALESCE(MAX(id), 0) m FROM notifications
               WHERE severity='critical' AND read_at IS NULL"""
        ).fetchone()
        return int(row["m"]) if row else 0

    def mark_read(self, notif_id: int) -> bool:
        with self.conn:
            cur = self.conn.execute(
                "UPDATE notifications SET read_at=datetime('now')"
                " WHERE id=? AND read_at IS NULL", (notif_id,))
        return cur.rowcount > 0

    def mark_all_read(self) -> int:
        with self.conn:
            cur = self.conn.execute(
                "UPDATE notifications SET read_at=datetime('now')"
                " WHERE read_at IS NULL")
        return cur.rowcount


# ---------------------------------------------------------------------- singleton
_instance: Optional[NotificationService] = None


def init_notification_service(conn: sqlite3.Connection) -> NotificationService:
    global _instance
    _instance = NotificationService(conn)
    return _instance


def get_notification_service() -> Optional[NotificationService]:
    return _instance


def set_notification_service(svc: Optional[NotificationService]) -> None:
    global _instance
    _instance = svc
