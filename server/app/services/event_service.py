"""Этап 6: приём, дедупликация и запись событий полива (ТЗ §9.2).

Контракт входа — сообщение топика poliv/{box_id}/event (Артефакт 0.5 §7,
схемы started/finished/stopped валидируются в mqtt_client._validate_event_payload):
- event_uid уникален; повтор того же event_uid — дубликат, игнорируется;
- run_id — TEXT (как watering_runs.run_id, миграция 0007); у manual-прогонов
  идентификатор прогона на сервере — event_uid события started (финиш ссылается
  через started_event_uid) — та же методика, что ScheduleService.on_event;
- finished/stopped несут объём и длительность — по ним строятся дневные
  агрегаты расхода (FlowService.apply_event_to_daily) и аварийная обработка;
- статусы событий: started | finished | stopped (ТЗ §3.11). Аварийный финал
  помечается reason_code (no_flow / over_flow / under_flow / emergency_stop)
  либо aborted=1 без кода → emergency_stop.

Интеграция: mqtt_client._handle_event вызывает handle_event() после валидации
(до/независимо от ScheduleService.on_event — watering_runs остаётся зоной
ответственности Этапа 4).
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from ..infra.logging import get_logger

log = get_logger("poliv.event")

# Статусы событий (ТЗ §3.11) — ровно те, что принимает валидатор MQTT.
EVENT_STATUSES = {"started", "finished", "stopped"}

# Аварийные причины (Этап 6): финиш с одним из этих кодов — авария потока.
EMERGENCY_REASONS = {"no_flow", "over_flow", "under_flow", "emergency_stop"}


class EventService:
    """Приём и сохранение событий полива с дедупликацией по event_uid."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ------------------------------------------------------------------ вход
    def handle_event(self, box_id: str, p: dict[str, Any]) -> Optional[int]:
        """Сохранить событие из топика event. Возвращает id записи или None
        (дубликат / неизвестный контроллер / невалидный payload)."""
        if not self._require_box(box_id):
            return None
        status = p.get("status")
        if status not in EVENT_STATUSES:
            log.warning("EVENT: неизвестный статус %r от %s — отброшено",
                        status, box_id)
            return None
        event_uid = p.get("event_uid")
        if not event_uid:
            log.warning("EVENT: от %s без event_uid — отброшено", box_id)
            return None

        ctrl = self.conn.execute(
            "SELECT id FROM controllers WHERE box_id=?", (box_id,)).fetchone()
        if ctrl is None:                      # гонка: контроллер удалили
            return None
        controller_id = int(ctrl["id"])

        # Дедупликация по event_uid (ТЗ §9.2): повтор — тихо игнорируем.
        exists = self.conn.execute(
            "SELECT id FROM watering_events WHERE event_uid=?",
            (str(event_uid),)).fetchone()
        if exists:
            log.info("EVENT: дубликат %s проигнорирован", event_uid)
            return None

        zones = [z for z in (p.get("active_zones") or [])
                 if isinstance(z, int) and not isinstance(z, bool)]
        run_id = self._resolve_run_id(p, status)
        primary_zone_id = self._resolve_zone(
            controller_id, p.get("primary_zone") or (zones[0] if zones else None))
        reason_code = self._resolve_reason(p, status)
        wall_sec = p.get("wall_sec")
        if wall_sec is None and p.get("end_ts") is not None \
                and p.get("start_ts") is not None:
            wall_sec = max(0, int(p["end_ts"]) - int(p["start_ts"]))

        try:
            with self.conn:
                cur = self.conn.execute(
                    """INSERT INTO watering_events(
                           event_uid, started_event_uid, run_id, controller_id,
                           primary_zone_id, parallel, source, status, start_ts,
                           end_ts, water_sec, wall_sec, volume_liters,
                           expected_flow_lpm, reason_code, aborted,
                           schedule_version, buffered)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        str(event_uid),
                        str(p["started_event_uid"]) if p.get("started_event_uid") else None,
                        run_id,
                        controller_id,
                        primary_zone_id,
                        1 if len(zones) > 1 else 0,
                        str(p.get("source") or "schedule"),
                        status,
                        int(p.get("start_ts") or 0),
                        int(p["end_ts"]) if p.get("end_ts") is not None else None,
                        int(p.get("water_sec") or 0),
                        int(wall_sec) if wall_sec is not None else None,
                        float(p["volume_liters"]) if p.get("volume_liters") is not None else None,
                        float(p["expected_flow_lpm"]) if p.get("expected_flow_lpm") is not None else None,
                        reason_code,
                        int(bool(p.get("aborted"))) if p.get("aborted") is not None else None,
                        p.get("schedule_version") if isinstance(p.get("schedule_version"), int) else None,
                        1 if p.get("buffered") else 0,
                    ),
                )
                event_id = int(cur.lastrowid)
                for zone_num in zones:
                    zone_id = self._resolve_zone(controller_id, zone_num)
                    if zone_id is None:
                        continue
                    self.conn.execute(
                        """INSERT OR IGNORE INTO watering_event_zones(
                               event_id, zone_id, water_sec, volume_liters,
                               expected_flow_lpm)
                           VALUES (?,?,?,?,?)""",
                        (event_id, zone_id,
                         int(p.get("water_sec") or 0),
                         float(p["volume_liters"]) if p.get("volume_liters") is not None else None,
                         float(p["expected_flow_lpm"]) if p.get("expected_flow_lpm") is not None else None),
                    )
        except sqlite3.IntegrityError:
            # Гонка двух копий одного event_uid — дубликат, не ошибка.
            log.info("EVENT: гонка дубликата %s — проигнорирована", event_uid)
            return None
        except Exception:
            log.exception("EVENT: ошибка записи события %s", event_uid)
            return None

        log.info("EVENT: сохранено id=%s uid=%s status=%s water_sec=%s volume=%s",
                 event_id, event_uid, status, p.get("water_sec"),
                 p.get("volume_liters"))

        flow = get_flow_service_ref()
        if flow is not None:
            try:
                flow.on_event_saved(event_id)
            except Exception:
                log.exception("EVENT: FlowService.on_event_saved упал для %s",
                              event_uid)
        return event_id

    # ------------------------------------------------------------------ helpers
    def _require_box(self, box_id: str) -> bool:
        row = self.conn.execute(
            "SELECT id FROM controllers WHERE box_id=?", (box_id,)).fetchone()
        if row is None:
            log.warning("EVENT: неизвестный box_id=%s — игнорируется", box_id)
            return False
        return True

    @staticmethod
    def _resolve_run_id(p: dict, status: str) -> Optional[str]:
        """run_id прогона (TEXT): schedule — из payload; manual — event_uid
        старта (финиш ссылается через started_event_uid). Как в Этапе 4."""
        if p.get("run_id"):
            return str(p["run_id"])
        if status == "started":
            return str(p.get("event_uid") or "") or None
        return str(p.get("started_event_uid") or p.get("event_uid") or "") or None

    @staticmethod
    def _resolve_reason(p: dict, status: str) -> Optional[str]:
        """Причина завершения — строка (no_flow/over_flow/under_flow/
        emergency_stop/manual_stop). aborted=1 без кода → emergency_stop."""
        rc = p.get("reason_code")
        if isinstance(rc, str) and rc.strip():
            return rc.strip().lower()
        if status == "stopped" and p.get("aborted"):
            return "emergency_stop"
        return None

    def _resolve_zone(self, controller_id: int, zone_num: Any) -> Optional[int]:
        if zone_num is None:
            return None
        try:
            num = int(zone_num)
        except (TypeError, ValueError):
            return None
        row = self.conn.execute(
            "SELECT id FROM zones WHERE controller_id=? AND zone_number=?",
            (controller_id, num)).fetchone()
        return int(row["id"]) if row else None

    # ------------------------------------------------------------------ запросы
    def recent_events(self, limit: int = 50, controller_id: Optional[int] = None,
                      status: Optional[str] = None) -> list[dict]:
        sql = ("SELECT e.*, c.name AS controller_name, c.box_id "
               "FROM watering_events e JOIN controllers c ON c.id = e.controller_id "
               "WHERE 1=1")
        params: list[Any] = []
        if controller_id is not None:
            sql += " AND e.controller_id = ?"
            params.append(controller_id)
        if status:
            sql += " AND e.status = ?"
            params.append(status)
        sql += " ORDER BY e.id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 500)))
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]


# ---------------------------------------------------------------------- singleton
_instance: Optional[EventService] = None


def init_event_service(conn: sqlite3.Connection) -> EventService:
    global _instance
    _instance = EventService(conn)
    return _instance


def get_event_service() -> Optional[EventService]:
    return _instance


def set_event_service(svc: Optional[EventService]) -> None:
    global _instance
    _instance = svc


def get_flow_service_ref():
    """Ленивый импорт — циклограмма event↔flow развязана импортом внутри."""
    from .flow_service import get_flow_service
    return get_flow_service()
