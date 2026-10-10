"""Этап 6: расход воды, дневные агрегаты, аварии потока (ТЗ §9).

Зоны ответственности:
- приём показаний poliv/{box_id}/flow — сырые показания НЕ пишем в отдельную
  таблицу; живые поля controllers.instant_lpm / flow_total_liters обновляет
  mqtt_client._handle_flow (уже делает с Этапа 3). Здесь flow используется
  для ДЕТЕКЦИИ аварий: полив идёт (есть активный прогон), а потока нет — no_flow;
  поток существенно выше ожидаемого — over_flow; ниже нормы — under_flow.
  Пороги и задержки — из таблицы settings (миграция 0008);
- дневные агрегаты flow_daily — источник истины СОБЫТИЯ (EventService вызывает
  on_event_saved → apply_event_to_daily): объём finished/stopped-события
  распределяется поровну на зоны прогона (параллельные группы);
- регистрация аварий: emergency_lock_until_ts на контроллере (колонка из
  миграции 0004) + критическое уведомление + журнал logs. Автозапуск полива
  блокируется проверкой is_emergency_locked() (routes_stage6/web + сервисы).

Отличия от рекомендации qwen_chat (осознанные):
- статусы событий проекта started/finished/stopped (не ok/no_flow/...):
  авария определяется по reason_code финишного события ИЛИ по детекции по
  потоку во время активного прогона;
- run_id TEXT; чтение настроек — общий хелпер get_setting().
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ..infra.logging import get_logger

log = get_logger("poliv.flow")


def get_setting(conn: sqlite3.Connection, key: str, default: Any) -> Any:
    """Настройка из таблицы settings (key/value_json, миграция 0001)."""
    try:
        row = conn.execute(
            "SELECT value_json FROM settings WHERE key=?", (key,)).fetchone()
        if row is None:
            return default
        return json.loads(row["value_json"])
    except Exception:
        return default


def set_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    with conn:
        conn.execute(
            """INSERT INTO settings(key, value_json, description, updated_at)
               VALUES (?, ?, 'Этап 6', datetime('now'))
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                   updated_at=datetime('now')""",
            (key, json.dumps(value)),
        )


EMERGENCY_MESSAGE = {
    "no_flow": "Авария: нет потока воды при открытом клапане",
    "over_flow": "Авария: поток выше ожидаемой нормы",
    "under_flow": "Внимание: поток ниже нормы",
    "emergency_stop": "Аварийная остановка полива",
}

# Причина → уровень уведомления (ТЗ: info/warning/error/critical)
EMERGENCY_SEVERITY = {
    "no_flow": "critical",
    "over_flow": "critical",
    "under_flow": "warning",
    "emergency_stop": "error",
}


class FlowService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        # Аудит 06.10 (P1-6): «наблюдение» нулевого потока — состояние ЭКЗЕМПЛЯРА,
        # а не атрибут класса: два приложения/сервиса не должны делить watch.
        self._no_flow_watch: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------ настройки
    def thresholds(self) -> dict[str, Any]:
        c = self.conn
        return {
            "enabled_default": bool(get_setting(c, "flow.enabled_default", False)),
            "no_flow_delay_sec": int(get_setting(c, "flow.no_flow_delay_sec", 10)),
            "overflow_percent": float(get_setting(c, "flow.overflow_percent", 150)),
            "underflow_percent": float(get_setting(c, "flow.underflow_percent", 70)),
            "stable_delay_sec": int(get_setting(c, "flow.stable_delay_sec", 15)),
            "emergency_lock_min": int(get_setting(c, "flow.emergency_lock_min", 30)),
            "min_samples_for_expected": int(
                get_setting(c, "flow.min_samples_for_expected", 3)),
        }

    # ------------------------------------------------------------------ вход flow
    def handle_flow(self, box_id: str, p: dict[str, Any]) -> None:
        """Детекция аварий по потоку. Вызывается из mqtt_client._handle_flow
        ПОСЛЕ записи live-полей. Сырые показания здесь не дублируются."""
        if not p.get("flow_enabled"):
            self._clear_no_flow_watch(box_id)
            return
        active = self._active_watering_box(box_id)
        if active is None:
            self._clear_no_flow_watch(box_id)
            return
        ctrl_id, expected_lpm = active
        il = p.get("instant_lpm")
        il = float(il) if isinstance(il, (int, float)) and not isinstance(il, bool) else None
        now = time.monotonic()
        watch = self._no_flow_watch.setdefault(box_id, {"since": None})

        # no_flow: полив идёт, потока нет дольше задержки → авария
        if il is not None and il <= 0.01:
            if watch["since"] is None:
                watch["since"] = now
            elif now - watch["since"] >= self.thresholds()["no_flow_delay_sec"]:
                self._clear_no_flow_watch(box_id)
                self.register_emergency(
                    ctrl_id, "no_flow",
                    {"box_id": box_id, "instant_lpm": il,
                     "expected_flow_lpm": expected_lpm},
                    event_id=None)
        else:
            watch["since"] = None
            # over_flow: только если известен ожидаемый расход зоны прогона
            if il is not None and expected_lpm:
                th = self.thresholds()
                if il > expected_lpm * th["overflow_percent"] / 100.0:
                    self.register_emergency(
                        ctrl_id, "over_flow",
                        {"box_id": box_id, "instant_lpm": il,
                         "expected_flow_lpm": expected_lpm},
                        event_id=None)

    def _active_watering_box(self, box_id: str) -> Optional[tuple[int, Optional[float]]]:
        """Идёт ли полив: активный прогон (live active_zones) + ожидаемый
        расход первой активной зоны (zones.expected_flow_lpm, миграция 0002)."""
        row = self.conn.execute(
            """SELECT id, active_zones_json FROM controllers WHERE box_id=?""",
            (box_id,)).fetchone()
        if row is None:
            return None
        try:
            zones = json.loads(row["active_zones_json"] or "[]")
        except (TypeError, ValueError):
            zones = []
        if not zones:
            return None
        zrow = self.conn.execute(
            """SELECT expected_flow_lpm FROM zones
               WHERE controller_id=? AND zone_number=?""",
            (row["id"], int(zones[0]))).fetchone()
        expected = None
        if zrow is not None and zrow["expected_flow_lpm"] is not None:
            expected = float(zrow["expected_flow_lpm"])
        return int(row["id"]), expected

    # no_flow «наблюдение» — монотонное время старта нулевого потока по box_id
    _no_flow_watch: dict[str, dict[str, Any]] = {}

    def _clear_no_flow_watch(self, box_id: str) -> None:
        self._no_flow_watch.pop(box_id, None)

    # ------------------------------------------------------------------ агрегаты
    def on_event_saved(self, event_id: int) -> None:
        """Вызов EventService после сохранения finished/stopped-события."""
        ev = self.conn.execute(
            "SELECT * FROM watering_events WHERE id=?", (event_id,)).fetchone()
        if ev is None or ev["status"] not in ("finished", "stopped"):
            return
        self.apply_event_to_daily(ev)
        reason = ev["reason_code"]
        if reason in EMERGENCY_MESSAGE:
            self.register_emergency(int(ev["controller_id"]), reason,
                                    {"event_uid": ev["event_uid"],
                                     "run_id": ev["run_id"]},
                                    event_id=event_id)

    def apply_event_to_daily(self, ev: sqlite3.Row) -> None:
        volume = float(ev["volume_liters"] or 0.0)
        water_sec = int(ev["water_sec"] or 0)
        start_ts = int(ev["start_ts"] or 0)
        if start_ts <= 0:
            return
        date = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        controller_id = int(ev["controller_id"])
        is_anomaly = 1 if ev["reason_code"] in EMERGENCY_MESSAGE else 0

        zones = self.conn.execute(
            "SELECT zone_id FROM watering_event_zones WHERE event_id=?",
            (ev["id"],)).fetchall()
        with self.conn:
            if zones:
                per_zone = volume / len(zones)
                for z in zones:
                    self._upsert_daily(date, controller_id, int(z["zone_id"]),
                                       "zone", per_zone, water_sec, is_anomaly)
            else:
                self._upsert_daily(date, controller_id, None, "controller",
                                   volume, water_sec, is_anomaly)

    def _upsert_daily(self, date: str, controller_id: int, zone_id: Optional[int],
                      attribution: str, volume: float, water_sec: int,
                      anomaly: int) -> None:
        # Схема 0008: zone_id NOT NULL DEFAULT -1 (-1 = агрегат по контроллеру),
        # поэтому в запросе сравниваем нормализованное значение напрямую.
        zone_key = -1 if zone_id is None else int(zone_id)
        existing = self.conn.execute(
            """SELECT id FROM flow_daily
               WHERE date=? AND controller_id=?
                 AND zone_id=? AND attribution=?""",
            (date, controller_id, zone_key, attribution)).fetchone()
        if existing:
            self.conn.execute(
                """UPDATE flow_daily SET
                       total_volume_l = total_volume_l + ?,
                       watering_count = watering_count + 1,
                       water_sec = water_sec + ?,
                       anomaly_count = anomaly_count + ?,
                       updated_at = datetime('now')
                   WHERE id=?""",
                (volume, water_sec, anomaly, existing["id"]))
        else:
            self.conn.execute(
                """INSERT INTO flow_daily(date, controller_id, zone_id,
                       attribution, total_volume_l, watering_count, water_sec,
                       anomaly_count)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (date, controller_id, zone_key, attribution, volume, 1,
                 water_sec, anomaly))

    # ------------------------------------------------------------------ аварии
    def register_emergency(self, controller_id: int, reason: str,
                           details: dict[str, Any],
                           event_id: Optional[int] = None) -> Optional[int]:
        """Блокировка автозапуска + критическое уведомление + журнал.

        Возвращает id уведомления (или None, если это повтор той же аварии —
        дедупликация по dedupe_key в NotificationService)."""
        lock_min = self.thresholds()["emergency_lock_min"]
        lock_until = int(time.time()) + lock_min * 60
        box_row = self.conn.execute(
            "SELECT box_id FROM controllers WHERE id=?",
            (controller_id,)).fetchone()
        with self.conn:
            self.conn.execute(
                """UPDATE controllers SET emergency_lock_until_ts=?,
                       current_mode='error', current_phase='error',
                       active_zones_json='[]', display_zones_json='[]',
                       updated_at=datetime('now')
                   WHERE id=?""",
                (lock_until, controller_id))
        message = EMERGENCY_MESSAGE.get(reason, f"Авария потока: {reason}")
        log.warning("FLOW: авария %s на контроллере %s (#%s), блокировка до %s",
                    reason, box_row["box_id"] if box_row else "?",
                    controller_id, lock_until)

        notif_id = None
        svc = get_notification_service_ref()
        if svc is not None:
            zone_num = None
            az = details.get("active_zones") or []
            if az:
                zone_num = az[0]
            notif_id = svc.create(
                type="flow_emergency",
                severity=EMERGENCY_SEVERITY.get(reason, "critical"),
                message=message,
                controller_id=controller_id,
                zone_number=zone_num,
                run_id=details.get("run_id"),
                event_id=event_id,
                dedupe_key=f"flow:{reason}:{controller_id}:{details.get('event_uid') or int(time.time()) // 300}",
                details={"reason": reason, **{k: v for k, v in details.items()
                                              if k != "payload"}},
            )
        try:
            from .notification_service import write_mqtt_log
            write_mqtt_log(self.conn, "controller.emergency",
                           box_row["box_id"] if box_row else str(controller_id),
                           {"reason": reason, "lock_until_ts": lock_until,
                            "event_id": event_id})
        except Exception:
            log.exception("FLOW: не удалось записать журнал аварии")
        return notif_id

    def is_emergency_locked(self, controller_id: int) -> bool:
        row = self.conn.execute(
            "SELECT emergency_lock_until_ts FROM controllers WHERE id=?",
            (controller_id,)).fetchone()
        if row is None or row["emergency_lock_until_ts"] is None:
            return False
        return int(row["emergency_lock_until_ts"]) > int(time.time())

    def clear_error(self, controller_id: int) -> bool:
        """Сброс аварийной блокировки сервером (команда контроллеру — из API)."""
        with self.conn:
            cur = self.conn.execute(
                """UPDATE controllers SET emergency_lock_until_ts=NULL,
                       current_mode='idle', current_phase='idle',
                       updated_at=datetime('now')
                   WHERE id=? AND emergency_lock_until_ts IS NOT NULL""",
                (controller_id,))
        self._clear_no_flow_watch_by_id(controller_id)
        return cur.rowcount > 0

    def _clear_no_flow_watch_by_id(self, controller_id: int) -> None:
        row = self.conn.execute(
            "SELECT box_id FROM controllers WHERE id=?",
            (controller_id,)).fetchone()
        if row is not None:
            self._clear_no_flow_watch(row["box_id"])

    # ------------------------------------------------------------------ сводка
    def summary(self, date_from: Optional[str] = None,
                date_to: Optional[str] = None) -> dict[str, Any]:
        sql = """SELECT COALESCE(SUM(total_volume_l),0) AS volume_l,
                        COALESCE(SUM(watering_count),0) AS runs,
                        COALESCE(SUM(water_sec),0) AS water_sec,
                        COALESCE(SUM(anomaly_count),0) AS anomalies
                 FROM flow_daily WHERE 1=1"""
        params: list[Any] = []
        if date_from:
            sql += " AND date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND date <= ?"
            params.append(date_to)
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else {}


# ---------------------------------------------------------------------- singleton
_instance: Optional[FlowService] = None


def init_flow_service(conn: sqlite3.Connection) -> FlowService:
    global _instance
    _instance = FlowService(conn)
    return _instance


def get_flow_service() -> Optional[FlowService]:
    return _instance


def set_flow_service(svc: Optional[FlowService]) -> None:
    global _instance
    _instance = svc


def get_notification_service_ref():
    from .notification_service import get_notification_service
    return get_notification_service()
