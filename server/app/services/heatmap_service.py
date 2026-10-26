"""Тепловая карта полива (Этап 8, блок 8.3; Артефакт 0.6 §5.9).

Строит построчную сетку «контроллер → зоны × временные слоты» за сутки.
Состояния ячеек (приоритет сверху вниз):

  error    — событие с аварийным reason_code (no_flow/over_flow/...) или
             run со статусом failed/aborted в слоте;
  blocked  — зона заблокирована (zone_locks.active) на период слота, либо
             пропуск run с reason_code='zone_locked' по зонам run;
  manual   — полив источника manual/service/test/emergency в слоте;
  soak     — фаза замачивания: step_type='soak' из details_json прогона
             (покрывается event-интервалом water-фаз);
  water    — идёт полив (пересечение интервала события со слотом);
  offline  — контроллер connection_status='offline' и в слоте нет событий;
  idle     — ничего не происходит.

watering_blocks (ТЗ Артефакт 0.4) в текущей схеме отсутствует (миграции
0001–0009 трогать запрещено, Этап 9A ещё не реализован) — блокировки
берутся из zone_locks (миграция 0002), что является штатным источником
на момент Этапа 8.

Производительность (DoD: 50 контроллеров × 16 зон × 96 слотов < 500 мс):
— ровно 4 SQL-запроса независимо от числа объектов (без N+1);
— пересечения интервалов со слотами считаются арифметикой целых чисел
  (min/max) над списком кортежей, без SQLite-функций и без Python-циклов
  по всем ячейкам: заполняются только реально занятые интервалы.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional

# Слоты: interval_min ∈ {5,10,15,30} — делители суток (96 слотов по 15 мин).
VALID_INTERVALS = (5, 10, 15, 30)

CELL_STATES = ("idle", "water", "soak", "manual", "blocked", "error",
               "offline")

# Аварийные причины (совпадает с event_service.EMERGENCY_REASONS).
_ERROR_REASONS = {"no_flow", "over_flow", "under_flow", "emergency_stop"}
_ERROR_RUN_STATUSES = ("failed", "aborted")
_MANUAL_SOURCES = ("manual", "service", "test", "emergency")


def day_bounds_ts(date_str: str) -> tuple[int, int]:
    """Границы суток [lo, hi) в unix-секундах (локальный часовой пояс —
    тот же подход, что в report_service/flow_daily)."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    lo = int(d.timestamp())
    return lo, lo + 86400


def _columns(n: int) -> list[str]:
    """Метки слотов 'HH:MM' для n слотов суток."""
    out = []
    for i in range(n):
        m = i * (1440 // n)
        out.append(f"{m // 60:02d}:{m % 60:02d}")
    return out


def build_heatmap(conn: sqlite3.Connection, date: str, *,
                  interval_min: int = 15,
                  controller_ids: Optional[list[int]] = None,
                  now_ts: Optional[int] = None) -> dict[str, Any]:
    """Собрать тепловую карту за сутки `date` (см. module docstring)."""
    if interval_min not in VALID_INTERVALS:
        raise ValueError(
            f"interval_min должен быть одним из {VALID_INTERVALS}")
    n = 1440 // interval_min
    slot_sec = interval_min * 60
    lo, hi = day_bounds_ts(date)
    if now_ts is None:
        now_ts = int(datetime.now().timestamp())
    day_end = min(hi, max(lo, now_ts))  # будущее не размечаем (только прошлое)

    # --------------------------------------------------------- 1) контроллеры
    where_c = ""
    args_c: list[Any] = []
    if controller_ids:
        ids = [int(x) for x in controller_ids]
        where_c = " AND c.id IN (%s)" % ",".join("?" * len(ids))
        args_c = ids
    controllers = conn.execute(
        "SELECT c.id, c.name, c.connection_status FROM controllers c"
        " WHERE c.deleted_at IS NULL" + where_c +
        " ORDER BY c.name COLLATE NOCASE, c.id", args_c).fetchall()
    ctrl_list = [dict(r) for r in controllers]
    cid_of = {c["id"]: idx for idx, c in enumerate(ctrl_list)}
    if not ctrl_list:
        return {"date": date, "interval_min": interval_min,
                "columns": _columns(n), "rows": []}

    # -------------------------------------------------------------- 2) зоны
    zones_rows = conn.execute(
        "SELECT z.id, z.controller_id, z.zone_number, z.name FROM zones z"
        " WHERE z.deleted_at IS NULL"
        + (" AND z.controller_id IN (%s)" % ",".join("?" * len(ctrl_list))
           if ctrl_list else "") + " ORDER BY z.controller_id, z.zone_number",
        [c["id"] for c in ctrl_list]).fetchall()

    # Собираем зоны по контроллерам
    by_ctrl: dict[int, list[dict[str, Any]]] = {}
    for r in zones_rows:
        zi = dict(r)
        if zi["controller_id"] in cid_of:
            by_ctrl.setdefault(zi["controller_id"], []).append(zi)

    # cells[ctrl_idx][zone_idx or -1] — массив состояний слотов
    def _grid(zones: list[dict[str, Any]]) -> list[list[str]]:
        return [["idle"] * n for _ in range(len(zones) + 1)]  # +1 — строка контроллера

    grids = {c["id"]: _grid(by_ctrl.get(c["id"], [])) for c in ctrl_list}
    zindex = {c["id"]: {z["id"]: j + 1 for j, z in
                        enumerate(by_ctrl.get(c["id"], []))}
              for c in ctrl_list}

    # --------------------------------------------- 3) события (один запрос)
    ev_rows = conn.execute(
        """SELECT e.controller_id, e.primary_zone_id, e.source, e.status,
                  e.start_ts, e.end_ts, e.reason_code, e.volume_liters,
                  e.run_id, ez.zone_id
             FROM watering_events e
             LEFT JOIN watering_event_zones ez ON ez.event_id = e.id
            WHERE e.start_ts < ? AND COALESCE(e.end_ts, e.start_ts) >= ?
              AND e.controller_id IN (%s)"""
        % ",".join("?" * len(ctrl_list)),
        [hi, lo] + [c["id"] for c in ctrl_list]).fetchall()

    # ------------------------------------------- 4) прогоны (один запрос)
    run_rows = conn.execute(
        """SELECT id, controller_id, run_id, program_id, source, status,
                  reason_code, planned_start_ts, actual_start_ts, end_ts,
                  water_sec, volume_liters, zones_json, details_json
             FROM watering_runs
            WHERE (COALESCE(actual_start_ts, planned_start_ts) < ?
                   OR end_ts >= ?)
              AND controller_id IN (%s)"""
        % ",".join("?" * len(ctrl_list)),
        [hi, lo] + [c["id"] for c in ctrl_list]).fetchall()
    runs_by_id: dict[int, dict[str, Any]] = {}
    for r in run_rows:
        rd = dict(r)
        runs_by_id[rd["id"]] = rd

    events_by_uid: dict[str, dict[str, Any]] = {}
    for r in ev_rows:
        uid_key = r["controller_id"], r["run_id"], r["start_ts"]
        ev = events_by_uid.get(uid_key)
        if ev is None:
            ev = {k: r[k] for k in
                  ("controller_id", "primary_zone_id", "source", "status",
                   "start_ts", "end_ts", "reason_code", "volume_liters",
                   "run_id")}
            ev["zones"] = set()
            if r["zone_id"] is not None:
                ev["zones"].add(r["zone_id"])
            events_by_uid[uid_key] = ev
        elif r["zone_id"] is not None:
            ev["zones"].add(r["zone_id"])

    def _slot_of(ts: int) -> int:
        return (ts - lo) // slot_sec

    def _paint(grid_i: int, grid_j: int, state: str, a: int, b: int) -> None:
        """Ячейки [a,b] сетки grids[grid_i][grid_j] := state (без понижения
        приоритета: error > blocked > manual > soak > water)."""
        order = {"idle": 0, "water": 1, "soak": 2, "manual": 3,
                 "blocked": 4, "error": 5}
        row = grids[grid_i][grid_j]
        pr = order[state]
        for s in range(max(a, 0), min(b, n - 1) + 1):
            if order[row[s]] <= pr:
                row[s] = state

    # Accepted plans are visible into the future, separately marked from facts.
    # A compiled/sent proposal is not a delivered controller plan.
    from pathlib import Path
    from datetime import timedelta, timezone
    plan_cells = {}
    for schedule in conn.execute("""SELECT cs.controller_id,cs.payload_path FROM controller_schedules cs
        WHERE cs.status='acknowledged' AND cs.schedule_version=(
            SELECT MAX(x.schedule_version) FROM controller_schedules x
            WHERE x.controller_id=cs.controller_id AND x.status='acknowledged')"""):
        cid=schedule['controller_id']
        if cid not in grids:continue
        try:
            payload=json.loads(Path(schedule['payload_path']).read_text())
            offset=int(payload.get('timezone_offset_min',0))
            for run in payload.get('runs',[]):
                base=int(datetime.fromisoformat(run['date']).replace(
                    tzinfo=timezone(timedelta(minutes=offset))).timestamp())+run['start_minute_local']*60
                cursor=base
                for step in run.get('steps',[]):
                    end=cursor+int(step['duration_sec'])
                    if cursor<hi and end>lo:
                        a,b=_slot_of(max(cursor,lo)),_slot_of(min(end-1,hi-1))
                        nums=step.get('active_zones') or step.get('display_zones') or []
                        state='water' if step['phase']=='water' else 'soak'
                        for z in by_ctrl.get(cid,[]):
                            if z['zone_number'] in nums:
                                _paint(cid,zindex[cid][z['id']],state,a,b)
                                plan_cells.setdefault(z['id'],set()).update(range(max(0,a),min(n-1,b)+1))
                        if nums:_paint(cid,0,state,a,b)
                    cursor=end
        except (OSError,ValueError,TypeError,KeyError):
            continue

    # ---- красим события: water/manual/error -------------------------------
    for ev in events_by_uid.values():
        cid = ev["controller_id"]
        g = grids.get(cid)
        if g is None:
            continue
        a = _slot_of(max(ev["start_ts"], lo))
        end = ev["end_ts"] if ev["end_ts"] else ev["start_ts"]
        b = _slot_of(min(end, day_end - 1))
        if b < a:
            b = a
        state = ("error" if (ev["reason_code"] in _ERROR_REASONS
                             or ev["status"] == "stopped"
                             and ev.get("reason_code")) else
                 "manual" if ev["source"] in _MANUAL_SOURCES else "water")
        targets = [zindex[cid][z] for z in ev["zones"] if z in zindex[cid]]
        if not targets:
            pz = ev["primary_zone_id"]
            targets = [zindex[cid][pz]] if pz in zindex[cid] else [0]
        for gj in targets:
            _paint(cid, gj, state, a, b)
        # строка контроллера (gj=0) — минимум «идёт полив»
        _paint(cid, 0, state, a, b)

    # ---- soak-фазы из details_json прогонов -------------------------------
    for rd in runs_by_id.values():
        cid = rd["controller_id"]
        g = grids.get(cid)
        if g is None or not rd["details_json"]:
            continue
        try:
            det = json.loads(rd["details_json"])
        except (ValueError, TypeError):
            continue
        steps = det.get("steps") if isinstance(det, dict) else None
        base = rd["actual_start_ts"] or rd["planned_start_ts"]
        if not steps or not base:
            continue
        acc = 0
        for st in steps:
            dur = int(st.get("duration_sec") or st.get("duration_minutes")
                      and int(st.get("duration_minutes")) * 60 or 0)
            if st.get("step_type") == "soak" and dur > 0:
                a = _slot_of(base + acc)
                b = _slot_of(base + acc + max(dur - 1, 0))
                nums = st.get("zones") or []
                zids = [z["id"] for z in by_ctrl.get(cid, [])
                        if z["zone_number"] in nums]
                for z in zids:
                    _paint(cid, zindex[cid][z], "soak", a, b)
                _paint(cid, 0, "soak", a, b)
            acc += dur

    # ---- skipped/failed/blocked прогоны ------------------------------------
    for rd in runs_by_id.values():
        cid = rd["controller_id"]
        if cid not in grids:
            continue
        base = rd["actual_start_ts"] or rd["planned_start_ts"]
        if base is None:
            continue
        a = _slot_of(max(base, lo))
        b = _slot_of(min(base + max(int(rd["water_sec"] or 0) - 1, 0),
                         day_end - 1))
        if b < a:
            b = a
        rc = rd["reason_code"] or ""
        if rd["status"] in _ERROR_RUN_STATUSES:
            _paint(cid, 0, "error", a, b)
        elif rc == "zone_locked" or rd["status"] == "skipped":
            if rc == "zone_locked":
                try:
                    nums = json.loads(rd["zones_json"] or "[]")
                except (ValueError, TypeError):
                    nums = []
                zids = [z["id"] for z in by_ctrl.get(cid, [])
                        if z["zone_number"] in nums]
                for z in zids:
                    _paint(cid, zindex[cid][z], "blocked", a, b)
                _paint(cid, 0, "blocked", a, b)

    # ---- активные блокировки зон (zone_locks) -------------------------------
    all_zone_ids = [z["id"] for zz in by_ctrl.values() for z in zz]
    lock_rows = conn.execute(
        """SELECT zl.zone_id, zl.locked_at, zl.unlock_at FROM zone_locks zl
            WHERE zl.active = 1 AND zl.zone_id IN (%s)"""
        % ",".join("?" * len(all_zone_ids)), all_zone_ids).fetchall() if all_zone_ids else []
    for lr in lock_rows:
        pos = None
        for cid, idx_map in zindex.items():
            if lr["zone_id"] in idx_map:
                pos = (cid, idx_map[lr["zone_id"]])
                break
        if pos is None:
            continue
        # Окно блокировки в слотах: от locked_at до unlock_at; если unlock_at
        # нет или это конец суток — блокировка тянется до конца дня.
        try:
            la = datetime.fromisoformat(lr["locked_at"]).timestamp()
        except (ValueError, TypeError):
            la = lo
        ua = None
        if lr["unlock_at"]:
            try:
                ua = datetime.fromisoformat(lr["unlock_at"]).timestamp()
            except (ValueError, TypeError):
                ua = None
        a = _slot_of(max(int(la), lo))
        b = n - 1 if ua is None else _slot_of(min(int(ua) - 1, day_end - 1))
        _paint(pos[0], pos[1], "blocked", a, b)

    # ---- offline: ни одного раскрашенного слота + контроллер офлайн --------
    for c in ctrl_list:
        g = grids[c["id"]]
        painted = any(s != "idle" for row in g for s in row)
        if not painted and c["connection_status"] != "online":
            today_slots_lo = _slot_of(lo)
            today_slots_hi = _slot_of(day_end - 1) if day_end > lo else -1
            if today_slots_hi >= 0:
                row = g[0]
                for s in range(today_slots_lo, min(today_slots_hi, n - 1) + 1):
                    row[s] = "offline"

    # ---- tooltip-данные: run/program по зоне+слоту --------------------------
    # Формат ответа: row.zones[j]["run_info"] — dict slot_idx(str) -> инфо
    # (program_id/source/status/volume_liters/run_id), для title-подсказок.
    tips_by_zone: dict[int, dict[int, dict[str, Any]]] = {}   # zone_id -> {slot: info}
    ctrl_tips: dict[int, dict[int, dict[str, Any]]] = {}      # controller_row0: -1 -> {slot: info}
    for rd in runs_by_id.values():
        cid = rd["controller_id"]
        base = rd["actual_start_ts"] or rd["planned_start_ts"]
        if base is None or base >= hi:
            continue
        end = rd["end_ts"] or (base + int(rd["water_sec"] or 0))
        a = _slot_of(max(base, lo))
        info = {"run_id": rd["run_id"], "program_id": rd["program_id"],
                "source": rd["source"], "status": rd["status"],
                "volume_liters": rd["volume_liters"]}
        try:
            nums = json.loads(rd["zones_json"] or "[]")
        except (ValueError, TypeError):
            nums = []
        zmap = {z["zone_number"]: z["id"] for z in by_ctrl.get(cid, [])}
        painted_any = False
        for num in nums:
            zid = zmap.get(num)
            if zid is None:
                continue
            painted_any = True
            b = _slot_of(min(end, hi - 1))
            for s in range(a, max(b, a) + 1):
                tips_by_zone.setdefault(zid, {})[s] = info
        if not painted_any:
            b = _slot_of(min(end, hi - 1))
            for s in range(a, max(b, a) + 1):
                ctrl_tips.setdefault(cid, {})[s] = info

    rows_out = []
    for c in ctrl_list:
        zs = by_ctrl.get(c["id"], [])
        g = grids[c["id"]]
        ct = {str(k): v for k, v in ctrl_tips.get(c["id"], {}).items()}
        rows_out.append({
            "controller_id": c["id"],
            "controller_name": c["name"],
            "connection_status": c["connection_status"],
            "expanded": False,
            "cells": g[0],
            "run_info": ct,
            "zones": [{
                "zone_id": z["id"],
                "zone_number": z["zone_number"],
                "name": z["name"],
                "cells": g[j + 1],
                "planned_cells": sorted(plan_cells.get(z['id'],set())),
                "run_info": {str(k): v for k, v in
                             tips_by_zone.get(z["id"], {}).items()},
            } for j, z in enumerate(zs)],
        })
    return {"date": date, "interval_min": interval_min,
            "columns": _columns(n), "rows": rows_out,
            "cell_states": list(CELL_STATES)}
