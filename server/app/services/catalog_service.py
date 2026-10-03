"""Сервис CRUD справочников Этапа 2: контроллеры, зоны, программы.

Правила из ТЗ (Этап 2):
- зоны не удаляются, только отключаются (soft-disable: deleted_at);
- контроллеры тоже сначала отключаем, а не удаляем (soft-disable);
- номер зоны уникален в пределах контроллера;
- программа может быть без зон и содержать зоны разных контроллеров;
- внутри одного контроллера зоны выполняются последовательно (порядок seq);
- для зон предусматриваем изображения и иконки (поля image_path, icon).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from ..infra.logging import get_logger
from ..infra.security import utcnow_iso

log = get_logger("poliv.catalog")


class ValidationError(ValueError):
    """Ошибка валидации данных справочников (422 на API, текст в форме)."""


def _parse_int(value: Any, name: str, minimum: int = 1) -> int:
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        raise ValidationError(f"Поле «{name}» должно быть целым числом")
    if ivalue < minimum:
        raise ValidationError(f"Поле «{name}» должно быть >= {minimum}")
    return ivalue


def _parse_bool(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    return 1 if str(value).lower() in ("1", "true", "on", "yes", "да") else 0


def _clean_str(value: Any, name: str, max_len: int = 200, required: bool = True) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ValidationError(f"Поле «{name}» обязательно")
    if len(text) > max_len:
        raise ValidationError(f"Поле «{name}» длиннее {max_len} символов")
    return text


def _validate_season(start: Any, end: Any) -> tuple[Optional[str], Optional[str]]:
    """Сезон задаётся парой MM-DD либо оставляется пустым (весь год)."""
    s = _clean_str(start, "Начало сезона", 5, required=False)
    e = _clean_str(end, "Конец сезона", 5, required=False)
    if bool(s) != bool(e):
        raise ValidationError("Сезон задаётся парой дат: начало и конец")
    for v in (s, e):
        if v:
            mm, _, dd = v.partition("-")
            if not (mm.isdigit() and dd.isdigit()) or not (1 <= int(mm) <= 12) or not (1 <= int(dd) <= 31):
                raise ValidationError(f"Некорректная дата сезона «{v}»: ожидается ММ-ДД")
    return (s or None), (e or None)


class CatalogService:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ------------------------------------------------------------ controllers
    def list_controllers(self, include_deleted: bool = False) -> list[dict]:
        sql = (
            "SELECT c.*, (SELECT COUNT(*) FROM zones z "
            " WHERE z.controller_id=c.id AND z.deleted_at IS NULL) AS zone_count "
            "FROM controllers c"
        )
        if not include_deleted:
            sql += " WHERE c.deleted_at IS NULL"
        sql += " ORDER BY c.name"
        return [dict(r) for r in self.conn.execute(sql).fetchall()]

    def get_controller(self, controller_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM controllers WHERE id = ?", (controller_id,)
        ).fetchone()
        return dict(row) if row else None

    def create_controller(self, data: dict) -> dict:
        name = _clean_str(data.get("name"), "Название", 100)
        box_id = _clean_str(data.get("box_id"), "Box ID", 50).upper()
        if not box_id.replace("-", "").replace("_", "").isalnum():
            raise ValidationError("Box ID: только буквы, цифры, дефис и подчёркивание")
        now = utcnow_iso()
        try:
            with self.conn:
                cur = self.conn.execute(
                    """INSERT INTO controllers(name, box_id, model, description, enabled,
                                               created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        name,
                        box_id,
                        _clean_str(data.get("model"), "Модель", 50, required=False) or None,
                        _clean_str(data.get("description"), "Описание", 500, required=False) or None,
                        _parse_bool(data.get("enabled", True)),
                        now,
                        now,
                    ),
                )
        except sqlite3.IntegrityError:
            raise ValidationError(f"Контроллер с Box ID «{box_id}» уже существует")
        log.info("Создан контроллер %s (id=%s)", box_id, cur.lastrowid)
        return self.get_controller(cur.lastrowid)  # type: ignore[arg-type]

    def update_controller(self, controller_id: int, data: dict) -> dict:
        current = self.get_controller(controller_id)
        if not current:
            raise ValidationError("Контроллер не найден")
        name = _clean_str(data.get("name"), "Название", 100)
        box_id = _clean_str(data.get("box_id"), "Box ID", 50).upper()
        try:
            with self.conn:
                self.conn.execute(
                    """UPDATE controllers SET name=?, box_id=?, model=?, description=?,
                       enabled=?, updated_at=? WHERE id=?""",
                    (
                        name,
                        box_id,
                        _clean_str(data.get("model"), "Модель", 50, required=False) or None,
                        _clean_str(data.get("description"), "Описание", 500, required=False) or None,
                        _parse_bool(data.get("enabled", current["enabled"])),
                        utcnow_iso(),
                        controller_id,
                    ),
                )
        except sqlite3.IntegrityError:
            raise ValidationError(f"Контроллер с Box ID «{box_id}» уже существует")
        return self.get_controller(controller_id)  # type: ignore[return-value]

    def delete_controller(self, controller_id: int) -> None:
        """Soft-disable: помечаем удалённым, если зон нет."""
        current = self.get_controller(controller_id)
        if not current:
            raise ValidationError("Контроллер не найден")
        zones = self.conn.execute(
            "SELECT COUNT(*) c FROM zones WHERE controller_id=? AND deleted_at IS NULL",
            (controller_id,),
        ).fetchone()["c"]
        if zones:
            raise ValidationError(
                f"Нельзя удалить контроллер: на нём {zones} активных зон. "
                "Сначала отключите/удалите зоны."
            )
        with self.conn:
            self.conn.execute(
                "UPDATE controllers SET deleted_at=?, enabled=0, updated_at=? WHERE id=?",
                (utcnow_iso(), utcnow_iso(), controller_id),
            )
        log.info("Контроллер id=%s отключён (soft-delete)", controller_id)

    # ------------------------------------------------------------------ zones
    def list_zones(
        self, controller_id: Optional[int] = None, include_deleted: bool = False
    ) -> list[dict]:
        sql = "SELECT z.*, c.name AS controller_name FROM zones z JOIN controllers c ON c.id=z.controller_id"
        conds, params = [], []
        if not include_deleted:
            conds.append("z.deleted_at IS NULL")
        if controller_id is not None:
            conds.append("z.controller_id = ?")
            params.append(controller_id)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY c.name, z.zone_number"
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def get_zone(self, zone_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT z.*, c.name AS controller_name FROM zones z "
            "JOIN controllers c ON c.id=z.controller_id WHERE z.id=?",
            (zone_id,),
        ).fetchone()
        return dict(row) if row else None

    def create_zone(self, data: dict) -> dict:
        controller_id = _parse_int(data.get("controller_id"), "Контроллер")
        if not self.get_controller(controller_id) or self.get_controller(controller_id)["deleted_at"]:
            raise ValidationError("Контроллер не найден или удалён")
        zone_number = _parse_int(data.get("zone_number"), "Номер зоны")
        if not 1 <= zone_number <= 16:
            raise ValidationError("Номер зоны: 1..16")
        name = _clean_str(data.get("name"), "Название", 100)
        duration = _parse_int(data.get("base_duration_minutes", 10), "Базовая длительность", 1)
        if duration > 240:
            raise ValidationError("Базовая длительность не должна превышать 240 минут")
        season_start, season_end = _validate_season(
            data.get("season_start"), data.get("season_end")
        )
        now = utcnow_iso()
        try:
            with self.conn:
                cur = self.conn.execute(
                    """INSERT INTO zones(controller_id, zone_number, name, enabled, icon,
                                         image_path, notes, base_duration_minutes,
                                         watering_adjustment_percent, cycle_soak_enabled,
                                         season_start, season_end, created_at, updated_at)
                       SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?
                       WHERE EXISTS (SELECT 1 FROM controllers
                                     WHERE id=? AND deleted_at IS NULL)""",
                    (
                        controller_id,
                        zone_number,
                        name,
                        _parse_bool(data.get("enabled", True)),
                        _clean_str(data.get("icon"), "Иконка", 10, required=False) or None,
                        _clean_str(data.get("image_path"), "Изображение", 300, required=False) or None,
                        _clean_str(data.get("notes"), "Примечания", 500, required=False) or None,
                        duration,
                        _parse_int(data.get("watering_adjustment_percent", 100), "Корректировка %"),
                        _parse_bool(data.get("cycle_soak_enabled", False)),
                        season_start,
                        season_end,
                        now,
                        now,
                        controller_id,
                    ),
                )
                if cur.rowcount == 0:
                    raise ValidationError("Контроллер не найден или удалён")
        except sqlite3.IntegrityError:
            raise ValidationError(
                f"Зона №{zone_number} уже существует на этом контроллере "
                "(номера не переиспользуются, даже у отключённых зон)"
            )
        log.info("Создана зона №%s контроллера id=%s", zone_number, controller_id)
        return self.get_zone(cur.lastrowid)  # type: ignore[arg-type]

    def update_zone(self, zone_id: int, data: dict) -> dict:
        current = self.get_zone(zone_id)
        if not current:
            raise ValidationError("Зона не найдена")
        name = _clean_str(data.get("name"), "Название", 100)
        duration = _parse_int(data.get("base_duration_minutes", 10), "Базовая длительность", 1)
        if duration > 240:
            raise ValidationError("Базовая длительность не должна превышать 240 минут")
        season_start, season_end = _validate_season(
            data.get("season_start"), data.get("season_end")
        )
        with self.conn:
            self.conn.execute(
                """UPDATE zones SET name=?, enabled=?, icon=?, image_path=?, notes=?,
                   base_duration_minutes=?, watering_adjustment_percent=?,
                   cycle_soak_enabled=?, season_start=?, season_end=?, updated_at=?
                   WHERE id=?""",
                (
                    name,
                    _parse_bool(data.get("enabled", current["enabled"])),
                    _clean_str(data.get("icon"), "Иконка", 10, required=False) or None,
                    _clean_str(data.get("image_path"), "Изображение", 300, required=False) or None,
                    _clean_str(data.get("notes"), "Примечания", 500, required=False) or None,
                    duration,
                    _parse_int(data.get("watering_adjustment_percent", 100), "Корректировка %"),
                    _parse_bool(data.get("cycle_soak_enabled", current["cycle_soak_enabled"])),
                    season_start,
                    season_end,
                    utcnow_iso(),
                    zone_id,
                ),
            )
        return self.get_zone(zone_id)  # type: ignore[return-value]

    def delete_zone(self, zone_id: int) -> None:
        """Зоны не удаляются физически — только soft-disable (ТЗ Этап 2)."""
        if not self.get_zone(zone_id):
            raise ValidationError("Зона не найдена")
        with self.conn:
            self.conn.execute(
                "UPDATE zones SET deleted_at=?, enabled=0, updated_at=? WHERE id=?",
                (utcnow_iso(), utcnow_iso(), zone_id),
            )
            self.conn.execute(
                "DELETE FROM program_zones WHERE zone_id=?", (zone_id,)
            )
        log.info("Зона id=%s отключена (soft-delete), убрана из программ", zone_id)

    # --------------------------------------------------------------- programs
    WEEKDAYS = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")

    def list_programs(self, include_disabled: bool = True) -> list[dict]:
        sql = (
            "SELECT p.*, (SELECT COUNT(*) FROM program_zones pz "
            " WHERE pz.program_id=p.id) AS zone_count FROM programs p"
        )
        if not include_disabled:
            sql += " WHERE p.enabled=1"
        return [
            dict(r) for r in self.conn.execute(sql + " ORDER BY p.name").fetchall()
        ]

    def get_program(self, program_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM programs WHERE id=?", (program_id,)
        ).fetchone()
        if not row:
            return None
        prog = dict(row)
        prog["zones"] = [
            dict(r)
            for r in self.conn.execute(
                """SELECT pz.seq, pz.duration_override_minutes, z.id AS zone_id,
                          z.zone_number, z.name, z.enabled, z.base_duration_minutes,
                          c.name AS controller_name, c.id AS controller_id
                   FROM program_zones pz
                   JOIN zones z ON z.id = pz.zone_id
                   JOIN controllers c ON c.id = z.controller_id
                   WHERE pz.program_id=? AND z.deleted_at IS NULL
                   ORDER BY pz.seq""",
                (program_id,),
            ).fetchall()
        ]
        return prog

    def _validate_schedule(self, data: dict) -> tuple[str, str, Optional[int]]:
        schedule_type = _clean_str(data.get("schedule_type"), "Тип расписания", 20)
        if schedule_type == "weekdays":
            days = data.get("weekdays") or []
            if isinstance(days, str):
                days = [d.strip() for d in days.split(",") if d.strip()]
            # поддержка индексов (0..6, чекбоксы формы) и названий (пн..вс)
            idxs: set[int] = set()
            for d in days:
                dl = str(d).strip().lower()
                if dl in ("0", "1", "2", "3", "4", "5", "6"):
                    # индексы чекбоксов формы: 0=пн .. 6=вс
                    idxs.add(int(dl))
                elif dl in self.WEEKDAYS:
                    idxs.add(self.WEEKDAYS.index(dl))
                elif dl == "7":
                    idxs.add(6)  # нумерация ISO: 7 = воскресенье
                else:
                    try:
                        n = int(dl)
                    except ValueError:
                        raise ValidationError(f"Некорректный день недели: {d}")
                    if not 1 <= n <= 7:
                        raise ValidationError(f"Некорректный день недели: {d}")
                    idxs.add(n - 1)  # 1=пн .. 7=вс
            mask = "".join("1" if i in idxs else "0" for i in range(7))
            if not idxs:
                raise ValidationError("Выберите хотя бы один день недели")
            return schedule_type, mask, None
        if schedule_type == "interval":
            interval = _parse_int(data.get("interval_days"), "Интервал (дней)", 1)
            if interval > 365:
                raise ValidationError("Интервал не должен превышать 365 дней")
            return schedule_type, "0000000", interval
        raise ValidationError("Тип расписания: дни недели или интервал")

    @staticmethod
    def _validate_time(value: Any) -> str:
        import re

        t = str(value or "").strip()
        if not re.fullmatch(r"([01]?\d|2[0-3]):[0-5]\d", t):
            raise ValidationError("Время старта в формате ЧЧ:ММ")
        h, m = t.split(":")
        return f"{int(h):02d}:{m}"

    def create_program(self, data: dict) -> dict:
        name = _clean_str(data.get("name"), "Название", 100)
        schedule_type, mask, interval = self._validate_schedule(data)
        start_time = self._validate_time(data.get("start_time", "06:00"))
        season_start, season_end = _validate_season(
            data.get("season_start"), data.get("season_end")
        )
        now = utcnow_iso()
        try:
            with self.conn:
                cur = self.conn.execute(
                    """INSERT INTO programs(name, description, enabled, schedule_type,
                                            weekdays_mask, interval_days, start_time,
                                            season_start, season_end, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        name,
                        _clean_str(data.get("description"), "Описание", 500, required=False) or None,
                        _parse_bool(data.get("enabled", True)),
                        schedule_type,
                        mask,
                        interval,
                        start_time,
                        season_start,
                        season_end,
                        now,
                        now,
                    ),
                )
        except sqlite3.IntegrityError:
            raise ValidationError(f"Программа «{name}» уже существует")
        prog = self.get_program(cur.lastrowid)  # type: ignore[arg-type]
        assert prog
        return prog

    def update_program(self, program_id: int, data: dict) -> dict:
        current = self.get_program(program_id)
        if not current:
            raise ValidationError("Программа не найдена")
        name = _clean_str(data.get("name"), "Название", 100)
        schedule_type, mask, interval = self._validate_schedule(data)
        start_time = self._validate_time(data.get("start_time", "06:00"))
        season_start, season_end = _validate_season(
            data.get("season_start"), data.get("season_end")
        )
        try:
            with self.conn:
                self.conn.execute(
                    """UPDATE programs SET name=?, description=?, enabled=?, schedule_type=?,
                       weekdays_mask=?, interval_days=?, start_time=?, season_start=?,
                       season_end=?, updated_at=? WHERE id=?""",
                    (
                        name,
                        _clean_str(data.get("description"), "Описание", 500, required=False) or None,
                        _parse_bool(data.get("enabled", current["enabled"])),
                        schedule_type,
                        mask,
                        interval,
                        start_time,
                        season_start,
                        season_end,
                        utcnow_iso(),
                        program_id,
                    ),
                )
        except sqlite3.IntegrityError:
            raise ValidationError(f"Программа «{name}» уже существует")
        return self.get_program(program_id)  # type: ignore[return-value]

    def delete_program(self, program_id: int) -> None:
        """Программы можно удалять (зоны при этом не страдают)."""
        if not self.conn.execute(
            "SELECT id FROM programs WHERE id=?", (program_id,)
        ).fetchone():
            raise ValidationError("Программа не найдена")
        with self.conn:
            self.conn.execute("DELETE FROM program_zones WHERE program_id=?", (program_id,))
            self.conn.execute("DELETE FROM programs WHERE id=?", (program_id,))
        log.info("Удалена программа id=%s", program_id)

    def set_program_zones(self, program_id: int, zone_ids: list[int]) -> dict:
        """Заменяет состав зон программы; порядок = порядок в списке.

        Программа может содержать зоны разных контроллеров (ТЗ).
        Внутри одного контроллера порядок сохраняется — позже компилятор
        машинограммы построит последовательное выполнение.
        """
        if not self.conn.execute(
            "SELECT id FROM programs WHERE id=?", (program_id,)
        ).fetchone():
            raise ValidationError("Программа не найдена")
        seen: set[int] = set()
        ordered: list[int] = []
        for raw in zone_ids:
            zid = _parse_int(raw, "Зона")
            if zid in seen:
                continue
            zone = self.get_zone(zid)
            if not zone or zone["deleted_at"]:
                raise ValidationError(f"Зона id={zid} не найдена или отключена")
            seen.add(zid)
            ordered.append(zid)
        with self.conn:
            self.conn.execute(
                "DELETE FROM program_zones WHERE program_id=?", (program_id,)
            )
            for seq, zid in enumerate(ordered, start=1):
                self.conn.execute(
                    "INSERT INTO program_zones(program_id, zone_id, seq) VALUES (?,?,?)",
                    (program_id, zid, seq),
                )
        log.info("Программа id=%s: установлено зон: %d", program_id, len(ordered))
        return self.get_program(program_id)  # type: ignore[return-value]

    # --------------------------------------------------------------- settings
    def list_settings(self) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT key, value_json, description, updated_at, updated_by "
                "FROM settings ORDER BY key"
            ).fetchall()
        ]

    def set_setting(self, key: str, value: Any, username: str = "") -> None:
        key = _clean_str(key, "Ключ настройки", 100)
        if not self.conn.execute(
            "SELECT key FROM settings WHERE key=?", (key,)
        ).fetchone():
            raise ValidationError(f"Настройка «{key}» не существует")
        # нормализуем значение в JSON
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = value
        else:
            parsed = value
        with self.conn:
            self.conn.execute(
                "UPDATE settings SET value_json=?, updated_at=?, updated_by=? WHERE key=?",
                (json.dumps(parsed, ensure_ascii=False), utcnow_iso(), username, key),
            )
        log.info("Настройка %s изменена пользователем %s", key, username)
