"""Погодные правила (Этап 8 — полноценная реализация; Этап 4 — точка данных).

Для компилятора Этапа 4 нужен только один факт: когда шёл последний дождь
(настройка adjustment.rain_delay_hours — задержка полива после дождя).
Источники погодных данных (погода по API/датчики) появляются на Этапе 8;
до тех пор таблица weather_observations отсутствует и функция возвращает None
— дождевые пропуски не применяются.
"""
from __future__ import annotations

import sqlite3
from typing import Optional


def latest_rain_ts(conn: sqlite3.Connection) -> Optional[int]:
    """Unix-ts последнего наблюдения с осадками, или None (данных нет)."""
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if "weather_observations" not in tables:
        return None
    row = conn.execute(
        """SELECT MAX(ts) FROM weather_observations
           WHERE precipitation_mm > 0"""
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None
