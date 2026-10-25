"""Миграция 0009_stage7 — Этап 7: погодные корректировки и сезонность.

Таблицы (по Артефакту 0.4 §5.9 и промпту Этапа 7):
- weather_observations      — ФАКТ погоды (наблюдения). Колонки ts +
  precipitation_mm обязательны: их читает core/weather_rules.latest_rain_ts
  (дождевая задержка rain_delay, компилятор Этапа 4) — имена согласованы,
  обратная совместимость сохранена;
- weather_forecasts         — ПРОГНОЗ (valid_from..valid_to, Open-Meteo);
- weather_rules             — правила пользователя (условие → действие
  skip / reduce_percent / increase_percent, область global/controller/zone);
- weather_rule_applications — журнал применения правил к машинограммам
  (до/после хеш, затронутые зоны).

Настройки weather.* (Артефакт 0.7 §17.7) — в таблице settings; значения по
умолчанию соответствуют config.default.toml-подходу (секция [weather] в
config НЕ трогается — разрешено только чтение cfg.raw.get("weather", {})).

Миграция идемпотентна (CREATE TABLE IF NOT EXISTS / INSERT OR IGNORE) и
ОБРАТИМА: downgrade удаляет таблицы и настройки weather.* (данные наблюдений
при откате теряются — это осознанно: без схемы Этап 7 не работает).
"""
from __future__ import annotations

import json
import sqlite3

# Значения по умолчанию — Артефакт 0.7 §17.7 (Этап 7).
DEFAULT_WEATHER_SETTINGS = {
    "weather.enabled": True,               # главный тумблер поправок
    "weather.latitude": None,              # координаты точки прогноза
    "weather.longitude": None,
    "weather.update_interval_min": 60,     # период обновления факт/прогноз
    "weather.offline_policy": "use_last",  # use_last | disable_corrections
    "weather.rules_enabled": True,         # применять ли weather_rules
}

SQL = [
    """
    CREATE TABLE IF NOT EXISTS weather_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id INTEGER REFERENCES controllers(id),  -- NULL = общий факт
        observed_at TEXT NOT NULL,          -- ISO-время наблюдения (человек.)
        ts INTEGER NOT NULL,                -- unix-секунды (для rain_delay)
        temp_c REAL,
        humidity_pct REAL,
        precip_mm REAL,                     -- осадки периода (Open-Meteo)
        precipitation_mm REAL,              -- то же, имя для latest_rain_ts
        wind_ms REAL,
        source TEXT NOT NULL DEFAULT 'open_meteo'
            CHECK (source IN ('open_meteo', 'manual')),
        raw_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_wo_ts ON weather_observations(ts)",
    "CREATE INDEX IF NOT EXISTS idx_wo_observed ON weather_observations(observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_wo_controller ON weather_observations(controller_id)",
    """
    CREATE TABLE IF NOT EXISTS weather_forecasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        controller_id INTEGER REFERENCES controllers(id),  -- NULL = общий прогноз
        forecasted_at TEXT NOT NULL,        -- когда получен прогноз
        valid_from TEXT NOT NULL,           -- ISO начало периода действия
        valid_to TEXT NOT NULL,             -- ISO конец периода действия
        temp_c REAL,
        humidity_pct REAL,
        precip_probability_pct REAL,
        precip_mm REAL,
        condition_code INTEGER,             -- WMO code (Open-Meteo)
        source TEXT NOT NULL DEFAULT 'open_meteo'
            CHECK (source IN ('open_meteo', 'manual')),
        raw_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_wf_valid_from ON weather_forecasts(valid_from)",
    "CREATE INDEX IF NOT EXISTS idx_wf_valid_to ON weather_forecasts(valid_to)",
    "CREATE INDEX IF NOT EXISTS idx_wf_controller ON weather_forecasts(controller_id)",
    """
    CREATE TABLE IF NOT EXISTS weather_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        priority INTEGER NOT NULL DEFAULT 100,      -- меньше = раньше применяется
        condition TEXT NOT NULL CHECK (condition IN
            ('rain', 'precip_probability', 'high_humidity', 'low_temp',
             'high_temp', 'wind')),
        operator TEXT NOT NULL DEFAULT 'gt'
            CHECK (operator IN ('gt', 'lt', 'eq', 'gte', 'lte')),
        threshold REAL NOT NULL,
        action TEXT NOT NULL CHECK (action IN
            ('skip', 'reduce_percent', 'increase_percent')),
        adjustment_value INTEGER NOT NULL DEFAULT 0, -- проценты; 0 для skip
        scope TEXT NOT NULL DEFAULT 'global'
            CHECK (scope IN ('global', 'controller', 'zone')),
        scope_id INTEGER,                            -- контроллер/зона для scope
        created_by TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_wr_condition ON weather_rules(condition)",
    "CREATE INDEX IF NOT EXISTS idx_wr_enabled ON weather_rules(enabled)",
    """
    CREATE TABLE IF NOT EXISTS weather_rule_applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_id INTEGER REFERENCES weather_rules(id),
        schedule_version INTEGER,           -- версия машинограммы (до присвоения
                                            -- может быть NULL — preview/test)
        controller_id INTEGER REFERENCES controllers(id),
        applied_at TEXT NOT NULL DEFAULT (datetime('now')),
        before_hash TEXT,
        after_hash TEXT,
        affected_zones_json TEXT,           -- [{program_id, zone_numbers...}]
        details_json TEXT                   -- действие, значения до/после
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_wra_applied ON weather_rule_applications(applied_at)",
    "CREATE INDEX IF NOT EXISTS idx_wra_rule ON weather_rule_applications(rule_id)",
]


def upgrade(conn: sqlite3.Connection) -> None:
    for stmt in SQL:
        conn.execute(stmt)
    for key, value in DEFAULT_WEATHER_SETTINGS.items():
        conn.execute(
            """INSERT OR IGNORE INTO settings (key, value_json, description, updated_at)
               VALUES (?, ?, ?, datetime('now'))""",
            (key, json.dumps(value), f"Этап 7: {key}"),
        )


def downgrade(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS weather_rule_applications")
    conn.execute("DROP TABLE IF EXISTS weather_rules")
    conn.execute("DROP TABLE IF EXISTS weather_forecasts")
    conn.execute("DROP TABLE IF EXISTS weather_observations")
    conn.execute("DELETE FROM settings WHERE key LIKE 'weather.%'")
