"""Seven-day weather snapshots and monitor preferences; preserve configured coordinates."""
import json


def upgrade(conn):
    defaults = {
        'weather.latitude': (44.423062, 'Широта точки прогноза'),
        'weather.longitude': (34.06452, 'Долгота точки прогноза'),
        'weather.wind_warning_ms': (8, 'Сильный ветер, м/с; предупреждение'),
        'weather.frost_warning_c': (2, 'Риск заморозка, °C; предупреждение'),
        'weather.heat_warning_c': (32, 'Жара, °C; предупреждение'),
        'weather.rain_warning_mm': (5, 'Заметные осадки за 24 часа, мм'),
        'dashboard.refresh_sec': (5, 'Интервал обновления монитора, секунд'),
        'dashboard.columns': (4, 'Колонки плиток на широком мониторе'),
        'dashboard.pause_minutes': (60, 'Пауза с плитки, минут'),
        'dashboard.manual_seconds': (60, 'Ручной полив по умолчанию, секунд'),
    }
    for key, (value, description) in defaults.items():
        conn.execute('INSERT OR IGNORE INTO settings(key,value_json,description,updated_at) VALUES(?,?,?,datetime(\'now\'))',
                     (key, json.dumps(value), description))
        if key in ('weather.latitude', 'weather.longitude'):
            conn.execute("UPDATE settings SET value_json=? WHERE key=? AND value_json='null'", (json.dumps(value), key))
    conn.execute('''CREATE TABLE IF NOT EXISTS weather_outlooks (
        latitude REAL NOT NULL, longitude REAL NOT NULL, fetched_at TEXT NOT NULL,
        payload_json TEXT NOT NULL, PRIMARY KEY(latitude,longitude))''')
