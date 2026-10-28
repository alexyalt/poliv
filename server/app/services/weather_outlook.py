"""Cached seven-day model forecast. Advisories never change irrigation commands."""
from datetime import datetime, timezone, timedelta
import json
import math
from ..infra import open_meteo_client as meteo
from .weather_service import weather_config, get_weather_setting

HOURLY = ('temperature_2m', 'relative_humidity_2m', 'precipitation_probability',
          'precipitation', 'wind_speed_10m', 'wind_gusts_10m', 'wind_direction_10m',
          'vapour_pressure_deficit', 'soil_moisture_0_to_1cm', 'soil_temperature_0cm')
DAILY = ('temperature_2m_max', 'temperature_2m_min', 'precipitation_sum',
         'precipitation_probability_max', 'wind_speed_10m_max', 'wind_gusts_10m_max',
         'et0_fao_evapotranspiration', 'uv_index_max', 'sunrise', 'sunset')


def normalize(raw):
    if not isinstance(raw, dict) or not isinstance(raw.get('hourly'), dict) or not isinstance(raw.get('daily'), dict):
        return None
    def rows(section, keys, limit):
        data = raw[section]
        if not isinstance(data.get('time'), list):
            return []
        result = []
        for i, stamp in enumerate(data['time'][:limit]):
            if not isinstance(stamp, (str, int)):
                continue
            row = {'time': stamp}
            for key in keys:
                values = data.get(key)
                v = values[i] if isinstance(values, list) and i < len(values) else None
                row[key] = v if isinstance(v, str) or (isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)) else None
            result.append(row)
        return result
    hourly, daily = rows('hourly', HOURLY, 192), rows('daily', DAILY, 8)
    if not hourly or not daily:
        return None
    return {'timezone': raw.get('timezone', 'UTC'), 'utc_offset_seconds': raw.get('utc_offset_seconds', 0),
            'hourly': hourly, 'daily': daily}


def update(conn):
    cfg = weather_config(conn)
    if not cfg['enabled'] or cfg['latitude'] is None or cfg['longitude'] is None:
        return False
    lat, lon = cfg['latitude'], cfg['longitude']
    raw = meteo._http_get(meteo.BASE_URL_FORECAST, {
        'latitude': lat, 'longitude': lon, 'timezone': 'auto', 'timeformat': 'unixtime',
        'forecast_days': 8, 'wind_speed_unit': 'ms',
        'hourly': ','.join(HOURLY), 'daily': ','.join(DAILY)})
    payload = normalize(raw)
    if payload is None:
        return False
    with conn:
        conn.execute('INSERT OR REPLACE INTO weather_outlooks VALUES(?,?,?,?)',
                     (lat, lon, datetime.now(timezone.utc).isoformat(), json.dumps(payload, allow_nan=False)))
        # Bounded cache; switching locations cannot display another location's weather.
        conn.execute('DELETE FROM weather_outlooks WHERE rowid NOT IN (SELECT rowid FROM weather_outlooks ORDER BY fetched_at DESC LIMIT 8)')
    return True


def view(conn, now=None):
    now = now or datetime.now(timezone.utc)
    cfg = weather_config(conn)
    lat, lon = cfg['latitude'], cfg['longitude']
    result = {'latitude': lat, 'longitude': lon, 'status': 'unavailable', 'fetched_at': None,
              'hourly': [], 'daily': [], 'advisories': [], 'timezone': 'UTC'}
    if not cfg['enabled']:
        result['status'] = 'disabled'
        return result
    row = conn.execute('SELECT * FROM weather_outlooks WHERE latitude=? AND longitude=?', (lat, lon)).fetchone()
    if row is None:
        return result
    try:
        fetched = datetime.fromisoformat(row['fetched_at'])
        payload = json.loads(row['payload_json'])
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        future = [h for h in payload['hourly'] if isinstance(h.get('time'), (int, float)) and h['time'] >= int(now.timestamp()) // 3600 * 3600]
        daily = [d for d in payload['daily'] if isinstance(d.get('time'), (int, float)) and d['time'] + 86400 > now.timestamp()][:7]
    except (ValueError, TypeError, KeyError):
        return result
    result.update(payload)
    result.update(hourly=future[:24], daily=daily, fetched_at=row['fetched_at'],
                  status='fresh' if timedelta(seconds=-30) <= now-fetched <= timedelta(hours=3) and len(future)>=24 and len(daily)>=7 else 'stale')
    def numbers(key):
        return [h[key] for h in result['hourly'] if isinstance(h.get(key), (int, float)) and math.isfinite(h[key])]
    def setting(key, default):
        return get_weather_setting(conn, 'weather.'+key, default)
    temp, wind, rain, vpd = (numbers(k) for k in ('temperature_2m', 'wind_gusts_10m', 'precipitation', 'vapour_pressure_deficit'))
    notices = []
    if temp and min(temp)<=setting('frost_warning_c',2):
        notices.append({'kind':'danger','text':'Риск заморозка: минимум '+str(round(min(temp),1))+' °C. Проверьте укрытия.'})
    if temp and max(temp)>=setting('heat_warning_c',32):
        notices.append({'kind':'warning','text':'Жара: проверьте влажность почвы и состояние молодых посадок.'})
    if wind and max(wind)>=setting('wind_warning_ms',8):
        notices.append({'kind':'warning','text':'Сильные порывы: дождевание может быть неравномерным; проверьте опоры растений.'})
    if len(rain)==24 and sum(rain)>=setting('rain_warning_mm',5):
        notices.append({'kind':'info','text':f'Осадки за 24 ч: {sum(rain):.1f} мм. Проверьте необходимость полива.'})
    if vpd and max(vpd)>1.6:
        notices.append({'kind':'warning','text':'Высокий дефицит давления пара (>1,6 кПа): возможен водный стресс.'})
    result['advisories'] = notices
    result['rain_24h_mm'] = round(sum(rain),1) if len(rain)==24 else None
    return result
