"""Клиент Open-Meteo (Этап 7): фактическая погода и прогноз без API-ключа.

Требования промпта Этапа 7, Шаг 2:
- синхронный httpx, таймаут 10 сек;
- fetch_current(lat, lon) — текущие условия;
- fetch_forecast(lat, lon, hours=72) — почасовой прогноз;
- сетевые/HTTP ошибки -> None + лог (graceful degradation: сервер работает
  без интернета, компилятор применяет offline_policy);
- User-Agent с версией приложения;
- кэш ответов в памяти на 5 минут (компиляция не дёргает API на каждый бокс).

Формат ответов Open-Meteo (current / hourly): значения лежат в p["current"]
или p["hourly"] параллельными массивами; здесь нормализуются в плоские словари
(temps/humidity/precip/wind/condition по часам), чтобы weather_service не
зависел от формата внешнего API.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Optional

import httpx

from .logging import get_logger

log = get_logger("poliv.open_meteo")

BASE_URL_CURRENT = "https://api.open-meteo.com/v1/forecast"
BASE_URL_FORECAST = "https://api.open-meteo.com/v1/forecast"
TIMEOUT_SEC = 10.0
CACHE_TTL_SEC = 300  # 5 минут

# current_weather отдаёт эти поля; прогнозный набор — hourly.
CURRENT_PARAMS = ("temperature_2m", "relative_humidity_2m", "wind_speed_10m",
                  "precipitation", "weather_code")
HOURLY_PARAMS = ("temperature_2m", "relative_humidity_2m",
                 "precipitation_probability", "precipitation", "weather_code")


class _TtlCache:
    """Простейший потокобезопасный кэш {key: (expires_at, value)}."""

    def __init__(self, ttl_sec: float = CACHE_TTL_SEC):
        self._ttl = ttl_sec
        self._data: dict[Any, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item and item[0] > time.monotonic():
                return item[1], True
            if item:
                self._data.pop(key, None)
        return None, False

    def put(self, key, value):
        with self._lock:
            self._data[key] = (time.monotonic() + self._ttl, value)

    def clear(self):
        with self._lock:
            self._data.clear()


_cache = _TtlCache()


def app_version_tag() -> str:
    """Версия для User-Agent; при недоступности main (тесты) — неизвестно."""
    try:
        from server import main as _main
        return getattr(_main, "APP_VERSION", "unknown")
    except Exception:
        return "unknown"


def _user_agent() -> str:
    return f"poliv-server/{app_version_tag()} (OpenMeteoClient)"


def _http_get(url: str, params: dict) -> Optional[dict]:
    """GET c кэшем по (url, params). Ошибка сети/HTTP -> None (не исключение)."""
    key = (url, tuple(sorted(params.items())))
    cached, hit = _cache.get(key)
    if hit:
        return cached
    try:
        resp = httpx.get(url, params=params, timeout=TIMEOUT_SEC,
                         headers={"User-Agent": _user_agent()})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:                      # сеть, таймаут, 4xx/5xx, JSON
        log.warning("Open-Meteo: запрос не удался (%s %s): %s",
                    url, params.get("latitude"), exc)
        return None
    _cache.put(key, data)
    return data


def fetch_current(lat: float, lon: float) -> Optional[dict]:
    """Текущая погода: temp_c, humidity_pct, wind_ms, precip_mm, condition_code.

    Возвращает None при любой ошибке (офлайн-режим, битные координаты).
    """
    if lat is None or lon is None:
        return None
    params = {
        "latitude": lat, "longitude": lon,
        "current": ",".join(CURRENT_PARAMS), "timezone": "auto",
    }
    p = _http_get(BASE_URL_CURRENT, params)
    if not p or "current" not in p:
        return None
    cur = p["current"]
    return {
        "observed_at": cur.get("time"),
        "temp_c": cur.get("temperature_2m"),
        "humidity_pct": cur.get("relative_humidity_2m"),
        "wind_ms": _kmh_to_ms(cur.get("wind_speed_10m")),
        "precip_mm": cur.get("precipitation"),
        "condition_code": cur.get("weather_code"),
        "raw": p,
    }


def fetch_forecast(lat: float, lon: float, hours: int = 72) -> Optional[list[dict]]:
    """Почасовой прогноз: список [{time, temp_c, humidity_pct,
    precip_probability_pct, precip_mm, condition_code}].

    None — при ошибке; пустой список маловероятен (API всегда отдаёт часы).
    """
    if lat is None or lon is None:
        return None
    days = max(1, min(16, (int(hours) + 23) // 24))   # Open-Meteo: forecast_days
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": ",".join(HOURLY_PARAMS),
        "forecast_days": days, "timezone": "auto",
    }
    p = _http_get(BASE_URL_FORECAST, params)
    if not p or "hourly" not in p:
        return None
    h = p["hourly"]
    times = h.get("time") or []
    out: list[dict] = []
    for i, t in enumerate(times):
        out.append({
            "time": t,
            "temp_c": _at(h.get("temperature_2m"), i),
            "humidity_pct": _at(h.get("relative_humidity_2m"), i),
            "precip_probability_pct": _at(h.get("precipitation_probability"), i),
            "precip_mm": _at(h.get("precipitation"), i),
            "condition_code": _at(h.get("weather_code"), i),
        })
    return out[:max(1, int(hours))]


def _at(arr, i):
    if arr is None or i >= len(arr):
        return None
    return arr[i]


def _kmh_to_ms(v):
    return round(float(v) / 3.6, 2) if v is not None else None


def reset_cache() -> None:
    """Для тестов: очистить кэш ответов."""
    _cache.clear()
