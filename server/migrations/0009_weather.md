# 0009_weather — Этап 7: погодные корректировки

## Назначение
Схема данных погоды и правил корректировки полива (ТЗ Этап 7, Артефакт 0.4 §5.9,
Артефакт 0.7 §17.7):

| Таблица | Роль |
|---|---|
| `weather_observations` | Факт: температура, влажность, осадки, ветер; `ts`/`precipitation_mm` совместимы с `core/weather_rules.latest_rain_ts` (rain_delay Этапа 4) |
| `weather_forecasts` | Прогноз на период `valid_from..valid_to` (Open-Meteo, почасовой) |
| `weather_rules` | Правила пользователя: условие (rain / precip_probability / high_humidity / low_temp / high_temp / wind) + оператор (gt/lt/eq/gte/lte) + порог → действие (skip / reduce_percent / increase_percent), область (global/controller/zone), приоритет |
| `weather_rule_applications` | Журнал применения: правило, версия машинограммы, хеш до/после, затронутые зоны |

## Настройки (таблица settings, ключи weather.*)
- `weather.enabled` (bool, по умолчанию true);
- `weather.latitude`, `weather.longitude` (float|null);
- `weather.update_interval_min` (int, 60);
- `weather.offline_policy` (`use_last` | `disable_corrections`);
- `weather.rules_enabled` (bool, true).

Ключи регистрируются миграцией через `INSERT OR IGNORE`. Секция `[weather]`
в `config/config.default.toml` НЕ создаётся (запрет этапа) — значения из БД.

## Идемпотентность и обратимость
- `upgrade`: только `CREATE TABLE/INDEX IF NOT EXISTS` и `INSERT OR IGNORE`;
- `downgrade`: удаление четырёх таблиц и ключей `weather.%` из settings.
