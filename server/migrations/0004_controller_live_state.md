# Миграция 0004_controller_live_state

## Зачем (причина)

Этап 3 вводит живой MQTT-обмен: сервер из сообщений `hello`/`status`/`lwt`/`flow`
должен хранить текущее состояние контроллера и отдавать view-модель
`GET /api/controllers/{id}/live` (Артефакт 0.6 §4.5). В `controllers` из 0001 есть
только базовые поля (`connection_status`, `last_seen_at`, `firmware_version`,
`schedule_version`, `time_valid`, `ip_address`), но нет полей режима/фазы/зон/таймеров/
расходомера.

**Правило процесса (AGENTS.md):** применённые миграции 0001–0003 не редактируются —
схема расширяется только новой миграцией 0004.

## Что делает

`ALTER TABLE controllers ADD COLUMN` (каждое — через `_has_column`, на новых БД
часть колонок уже существует из 0001 → no-op):

| Колонка | Тип | Источник |
|---|---|---|
| connection_status | TEXT DEFAULT 'offline' | hello/status → online, lwt/порог → offline |
| last_seen_at | TEXT | любое входящее сообщение контроллера |
| last_hello_at | TEXT | hello |
| firmware_version | TEXT | hello/status |
| schedule_version | INTEGER DEFAULT 0 | status/hello; заглушка Этапа 3 = 0 |
| schedule_hash | TEXT | status/hello |
| time_valid | INTEGER DEFAULT 0 | status |
| current_mode | TEXT | status.mode (idle/schedule/manual/paused/error/…) |
| current_phase | TEXT | status.phase (idle/water/soak/waiting/paused/error) |
| primary_zone | INTEGER | status.primary_zone |
| active_zones_json | TEXT | JSON-массив реально открытых зон |
| display_zones_json | TEXT | JSON-массив зон для отображения (впитывание) |
| phase_end_ts / run_end_ts | INTEGER | unix-таймеры фазы/запуска |
| pause_until_ts / emergency_lock_until_ts | INTEGER | пауза/аварийная блокировка |
| flow_enabled | INTEGER DEFAULT 0 | status/flow |
| flow_total_liters | REAL | суммарный литраж полива |
| instant_lpm | REAL | мгновенный расход |
| service_mode_active | INTEGER DEFAULT 0 | status.service_mode |
| ip_address | TEXT | hello (если прислан) |

Индексы (для фоновой офлайн-проверки раз в минуту):
`idx_controllers_conn_status (connection_status)`, `idx_controllers_last_seen (last_seen_at)`.

## Идемпотентность

- все ADD защищены `_has_column`;
- индексы — `CREATE INDEX IF NOT EXISTS`;
- повторный прогон upgrade() не меняет схему и не падает.

## Проверка после применения

`PRAGMA table_info(controllers)` содержит перечисленные колонки; в
`schema_migrations` появилась версия `0004_controller_live_state`.
Тесты: `server/tests/test_stage3.py::test_migration_0004_columns_and_indexes`.
