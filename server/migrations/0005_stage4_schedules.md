# 0005_stage4_schedules — машинограммы и базовое исполнение (Этап 4)

## Назначение

Миграция добавляет объекты, необходимые компилятору машинограмм и раздаче
расписаний контроллерам (ТЗ «Этап 4. Машинограммы и базовая логика исполнения»,
Артефакт 0.7 §8.1 `controller_schedules`, Артефакт 0.5 §3 формат машинограммы).

## Состав

### Таблица `controller_schedules` (метаданные машинограмм)

| Колонка | Смысл |
|---|---|
| `controller_id` | контроллер-получатель |
| `schedule_version` | монотонная версия на контроллер (§3.10 ТЗ); UNIQUE(controller_id, schedule_version) |
| `valid_from_date`, `valid_to_date` | период действия (локальные даты) |
| `timezone_offset_min` | часовой сдвиг на момент компиляции |
| `source` | auto / manual / weather / normalization / offline_export |
| `status` | draft → compiled → sent → acknowledged; failed — ошибка доставки/подтверждения |
| `schedule_hash` | sha256 канонического JSON содержимого (контроллер проверяет целостность) |
| `payload_path` | путь к файлу машинограммы `data/schedules/{box_id}/v<N>.json` (содержимое — файлом, в БД метаданные) |
| `runs_count`, `warnings_json`, `errors_json`, `error_message` | сводка компиляции |
| `created_by`, `created_at`, `sent_at`, `acknowledged_at` | жизненный цикл |

### Таблица `watering_runs` (журнал запусков полива)

План/факт каждого запуска: `run_id` (uuid), `program_id`, `schedule_id/version`,
`source` (schedule/manual/…), `status` (planned/active/completed/skipped/aborted/failed),
`reason_code` (rain_delay / zone_locked / disabled), временные метки и состав зон.
Нужна для критериев готовности Этапа 4 («программа стартует», пауза ручного
режима, остановка) и для отчётов Этапов 6–7.

### Настройки (`settings`, INSERT OR IGNORE)

- `schedule.apply_policy` — `"next_run"` (по умолчанию): новая версия
  расписания применяется после завершения текущего активного прогона
  (безопасная политика из п. 4 ТЗ Этапа 4); значение `"immediate"` — немедленная
  замена непроставленных запусков;
- `schedule.min_duration_minutes` = 1, `schedule.max_duration_minutes` = 240 —
  клампинг длительностей шагов (§17.4 ТЗ).

## Идемпотентность

`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`,
`INSERT OR IGNORE` для настроек. Применённые миграции 0001–0004 не изменяются.
