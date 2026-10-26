# Актуальное состояние 8.S

База реализации: e026ee4b890c677ded9ab726b43c7caa497a5204. Ветка:
`fix/stage8-audit-stabilization`. Этапы 0–8 реализованы функционально;
эксплуатационная стабилизация продолжается. 9A/9B и прошивка здесь не реализуются.
См. ADR-18 и последнюю запись WorkBook для точных результатов тестов.

| ID | Состояние в этой ветке | Реализация / проверка | Остаток |
|---|---|---|---|
| F01 | Исправлена общая блокировка ACK | единый `_execute_command`, per-box lane, stop generation; тесты двух/одного контроллера | предел числа daemon command workers, физическая задержка MQTT |
| F02 | Исправлено | явный weather_rules override; 4 комбинации enabled, исключение, другое правило | нагрузка фоновой компиляции |
| F03 | Исправлено для новых архивов | v2 manifest с watering_event_zones; CSV/JSON round-trip | старые утраченные связи автоматически не восстановить |
| F04 | Исправлено для новых архивов | уникальный неизменяемый файл партии; повторная выгрузка за день | ручная диагностика повреждённых legacy JSON |
| F05 | Базовый протокол целостности реализован | отдельный BEGIN IMMEDIATE, durable files до удаления, журнал+delete в одной транзакции; сбои manifest/журнала, конкуренция | реальное отключение питания, Windows durability, оптимизация длительного write lock |
| F06 | Двойной учёт исправлен | факт event приоритетен над run fallback, 50 л остаются 50 л | точная атрибуция параллельных зон и сверка аварий всех отчётов |
| F07 | Частично | архив/refresh/account middleware имеют отдельные conn | остальные auth/catalog/event/flow/notification ещё делят conn; нужен отдельный PR |
| F08 | Частично | weather refresh и rotate вынесены в threadpool с собственными conn | другие тяжёлые async отчёты/компиляция |
| F09 | Частично | ignore local config/runtime/WAL/SHM дополнен | ранее отслеженные данные сохранены для безопасного обновления; ниже процедура |
| F10 | Исправлено для v2 | NULL mask, схема колонок, rollback конфликта | неоднозначные legacy NULL/пустые строки |
| F11 | Исправлена единая политика | connect_path: FK, WAL, busy_timeout; основные фоновые открытия используют фабрику | это не закрывает F07 |
| F12 | Исправлено в основном приложении | middleware + self-service /account/password, отзыв других сессий | браузерная приёмка |
| F13 | Усилено | Secure на HTTPS/config, Origin/Referer и Sec-Fetch-Site | проверка proxy/браузеров, CSP отдельно; headerless API разрешён |
| F14 | Исправлено | строгие поля/типы запроса, malformed JSON →422 | Pydantic можно выделить позже без смены контракта |
| F15 | Исправлено для новых файлов | API и daemon передают cfg.data_dir | старые пути читаются из журнала, не перемещаются |
| F16 | Реализовано | requirements.lock, Python 3.13, шаблон CI и чистая среда | CI не активирован: токен не имеет workflow scope; другие Python/ОС не объявлены проверенными |

## Трассировка требований

| Требование | Код | Тест / уровень | Ограничение |
|---|---|---|---|
| Stop не ждёт чужого ACK | mqtt_command_service._execute_command | test_stop_bypasses_ack_wait_and_cancels_earlier_retries; fake MQTT threads | физическая плата не проверялась |
| Preview не меняет конфигурацию | routes_weather.rules_test, compiler.weather_rules | test_weather_preview_never_changes_settings, test_preview_exception_keeps_global_state; HTTP/SQLite | реальная сеть не нужна |
| Факт расхода один раз | report_service.report_by_zones | test_zone_report_counts_fact_once; SQL | parallel attribution отдельно |
| Обратимый архив | archive_service, archive_integrity | typed_relational_roundtrip; CSV/JSON | только events/notifications/logs restore |
| Отказоустойчивый архив | integrity.transaction/durable_write | corruption, write/journal failure, concurrent rotation | crash/power стенд отдельно |
| Миграция без удаления | 0011_archive_integrity | upgrade_preserves_legacy_journal_and_reader | additive metadata сохраняется при downgrade |
| Временный пароль | account_security | password_change_guard_and_other_sessions_revoked; HTTP | main регистрирует middleware, голые тестовые harness — отдельно |
| Происхождение запроса | account_security | origin_rejected_before_mutation | headerless API совместим |
| Седьмой missed-report | этап 0 / этап 8 | открыто | skips_count не заменяет отдельную приёмку |
| Недельная heatmap | этап 0 / этап 8 | открыта UI-приёмка | дневной API не доказывает неделю |
| Устойчивые event_uid после архива | MQTT dispatcher/event service | открыто | UNIQUE удаляемой таблицы недостаточен |

## Runtime-файлы в Git: миграция без потери рабочей БД

В этой ветке намеренно не создаётся deletion commit для уже отслеженных data/logs:
при обычном pull он мог бы удалить файлы существующей установки. Новые runtime
файлы игнорируются. Полное закрытие F09 требует отдельной эксплуатационной миграции:

1. Остановить приложение либо создать консистентную SQLite backup API копию;
   сохранить config.local.toml, data, архив по обоим старому/новому путям и журналы.
2. Проверить восстановление копии в отдельный POLIV_DATA_ROOT.
3. Перенести рабочие данные в независимый каталог вне checkout, явно настроить пути.
4. Только после этого убрать точный перечень runtime из индекса Git отдельным PR.
   Не выполнять `git rm` над единственной рабочей копией данных.
5. Для чистой установки использовать новый data root. Не считать пользовательскую
   data/poliv.db в истории проекта production seed.

Не переписывать историю Git автоматически. Проверка старой истории на действующие
секреты и отзыв найденных токенов — отдельная задача.

## Обновление и откат

Сначала backup БД/архивов и тестовая копия. Установить requirements.lock на Python
3.13, выполнить полный pytest, затем один worker приложения. Миграция 0011 добавляет
два столбца, не меняет старые пути/файлы. Новые партии требуют companion manifest.
При откате сохранить новую БД и файлы: старый reader не знает v2 отношений и не
должен использоваться для restore новых партий. Старый daemon должен быть отключён
до отката архивного кода, иначе он снова будет писать небезопасный дневной append.

Архив не является резервной копией всей системы. Перед реальной водой нужны
Mosquitto/ESP32, браузерная и нагрузочная приёмки, безопасное отключение питания,
локальный failsafe; результат pytest этого не подтверждает.
