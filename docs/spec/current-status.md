# Актуальное состояние после аудита — этап 8.S

База завершающей доработки: main `474d275` (слияние PR #19).
Предыдущие исправления: `aea4061`, `d898fab`. Хронология сохранена в WorkBook.
Этапы 0–8 реализованы; 9A/9B и прошивка не входят в эту серию исправлений.
Матрица ниже отражает итоговый код, а не состояние первого прохода аудита.

| ID | Результат | Проверка / предел гарантии |
|---|---|---|
| F01 | Stop обходит ожидание ACK, старые повторы отменяются; async ёмкость 16 обычных + 64 stop, shutdown cleanup | Fake MQTT и настоящий приватный Mosquitto: независимый stop до окончания чужого таймаута |
| F02 | Preview передаёт правила в памяти, не пишет настройки | enabled-комбинации, исключение, изоляция выбранного правила |
| F03 | Архив v2 сохраняет родителей, зоны, NULL | CSV/JSON round-trip; исторические уже утраченные связи не выдумываются |
| F04 | Новая партия — отдельный immutable файл | Повтор за день оставляет валидный JSON и неизменный прежний файл |
| F05 | Durable files до удаления; журнал/delete атомарны под BEGIN IMMEDIATE | Corruption, manifest/journal failure, concurrent rotation; аппаратный power-cut отдельно |
| F06 | Факт event имеет приоритет над run fallback; parallel не умножает общий объём | 50 л остаются 50 л; исторические child volumes нормализуются. Доли зон — оценка, не измерение каждого клапана |
| F07 | ScopedConnection привязан к запросу/задаче/потоку; состояние nested transaction принадлежит native conn | Concurrent rollback/commit; две корутины одного потока; child tasks; закрытие в finally |
| F08 | ACK/compile/preview/report/heatmap/weather/archive вне event loop | Медленный отчёт не блокирует независимый HTTP; отдельные соединения в workers |
| F09 | Новые runtime и local config игнорируются; добавлен безопасный перенос с SQLite backup API | Ранее отслеженные runtime пока сохранены. Перед удалением из индекса владелец установленной системы должен перенести их; см. процедуру ниже |
| F10 | v2 NULL-mask, схема колонок и transactional restore | NULL/пустая строка/тип/конфликт ID; legacy помечается incomplete |
| F11 | Фабрика FK/WAL/busy_timeout и scopes во всех основных сервисных путях | FK-тесты, upgrade копии старой БД, изоляция транзакций |
| F12 | Временный пароль требует собственной смены, другие сессии отзываются | HTTP password flow и роли после смены |
| F13 | Secure cookie HTTPS/config; Origin/Referer/Sec-Fetch-Site | HTTP-тесты; headerless API поддержан; reverse proxy и production-browser проверяет оператор |
| F14 | Неверный JSON/типы не запускают ротацию, ответ 422 | malformed/list/null/string bool/date/unknown поля, без удаления |
| F15 | Новый archive.path от data_dir, legacy пути читаются из журнала | prepare_runtime переводит скопированные локальные file_path/payload_path на новый root, v2 хеши сверяются |
| F16 | requirements.lock, Python 3.13, активный `.github/workflows/tests.yml` | CI: полный pytest/migrations, браузер Chromium, настоящий loopback Mosquitto; статус запуска проверяется в Actions |

## Дополнения этапа 8

- `processed_events` (миграция 0012): независимые markers event/flow/schedule не
  удаляются архивом. Replay после ротации не увеличивает статистику; прерывание
  между сохранением event и flow допускает возобновление flow без удвоения.
  Backfill возможен только по сохранившимся watering_events, не по исчезнувшим UID.
- Седьмой отчёт missed в JSON/CSV/UI показывает пропуски/отмены и причину.
- Heatmap «7 дней» выполняет последовательные дневные запросы; Chromium smoke
  проверяет семь таблиц, раскрытие зон и missed-tab без JS ошибок с mock API.
  Offline — текущий индикатор, не реконструкция истории связи.
- Restore реализован для events/notifications/logs/flow/weather. Legacy-ограничения
  по NULL/связям и повреждённому JSON остаются явно отмеченными.
- Архив выбирает до 1000 строк на партию; rotate_all до 100 партий на тип за цикл.
  Оставшийся backlog остаётся в БД; одна bounded партия держит write lock во время IO.
- Сессия touch ограничена интервалом, cookie продлевается с активной сессией.
  `/health` — liveness; `/ready` проверяет БД/MQTT и возвращает 503 без брокера.

## Runtime-файлы в Git: миграция без потери рабочей БД

Удаляющий commit по уже отслеженным data/logs мог бы стереть их при pull
существующей установки. Поэтому в этой серии исходные файлы не удаляются и история
Git не переписывается. Программная подготовка переноса реализована; действие на
реальной установленной системе выполняет её владелец:

1. Остановить приложение для согласованности schedules/архивных файлов; сохранить
   config.local.toml и все пользовательские абсолютные пути вне checkout отдельно.
2. Выполнить `python -m server.tools.prepare_runtime --destination /srv/poliv-runtime`.
   Путь должен быть новым и находиться вне checkout. Используется SQLite backup API.
3. Утилита проверяет integrity/FK, переводит локальные ссылки controller_schedules и
   archive_exports на копии, сверяет checksum пары v2. В исходной БД ничего не меняет.
   Внешние ссылки перечисляются в `external_files` и требуют отдельного копирования.
4. Проверить `runtime-copy.json`: `ready=true`. При ошибке остаётся `ready=false`;
   такую копию не использовать. Проверить доступность внешних файлов и восстановление
   на отдельной тестовой установке. Существующий destination не перезаписывается.
5. Настроить POLIV_DATA_ROOT до импорта приложения, согласовать config absolute paths.
   Только после этого можно отдельным deployment-коммитом убрать runtime из индекса.
   Не выполнять git rm над единственной рабочей копией.

Публичная data/poliv.db в истории не production seed. Чистая установка использует
новый root/собственный config и создаёт нового admin. Старые секреты из Git-истории,
если они были, требуют отдельного отзыва; переписывание истории здесь не выполняется.

## Проверки и CI

Workflow запускается на push main/fix/**, pull_request и вручную. Доступ contents:read,
без PAT/secrets, без автоматического deploy. Jobs:

- tests: lock install, pip check, полный pytest (включая миграции), неизменность runtime,
  проверка whitespace коммита;
- browser: отдельный pinned Playwright/Chromium, day/week/missed smoke с mock API;
- mqtt: настоящий отдельный Mosquitto на случайном loopback-порту, hello/ACK/QoS1 retry,
  независимый stop и replay после архива. Железо или production broker не используются.

Локальные команды: `python -m pytest server/tests -q --disable-warnings --tb=short`,
`python -m server.tools.browser_smoke`, `python -m server.tools.mqtt_smoke`.
Браузеру нужны optional Playwright/Chromium/deps; MQTT smoke нужен executable mosquitto.
Точные результаты каждого прохода — в WorkBook, remote результат — в GitHub Actions.

## Обновление и откат

Backup до обновления. Python 3.13, requirements.lock, один worker Uvicorn до отдельной
приёмки multi-process lifecycle. 0011/0012 additive: старые файлы/пути не перемещаются
при миграции; ledger при downgrade сохраняется. После переноса через prepare_runtime
меняются только пути в копии. Для v2 restore сохранить основной файл + manifest + БД.
Старый writer/daemon нельзя включать при откате архивного кода: он небезопасно пишет
дневной append. Подробнее — ADR-18 и его дополнения.

Реальный Mosquitto проверен изолированно. ESP32/клапаны, brownout/power loss,
производственные TLS/ACL, reverse proxy, Windows и длительная production нагрузка
не проверялись. Regression 50×16×96 входит в полный pytest, но не заменяет нагрузку
с реальной телеметрией. Эти эксплуатационные проверки не объявлены выполненными.
