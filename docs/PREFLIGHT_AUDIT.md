# Аудит перед Windows / ESP32 ручной приёмкой

База 5923975, ветка fix/windows-preflight-audit. Проверка охватывает установку,
сервер/БД, MQTT/TLS/ACL, интерфейс, firmware build и recovery/reset. Это аудит кода
и воспроизводимых сценариев, не независимая сертификация и не физический стенд.

| Находка | Исправление / доказательство |
|---|---|
| Windows rename regression in runtime test | явное conn.close, sqlite context manager сам не закрывает файл |
| test harness оставлял DB/temp после close | shutdown command workers, conn.close и удаление своего temp |
| requirements.lock включал uvloop без платформы | sys_platform marker + colorama; тест Windows selection, Windows CI |
| clear-error снимал lock до устройства | dedicated ACK команда; 6 отрицательных outcomes сохраняют lock; completed снимает |
| error/config/cursor можно было reset как flow fault | whitelist локального reset; native regression |
| NVS putString пустой строки возвращает 0 даже при успехе | reset удаляет ключ через проверяемый remove вместо сравнения strlen |
| hello ESP32 не содержал обязательный online | исправлено hello envelope |
| глобальный Executor вызывал GPIO до setup | HAL initialized guard |
| cursor version mismatch молча пропускал restore | invalid_cursor fault, выходы закрыты |
| новый storage fault перекрывался старым NVS flow fault | новая ошибка имеет приоритет |
| FORMAT не удалял сохранённый storage fault | явный service FORMAT после успешного FS format снимает NVS fault |
| Thread._stop перекрывался Event | stop_event, проверен join |
| Windows broker пути с кавычками не читались Mosquitto | raw paths, реальный broker smoke с пробелами в пути |
| недельный snapshot терял день около полуночи | хранение 8 дней с выводом следующих 7 |
| ручной полив до валидного времени создавал невалидные события | открытие требует validTime |
| compile defaults зависели от общего #ifndef MQTT_HOST | независимые макро-defaults |

Проверки: полный pytest 308 passed/1 skipped на Linux до заключительных добавлений;
12 native passed, ESP32 build success; Chromium weather/monitor/dashboard/heatmap/
stage9, настоящий Mosquitto retries/stop/replay/reset и TLS/ACL; pip check.
Итоговые результаты и Windows CI фиксируются в WorkBook после заключительного прогона.

## Границы готовности

Готовится стенд Windows local UI + authenticated LAN MQTT + один ESP32 с USB
обновлением. Plaintext только доверенная LAN, UI localhost, никаких входящих WAN.

Открытые функциональные задачи не спрятаны за словом «готово»:
- Signed OTA/anti-rollback/A-B rollback/auto-backup отсутствуют. USB upgrade —
  единственный поддерживаемый способ этой приёмки. Не тестировать удалённое OTA.
- Schedule replacement во время active run отклоняется. Для теста дождаться idle,
  повторить отправку; next_run/server pending может требовать ручного повтора.
- Кнопка failsafe/DS3231, boot/reset/OE/полярность, flow и power-cut — физический стенд.
- Events: broker PUBACK не равен server durable ACK; атомарность event+cursor при
  power cut не доказана. Проверить повторные/пропавшие факты в журнале; production
  требует persistent outbox/протокола подтверждения/стендовых испытаний.
- 50 плиток проверены UI fixture, не 50 MQTT-контроллеров под длительной нагрузкой.
- Поддержка нового clear_error должна быть в прошивке из этой ветки. Старые firmware
  могут отвергнуть reset; сервер сохранит lock, а не покажет ложный успех.

Начало ручного тестирования: WINDOWS_MANUAL_TEST.md. Шаблон отчёта:
MANUAL_ACCEPTANCE_REPORT.md. Полевой непрерывный режим требует отдельного решения.

## Подтверждённый Windows CI

Коммит 4011035: Windows job SUCCESS — установка locked dependencies, pip check,
полный pytest, Chromium weather/monitor/reset, ESP32 cross-build, PowerShell parse.
Также SUCCESS audit-checks (tests/browser/MQTT) и firmware-checks.
Windows run: https://github.com/alexyalt/poliv/actions/runs/40748512661 .
Это Windows runner без USB-платы; реальный COM/клапаны остаются ручной приёмкой.
