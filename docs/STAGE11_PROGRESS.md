# Этап 11 — состояние и продолжение

Ветка: feature/stage11-dashboards, база fb54c1a (содержит этап 10).

Готово:
- `/` — плитки контроллеров с цветом/текстом статуса, зоной, потоком, старт/пауза/стоп.
- `/active-zones` — только online water, без paused/error/service; поток общий для контроллера.
- Обновление каждые 5 секунд, отказ сети блокирует управление, viewer read-only.
- Старт: выбор существующей включённой зоны и 1..3600 сек; paused — resume.
- MQTT ACL generator (изолированные учётные записи), systemd unit, snapshot/verify/restore.
- Инструкция: docs/ИНСТРУКЦИЯ_РЕАЛЬНАЯ_МАШИНА.md.
- Firmware: NVS срок паузы по duration_sec/until_ts, автоматическое возобновление.

Проверено: 290 pytest passed/1 skipped; 9 native passed; ESP32 build SUCCESS;
Chromium/FastAPI/SQLite: desktop/mobile, статус/поток, Start dialog, active polling,
offline disabled, viewer. CI job расширен dashboard smoke.

Не выполнено: аппаратная приёмка, реальный ESP32 MQTT E2E; TLS/provisioning/OTA
runtime/rollback, DS3231/failsafe button; испытания на 50 физических платах.
Этап 11 частично реализован и не объявляется полностью принятым. Stage12 требует
реального пилота; без оборудования его результаты не подменяются симуляцией.

Продолжение: сначала аппаратная матрица из инструкции; отдельно проектировать
OTA с проверкой подписи и rollback, и настройку без пересборки прошивки.
Текущая прошивка обновляется USB. Broker ACL и systemd проверены генерацией/
тестами, unit ещё требует реального host service test.

## Продолжение 28.10.2026 — USB provisioning / TLS

Реализовано: service-only USB CONFIG/NETWORK_RESET; отдельная CLI с getpass;
сетевой профиль одним NVS значением; строгая валидация длины/полей; ESP32 TLS
с CA/hostname без insecure, после NTP; сервер TLS >=1.2 с hostname verification;
broker generator --tls на 8883. Настройки переживают USB-прошивку. Физические
параметры реле остаются compile-time, NVS без шифрования. Wi-Fi captive portal
и массовый onboarding пока отсутствуют.

Проверено: 292 pytest passed / 1 skipped, 10 native passed, ESP32 SUCCESS.
Реальный Mosquitto: правильный CA, отказ чужого CA/hostname/пароля, собственные
события и запрет публикации в топик другой платы. Smoke включён в CI.
Инструкция: docs/ИНСТРУКЦИЯ_USB_TLS.md. Плата не проверена. OTA/rollback остаются
следующей программной задачей; предыдущий список «TLS/provisioning не выполнено»
теперь относится к аппаратной приёмке/массовому onboarding, не к отсутствию кода.
