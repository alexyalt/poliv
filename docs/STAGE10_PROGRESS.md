# Этап 10 — состояние и восстановление

Ветка: `feature/stage10-esp32`, база: `5977a3f`.

- [x] Переносимое ядро: water/soak/gap, mono timeout, manual с возвратом,
  pause/resume, stop, fault, duration limit, recovery skip/resume.
- [x] Native-тесты, включая fixture настоящего server compile/persist и canonical hash.
- [x] ESP32 74HC595, калибруемый pulse flow, watchdog, независимый deadline timer.
- [x] MQTT runtime, CRC flash, cursor/events/command dedup, NVS fault/pause.
- [x] ESP32 build, service default, документация, CI workflow.
- [ ] Проверка физической платы, полярности, расходомера, reset/OE, power cut.
- [ ] Реальный ESP32↔broker↔server E2E; сценарий смены активного расписания.
- [ ] DS3231/кнопка failsafe, auto-backup/OTA runtime (границы: ADR-20).

Сборка: `pio run -d firmware -e esp32dev`.
Native: `pio test -d firmware -e native`.
Сервер: `python -m pytest server/tests -q` (последний полный: 287 passed/1 skipped).
Инструкция: `docs/ИНСТРУКЦИЯ_ЭТАП10.md`, решения/ограничения: `docs/adr/ADR-20-stage10-firmware.md`.

Этап 10 не объявлять принятым до стендовой проверки. Новая версия при активном run
отклоняется; после завершения нужна повторная отправка. Потеря питания default skip,
resume опционален и может повторить время с последнего 5-секундного checkpoint.
Секреты: firmware/include/secrets.h игнорируется Git. Никогда не сохранять GitHub token.
