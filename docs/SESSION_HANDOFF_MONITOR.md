# Продолжение после сессии: монитор/погода

Рабочая ветка `feature/weather-monitor`; включает stage10/stage11.
Не сливать main до ревью/приёмки пользователем согласно AGENTS.

## Сделано

Монитор 50 плиток в browser fixture, error/buffer diagnostics, problem priority,
фильтр/fullscreen, компактные формы settings с валидируемыми координатами,
прогноз Open-Meteo 24ч/7д, wind/gusts/direction, precipitation/probability,
ET0/VPD/humidity/soil/UV, stale/null/location cache, рекомендации.
Сохранённая диагностическая ошибка отображается с расшифровкой на плитке.
Firmware принимает server command/time без box_id в теле, проверяя адрес топика;
manual finished ссылается на started_event_uid.

## Открытые задачи по исходным этапам (НЕ объявлять завершёнными)

1. OTA с проверкой подписи образа и anti-rollback, серверное хранение/выдача,
   доверенный ключ на устройстве, A/B rollback и auto-backup. Сейчас есть только
   разделы A/B и USB. Сначала согласовать модель ключей/обновления; проверить
   целостность/подпись на native и стенде, затем реализация приёма/подтверждения.
2. Аварийный reset согласовать end-to-end: endpoint clear-error сейчас снимает
   серверный lock и посылает ping, а реальная прошивка имеет собственный NVS fault.
   Нужна ACK-команда clear_error с проверкой физической причины и честным UI,
   не только сбросом серверного представления. Это блокер аппаратной приёмки.
3. Новая версия schedule при активном run сейчас отклоняется; pending/immediate
   semantics не соответствуют всем возможностям эмулятора. Добавить recovery tests
   с power cut между schedule/cursor commit и outbox server ACK для событий.
4. DS3231, физическая failsafe-кнопка, подтверждение boot/reset/OE/полярности,
   flow калибровка, cold boot/NTP/TLS и watchdog на стенде.
5. Пилот 50 настоящих контроллеров/нагрузочный тест брокера с длительной работой.
   Browser fixture 50 плиток — только UI, не доказательство сетевой ёмкости.
6. Kiosk security: отдельный viewer и политика session expiry; физический монитор,
   Firefox/Safari, клавиатура/screen reader и длительный browser soak.

## Полезные следующие настройки (пока предложения, не реализованы)

- Kc/тип насаждений и сезонная фаза, глубина корней/тип почвы и эффективность
  дождевания: основа ET0→расчётной потребности; не давать авто-норму без калибровки.
- Локальный NTP и RTC policy, безопасный срок stale погоды с явной политикой.
- Фильтры по участку/группе, темы высокой контрастности, сохранение filter/
  fullscreen preferences на конкретном мониторе, минимальный размер плитки.
- Порог ожидаемого потока/падения давления и локальные лимиты времени по зоне
  после верификации firmware/config ACK.
- Управление сроками сертификатов, отзыв устройств и ротация MQTT credentials.

## Воспроизведение

`python -m pytest server/tests -q`
`python -m server.tools.weather_monitor_smoke --screenshots /tmp/poliv-previews`
`python -m server.tools.dashboard_smoke`
`python -m server.tools.browser_smoke`
`python -m server.tools.stage9_smoke`
`PATH="$PATH:/usr/sbin" python -m server.tools.mqtt_smoke`
`python -m server.tools.mqtt_tls_smoke`
`pio test -d firmware -e native` и `pio run -d firmware -e esp32dev`.

Инструкция проверки новых возможностей: WEATHER_MONITOR_ACCEPTANCE.md.
Инструкции реального развёртывания и USB/TLS сохранены без изменений.
