# Полив — локальная система управления ESP32

Сервер FastAPI/SQLite, Mosquitto MQTT, прошивка ESP32/74HC595, монитор контроллеров
и активных зон, Open-Meteo 24 часа / 7 дней.

**Начало и интерактивная проверка Windows:** [docs/START_HERE_WINDOWS.md](docs/START_HERE_WINDOWS.md).

- [Подготовка Windows, USB и критерии стенда](docs/WINDOWS_SETUP.md)
- [Погода, монитор и настройки](docs/WEATHER_MONITOR_ACCEPTANCE.md)
- [USB-настройка сети и TLS](docs/ИНСТРУКЦИЯ_USB_TLS.md)
- [Текущие ограничения](docs/STATUS.md)

Рабочая версия — main. Личные данные создаются в отдельном runtime, не в checkout.
OTA/rollback и аппаратная приёмка не завершены. Обновление прошивки по USB.
Не подключайте насос до проверки полярности и выходов без нагрузки.

## Разработка

```sh
python -m pytest server/tests -q
pio test -d firmware -e native
pio run -d firmware -e esp32dev
```

Исходное ТЗ — stage0, принятые решения — docs/adr, история — WorkBook/WORKBOOK.md.
Устаревшие инструкции и прошлые аудит-отчёты доступны через историю Git.
