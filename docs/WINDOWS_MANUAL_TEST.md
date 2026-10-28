# Windows 10/11: запуск и предфинальное ручное тестирование

Рабочая ветка для ручного тестирования: **main**. Она содержит предыдущую прошивку,
монитор и погоду. Руководство — для PowerShell, Python 3.13 x64 и Mosquitto 2.x
на самой Windows, без WSL. Поддерживаемый сценарий этой приёмки: локальный браузер,
один ESP32 по Wi-Fi, прошивка по USB, одна зона на контроллере одновременно.
OTA пока не реализовано: обновление только USB. Полевой/безнадзорный запуск не принят.

## 0. Записать перед началом

- Windows/version, Python/version, Git commit (`git rev-parse HEAD`).
- LAN IPv4 компьютера (`ipconfig`), SSID 2.4 GHz, COM-порт из диспетчера устройств,
  USB-UART (CP210x/CH340), MAC/box_id платы. Для референса ожидается **5B2338**,
  но проверить фактический идентификатор в Serial, не назначать его вслепую.
- Тип реле/активный уровень пока неизвестен? Тогда не подключать силовую нагрузку.
- Есть датчик расхода? Без датчика оставить FLOW_ENABLED=false.
- Сохранить прежнюю прошивку esptool и рабочие данные перед перезаписью, если нужны.

## 1. Установить зависимости

1. Установить Git for Windows, Python **3.13 x64** с Python Launcher (`py`) и
   Eclipse Mosquitto **2.x x64** из https://mosquitto.org/download/.
   Microsoft Store alias `python` не используется: ниже явный py/venv.
2. Mosquitto будет запущен в окне PowerShell. Если установщик создал службу,
   остановить её перед тестом (PowerShell администратор: `Stop-Service mosquitto`).
   Не оставлять второй брокер на том же порту. При неизвестной службе проверить
   `Get-Service *mosquitto*` и `Get-NetTCPConnection -LocalPort 1883`.
3. Плата: установить драйвер её USB-UART, USB-кабель должен передавать данные.

В обычном PowerShell:

```powershell
New-Item -ItemType Directory -Force C:\PolivLab | Out-Null
Set-Location C:\PolivLab
git clone https://github.com/alexyalt/poliv.git
Set-Location .\poliv
git switch main
py -3.13 --version
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -q -r requirements.lock
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pip install -q platformio==6.2.0 pyserial==3.5
```

Не активировать venv обязательно: все команды используют явный Python. `uvloop`
исключён на Windows. Если установка не прошла, сохранить полный вывод, не продолжать
с частично установленным окружением. Для скачивания пакетов/сборки нужен интернет.

## 2. Создать отдельный runtime и broker

Уточнить IPv4 через `ipconfig`. Ниже **192.168.1.20 заменить своим адресом**.
Первый runtime не должен уже существовать:

```powershell
.\.venv\Scripts\python.exe -m server.tools.windows_setup --runtime C:\PolivLab\runtime-001 --host 192.168.1.20 --box 5B2338
```

CLI спросит пароль администратора (12+ символов) и пароль серверного MQTT-пользователя.
Затем mosquitto_passwd ещё раз запросит пароль `poliv_server`: ввести ТОТ ЖЕ серверный
MQTT-пароль. После этого задать отдельный пароль `box_5B2338` — он нужен для USB-настройки.
Пароли не передаются через аргументы процесса. При другом box_id добавить его `--box`.
Путь Mosquitto по умолчанию `C:\Program Files\mosquitto`; другой: `--mosquitto-dir`.

Создаётся TOML с секретами, password file/ACL и broker config. При прерывании
создания не считать runtime готовым: исправить причину и создать новый runtime,
старый оставить для диагностики. Повторный запуск не перезаписывает каталог.
Ограничить доступ через «Свойства папки → Безопасность»: ваша учётная запись и
администраторы; не размещать runtime в общей сетевой папке/OneDrive/Git.

Брокер слушает только loopback и указанный LAN-IP, anonymous запрещён. Сервер слушает
только localhost: UI доступен на тестовом ПК; ESP32 подключается к MQTT по LAN-IP.
Входящий TCP 1883 разрешить в Windows Firewall **только для Private profile и LocalSubnet**.
PowerShell администратор (если правило ещё не создано):

```powershell
New-NetFirewallRule -DisplayName 'Poliv MQTT test LAN' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 1883 -Profile Private -RemoteAddress LocalSubnet
```

Убедиться, что Wi-Fi сеть Windows имеет профиль Private. Не открывать порт в роутере.
Plaintext — только доверенная тестовая LAN; TLS отдельным сценарием по ИНСТРУКЦИЯ_USB_TLS.md.
Если IP изменился после DHCP, обновить broker config, USB host и перезапустить тест.
Удобно закрепить IP компьютера в DHCP роутера. Отключить сон ПК на время прогона.

## 3. Запустить брокер и сервер

Открыть **два обычных окна PowerShell**, в обоих `Set-Location C:\PolivLab\poliv`.
Не создавать службу и задачу планировщика до ручной приёмки.

Окно A, брокер (оставить открытым):

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\windows\Start-Poliv.ps1 -Runtime C:\PolivLab\runtime-001 -Component broker
```

Окно B, сервер (оставить открытым):

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\windows\Start-Poliv.ps1 -Runtime C:\PolivLab\runtime-001 -Component server
```

Bypass действует только для запуска этого процесса; системную policy не менять.
Если корпоративная политика запрещает scripts, запускать вручную:

```powershell
$env:PYTHONUTF8='1'
$env:POLIV_DATA_ROOT='C:\PolivLab\runtime-001'
$env:POLIV_CONFIG_LOCAL='C:\PolivLab\runtime-001\config.local.toml'
& 'C:\Program Files\mosquitto\mosquitto.exe' -c 'C:\PolivLab\runtime-001\broker\mosquitto.conf' -v
# Сервер в другом окне, с теми же env:
.\.venv\Scripts\python.exe -m uvicorn server.main:app --host 127.0.0.1 --port 8080 --workers 1
```

Открыть **http://127.0.0.1:8080**, войти admin с начальным паролем и сменить его.
В «Контроллеры» создать фактический box_id и назвать участок. Создать зоны 1…16
или только физически подключённые. Выключить parallel_enabled; физическая прошивка
отклоняет многозонный шаг. До платы online не ожидается.

## 4. Прошить service-сборку и настроить USB

Из checkout, без нагрузки/насоса/давления:

```powershell
.\.venv\Scripts\python.exe -m platformio run -d firmware -e esp32dev
.\.venv\Scripts\python.exe -m platformio run -d firmware -e esp32dev -t upload --upload-port COM3
.\.venv\Scripts\python.exe -m platformio device monitor -p COM3 -b 115200
```

COM3 заменить реальным. При невозможности подключения попробовать BOOT по инструкции
платы и проверить кабель/драйвер. Serial Monitor закрывается Ctrl+C; освободить порт
перед upload или provision. По умолчанию OUTPUTS_VERIFIED=false: открытия запрещены.

Первичная пустая LittleFS даёт storage_error. **Только для первичной инициализации
или сознательного стирания локального расписания/истории** отправить `FORMAT` и newline
в service Serial Monitor. Это стирает LittleFS, снимает сохранённый fault и перезапускает
плату; сетевой профиль NVS сохраняется. Не применять для обычной перепрошивки.

Закрыть монитор и выполнить:

```powershell
.\.venv\Scripts\python.exe -m server.tools.provision_controller --device COM3 --host 192.168.1.20 --username box_5B2338
```

Ввести SSID 2.4GHz, Wi-Fi пароль и отдельный пароль платы из шага 2. Ожидается
`Configuration saved; controller is restarting.` Затем Serial показывает box_id,
плитка становится service/online. NTP требует доступа из Wi-Fi; time_valid должен
стать true до полива. Потеря интернет-доступа к NTP после холодного старта запрещает
запуск. До синхронизации время с сервера также может помочь в plaintext LAN.

Если offline: сверить реальный box_id, MQTT user `box_ID`, пароль, IP, ACL, Firewall,
одну Wi-Fi LAN без client isolation/гостевой сети. Если CONFIG не подтверждён,
проверить COM и отсутствие Monitor. Пароли в журналы и отчёты не копировать.

## 5. Проверка ПО до подключения воды

В третьем PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\windows\Start-Poliv.ps1 -Runtime C:\PolivLab\runtime-001 -Component doctor
```

Doctor читает конфиг и БД, проверяет integrity/migrations и MQTT auth; не делает
миграций, не отправляет команды и не открывает клапаны. PASS всех пунктов —
не подтверждение исправности реле. JSON-отчёт (новое имя):

```powershell
.\.venv\Scripts\python.exe -m server.tools.preflight --runtime C:\PolivLab\runtime-001 --output C:\PolivLab\preflight-001.json
```

1. UI: имя, service, online; «Старт» заблокирован. Подробнее — click плитки.
2. `/settings`: координаты 44.423062/34.06452; `/weather`: обновить, 24ч/7д, timezone,
   ET0/ветер/порывы; отсутствие данных обозначено, не ноль.
3. Создать viewer/operator; viewer не управляет и не сохраняет настройки.
4. Отключить брокер: offline по LWT/серверному threshold (в тестовом setup 1 мин).
   Нельзя считать last UI refresh свежестью устройства. Вернуть брокер, online восстановится.
5. Перезапустить сервер: имена/зоны/параметры остаются. Не запускать два uvicorn.

## 6. Разрешить выходы только после проверки полярности

Питание насосов/клапанов отключено. Data GPIO13, Clock GPIO12, Latch GPIO14.
Проверить по схеме платы полярность и порядок; default RELAY_ACTIVE_HIGH=true —
**предположение сборки, не измерение реальной платы**. Особенно проверить состояния
до setup, reset/USB reconnect и OE 74HC595. Сама прошивка не удерживает GPIO при
обесточивании/reset. Если реле самопроизвольно включается — STOP, стенд не принят.

```powershell
Copy-Item .\firmware\include\secrets.example.h .\firmware\include\secrets.h
notepad .\firmware\include\secrets.h
```

Задать OUTPUTS_VERIFIED=true и измеренную RELAY_ACTIVE_HIGH; FLOW_ENABLED=false
при отсутствии датчика. Сеть будет взята из сохранённого USB NVS-профиля.
Заново собрать и прошить командами шага 4, обычный upload сохраняет NVS/LittleFS.
Открытия теперь возможны. Полив без time_valid отвергается.

Коротко, без нагрузки, проверить каждую зону 1…16 и что открыта ровно одна.
Затем подключить **один клапан**, проверить без насоса, затем кратко с водой под
наблюдением. Не начинать первым тестом весь участок.

## 7. Матрица ручной приёмки с контроллером

| № | Действие | Ожидаемый результат / критерий |
|---|---|---|
| W01 | Старт зоны 1 на 10 сек | Один клапан, голубая плитка, зона на active-zones; остановка по времени |
| W02 | Пауза на работающем прогоне | Все выходы закрыты; жёлтая плитка; resume не создаёт вторую зону |
| W03 | Стоп во время manual/schedule | Выход закрыт; событие stopped/aborted; текущий run не запускается повторно |
| W04 | Зона 2 во время зоны 1 | Первая закрыта, вторая одна; данные/история согласованы |
| W05 | Расписание water/soak/water | applied ACK; на soak всё закрыто; будущий запуск не начинается раньше времени |
| W06 | Выключить сервер при сохранённом плане | После time sync устройство исполняет план локально; события после связи |
| W07 | Обесточить плату во время шага | Default skip активного run, не самопроизвольный повтор; повреждённый cursor блокирует работу |
| W08 | Отключить Wi-Fi/брокер | UI offline в пределах keepalive/threshold, после связи актуальный статус |
| W09 | С датчиком: нет импульсов при поливе | no_flow закрывает всё; красная плитка с причиной |
| W10 | Импульсы при закрытых клапанах | unexpected_flow; закрытие и блокировка; после устранения причины сброс с ACK |
| W11 | Сброс аварии при offline/отказе | HTTP409/503, серверная блокировка и причина остаются |
| W12 | Устранить причину, повторить сброс | completed ACK → снятие server lock; storage/config/cursor ошибки дистанционно не сбрасываются |
| W13 | Перезапустить сервер/плату после reset | Сохранность NVS/настроек, fault не возвращается без причины |
| W14 | Монитор 1366/1920px, filter/fullscreen | Ошибки первыми, текст читаем, нет горизонтального скролла |
| W15 | Смена координат | Старый snapshot другой точки не показан; новый прогноз после обновления |
| W16 | Backup/restore | БД/файлы проверены, восстановленный runtime независим; только один сервер подключён |

Без расходомера W09/W10 отметить «не проверено — нет датчика», не «passed».
Проверка no_flow не доказывает восстановление подачи: перед reset оператор физически
устраняет причину; нужен минимум 3 сек спокойного входа flow. Неисправности flash,
курсора и конфигурации исправляются на стенде, а не кнопкой «сбросить».
UI сброса — кнопка «Сброс аварии» в деталях контроллера с подтверждением причины; API — POST /api/controllers/{id}/clear-error с ролью operator/admin.
После успеха дождаться свежего device status; не продолжать при error_code.
Новая версия во время активного прогона может быть rejected: дождаться idle и
повторить отправку. Для этой ручной приёмки не использовать immediate replacement.

## 8. Резервная копия и сбор отчёта

Остановить сервер Ctrl+C, оставить исходный runtime без изменений:

```powershell
.\.venv\Scripts\python.exe -m server.tools.runtime_snapshot backup --source C:\PolivLab\runtime-001 --destination C:\PolivLab\backup-001
.\.venv\Scripts\python.exe -m server.tools.runtime_snapshot verify --source C:\PolivLab\backup-001
.\.venv\Scripts\python.exe -m server.tools.runtime_snapshot restore --source C:\PolivLab\backup-001 --destination C:\PolivLab\restored-001
```

Каталоги назначения не должны существовать. Конфиг/ACL/пароли хранятся отдельно:
snapshot runtime не заменяет защищённую копию config.local.toml и broker password file.
Для запуска восстановленного runtime явно задать env на `restored-001`, а
POLIV_CONFIG_LOCAL — на исходный защищённый конфиг; Start-Poliv ожидает config в
своём runtime, при желании скопировать его туда вручную. Не менять существующую
копию/БД без backup. Восстановление тестировать без автоматических открытий/насоса.

Отчёт: `docs/MANUAL_ACCEPTANCE_REPORT.md`; записать PASS/FAIL/не проверено для
каждой строки, длительности stop/offline, журналы server/Serial/broker, commit,
снимки. Удалить пароли/secret_key/сетевые данные, которые нельзя публиковать.

## 9. Дополнительные автоматические проверки Windows

```powershell
.\.venv\Scripts\python.exe -m pytest server/tests -q
.\.venv\Scripts\python.exe -m pip install -q playwright==1.64.1
.\.venv\Scripts\python.exe -m playwright install chromium
.\.venv\Scripts\python.exe -m server.tools.weather_monitor_smoke
.\.venv\Scripts\python.exe -m server.tools.dashboard_smoke
```

Перед полным pytest оставить не менее 8 ГБ свободного места в `%TEMP%`. При прерванных
прогонах тестовые SQLite каталоги могут остаться: после закрытия всех тестов удалять
только созданные тестами временные `poliv-*`, не рабочие каталоги.

Native C++ tests требуют host C++ compiler; для первого Windows запуска достаточно
уже выполненных native-тестов + cross-build ESP32, не ставить компилятор ради USB.
Проверки Windows CI добавлены, но их результат нужно сверить отдельно от Linux-прогонов.

## Вопросы при проверке и критерий перехода дальше

- Видно ли с рабочего расстояния монитора, где авария и где offline? Нужны ли крупнее
  цифры/пять колонок/группировка по участку? Не полагаться только на цвет.
- Соответствует ли имя зоны физическому клапану и порядку 74HC595? Никаких двух
  одновременно. Какое физическое состояние при reset/питании/сне Windows?
- Какова измеренная задержка stop? Задать приемлемый предел до полевого запуска;
  любой самопроизвольный старт или потеря stop — блокирующий дефект.
- Нужен ли расходомер и подтверждена ли калибровка реальным объёмом? Совпадают ли
  показания/литры истории с фактом, а не только со значениями эмулятора?
- Подходят ли пороги ветра/заморозка/жары и фактическое место прогноза? ET0 и
  модельная почва не заменяют измерение участка.
- Нужен ли круглосуточный экран? Проверить сон, session expiry, reconnect и
  резервную копию. Не отключать защиту учётной записи ради удобства.

**Начать предфинальную ручную проверку можно с шагов 1–5.** Переход к клапану/воде
только после подтверждения полярности и W01–W04 без нагрузки. Для принятия локального
пилота все применимые W01–W16 должны пройти, отсутствие датчика/непроверенные
сценарии явно отмечены. Готовность к круглосуточному production требует отдельной
аппаратной приёмки, решения оставшихся ограничений и длительного прогона.
