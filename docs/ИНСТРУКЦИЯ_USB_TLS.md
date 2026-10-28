# USB-настройка ESP32 и MQTT TLS

Продолжение `ИНСТРУКЦИЯ_РЕАЛЬНАЯ_МАШИНА.md`. Реальная ESP32 ещё не проверена.
Программно проверены сборка, decoder и настоящий Mosquitto с TLS/ACL.

## 1. Настроить Wi-Fi/MQTT без записи паролей в исходники

1. Один раз прошейте service-сборку с `OUTPUTS_VERIFIED=false`. `secrets.h` для сети теперь необязателен: сеть можно настроить по USB. Сохраните текущую прошивку, если нужен возврат.
2. Закройте Serial Monitor: один USB-порт не может одновременно использоваться двумя программами.
3. В активированном venv установите CLI-зависимость: `pip install -q pyserial==3.5`.
4. Выполните из корня проекта:

```sh
python -m server.tools.provision_controller --device /dev/ttyUSB0 --host 192.168.1.2 --username box_5B2338
```

В Windows замените `/dev/ttyUSB0` на `COM3`. CLI отдельно спросит SSID, Wi-Fi пароль и MQTT пароль. Пароли вводятся без отображения, не попадают в аргументы процесса или создаваемый CLI файл. CLI не печатает произвольные сообщения платы: они могут принадлежать другой прошивке.

5. Дождитесь `Configuration saved; controller is restarting.` Это подтверждает запись, но ещё не успешное подключение к сети. Проверьте online в UI.
6. Настройки хранятся в NVS одним значением и используются после перезапуска. Они сохраняются при обычной USB-прошивке и LittleFS FORMAT. Полное стирание flash удалит их.
7. Если подключение не получилось, повторите CONFIG с правильными данными. Через Serial можно отправить `NETWORK_RESET` для удаления только сетевого профиля. Обе команды разрешены только service-сборке; секреты не выдаются обратно.
8. Перед включением реальных выходов задайте проверенную полярность и `OUTPUTS_VERIFIED=true`, затем пересоберите и прошейте. NVS-профиль сохранится. Чтобы позже изменить его, снова прошейте service-сборку, выполните настройку, верните проверенную рабочую сборку.

CLI отправляет данные по физическому USB, не шифрует NVS и не является удалённым provisioning-порталом. Защита от физического чтения flash/secure boot ещё не настроена. Отмена или отсутствие подтверждения не означает откат к предыдущему сетевому профилю: проверьте плату и при необходимости повторите настройку. Сообщение о неверной конфигурации или ошибке записи означает, что новый профиль не принят.

## 2. Создать частный CA и сертификат брокера

Для тестовой LAN ниже используется DNS-имя `poliv-broker.lan`. Оно должно разрешаться в LAN-IP компьютера и на ESP32 (настройте DNS роутера). Если используете IP, включите именно этот адрес в SAN сертификата и передавайте его как host. Нельзя отключать проверку имени ради подключения.

На Linux с OpenSSL выполните в отдельном защищённом каталоге, которого ещё нет:

```sh
mkdir -m 700 "$HOME/poliv-pki"
cd "$HOME/poliv-pki"
umask 077
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out ca.key
openssl req -x509 -new -key ca.key -sha256 -days 3650 -out ca.crt -subj '/CN=Poliv LAN CA' -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign'
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out broker.key
openssl req -new -key broker.key -out broker.csr -subj '/CN=poliv-broker.lan'
printf '%s\n' 'subjectAltName=DNS:poliv-broker.lan,IP:192.168.1.2' 'basicConstraints=critical,CA:FALSE' 'keyUsage=critical,digitalSignature,keyEncipherment' 'extendedKeyUsage=serverAuth' > broker.ext
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out broker.crt -days 365 -sha256 -extfile broker.ext
openssl verify -CAfile ca.crt broker.crt
```

Замените `192.168.1.2` на реальный адрес перед созданием сертификата. Ожидается `broker.crt: OK`. Закрытый ключ CA храните отдельно от брокера; на контроллер копируется только публичный ca.crt. Запланируйте замену сертификата до окончания срока.

## 3. Включить TLS listener Mosquitto

В корне checkout и активированном venv:

```sh
python -m server.tools.broker_config --destination "$HOME/poliv-broker-tls" --box 5B2338 --tls
sudo install -d -m 750 -o root -g mosquitto /etc/mosquitto/certs
sudo install -m 644 "$HOME/poliv-pki/broker.crt" /etc/mosquitto/certs/broker.crt
sudo install -m 640 -o root -g mosquitto "$HOME/poliv-pki/broker.key" /etc/mosquitto/certs/broker.key
sudo install -m 640 -o root -g mosquitto "$HOME/poliv-broker-tls/poliv.acl" /etc/mosquitto/poliv.acl
sudo install -m 644 "$HOME/poliv-broker-tls/poliv.conf" /etc/mosquitto/conf.d/poliv.conf
sudo systemctl restart mosquitto
sudo systemctl status mosquitto
```

Перед заменой сохраните прежние conf/ACL; генератор создаёт новую конфигурацию, а install заменяет соответствующие файлы. Если нужны эмуляторы/другие платы, включите их `--box` при генерации, иначе их права будут отсутствовать. Файл `/etc/mosquitto/poliv.passwd` с poliv_server и box_5B2338 должен уже быть создан по основной инструкции. Удалите только прежний дублирующий listener приложения; не отключайте настройки других приложений. Сгенерированный профиль содержит только 8883, plaintext listener 1883 в нём отсутствует. Проверьте остальные включённые конфиги Mosquitto на лишний listener. Откройте 8883 только для своей LAN.

## 4. Перевести сервер и ESP32 на TLS

В существующий раздел `[mqtt]` локального TOML (не создавайте второй раздел с тем же именем) добавьте:

```toml
host = "poliv-broker.lan"
port = 8883
tls_enabled = true
tls_ca_file = "/etc/poliv/ca.crt"
```

Скопируйте ca.crt в указанный путь и дайте пользователю сервера право чтения. Сохраните username/password из прежнего конфига. Перезапустите сервер. Пустой tls_ca_file использует системные доверенные CA; для частного CA нужен файл. Неверный/нечитаемый CA прерывает запуск MQTT, без fallback на 1883. Сервер использует TLS >=1.2 и обязательную проверку имени.

Настройте service-сборку контроллера:

```sh
python -m server.tools.provision_controller --device /dev/ttyUSB0 --host poliv-broker.lan --username box_5B2338 --ca-file "$HOME/poliv-pki/ca.crt"
```

Порт по умолчанию при --ca-file — 8883. CA-профиль до 6000 байт, CONFIG до 8192 байт. ESP32 использует проверку CA/hostname через WiFiClientSecure; TLS-подключение разрешается только после синхронизации времени NTP. До MQTT время от сервера ещё недоступно: при блокированном NTP TLS не подключится. Настройка локального NTP/RTC — отдельная задача. Самопроизвольного перехода на plaintext нет. Тестовый эмулятор пока не поддерживает TLS; для него оставьте отдельный изолированный тестовый брокер, не ослабляйте рабочий TLS listener.

## 5. Проверить отказные сценарии

1. Правильный CA/имя/пароль: online и ping ACK.
2. Ошибка DNS или имени сертификата: offline, клапаны не должны самопроизвольно открываться.
3. Другой CA: TLS handshake отклонён; не включайте insecure mode.
4. Неверный MQTT пароль: отказ подключения, хотя TLS может быть установлен.
5. Пользователь box_5B2338 публикует только в свои разрешённые топики. Публикация в чужой box_id запрещена ACL.
6. Перезапуск платы: профиль NVS остаётся, повторный CONFIG не требуется.
7. USB CONFIG в рабочей сборке с OUTPUTS_VERIFIED=true не применяется. Для перенастройки используйте service-сборку без нагрузки.

Проверка на компьютере без платы:

```sh
python -m server.tools.mqtt_tls_smoke
pio test -d firmware -e native
pio run -d firmware -e esp32dev
```

Smoke поднимает временный настоящий брокер с тестовым сертификатом, проверяет hostname, CA, неправильный пароль и отказ cross-device publish, завершает процесс и удаляет временные файлы. Он не использует рабочие ключи/пароли. OTA-загрузка/rollback, физическая приёмка и пилот 50 плат остаются открытыми.
