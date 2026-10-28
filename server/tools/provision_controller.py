"""Configure an ESP32 service build over USB, without saving credentials on the PC."""
import argparse
import getpass
import json
from pathlib import Path
import time


def payload(ssid, wifi_password, host, username, password, port, ca=None):
    if not ssid or len(ssid.encode()) > 32 or len(wifi_password.encode()) > 63:
        raise ValueError('Invalid Wi-Fi SSID/password length')
    if not host or len(host) > 253 or any(not (c.isascii() and (c.isalnum() or c in '.-:')) for c in host):
        raise ValueError('Host must be a hostname or IP address, without URL scheme/path')
    if not username or len(username.encode()) > 96 or not password or len(password.encode()) > 256:
        raise ValueError('MQTT username/password required and must fit device limits')
    if not 1 <= port <= 65535:
        raise ValueError('Port must be 1..65535')
    if any('\0' in x for x in (ssid, wifi_password, username, password)):
        raise ValueError('Embedded NUL is not allowed')
    if ca is not None and ('-----BEGIN CERTIFICATE-----' not in ca or
                           '-----END CERTIFICATE-----' not in ca or len(ca.encode()) > 6000):
        raise ValueError('CA must be a PEM certificate up to 6000 bytes')
    data = dict(ssid=ssid, wifi_password=wifi_password, host=host, username=username,
                password=password, port=port, tls=ca is not None)
    if ca is not None:
        data['ca_pem'] = ca
    encoded = ('CONFIG ' + json.dumps(data, ensure_ascii=False, separators=(',', ':')) + '\n').encode()
    if len(encoded) > 8192:
        raise ValueError('Encoded configuration exceeds device serial limit')
    return encoded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--device', required=True, help='COM3 or /dev/ttyUSB0')
    parser.add_argument('--host', required=True)
    parser.add_argument('--username', required=True)
    parser.add_argument('--port', type=int)
    parser.add_argument('--ca-file', type=Path, help='Enable TLS with this CA PEM')
    args = parser.parse_args()
    ca = args.ca_file.read_text() if args.ca_file else None
    message = payload(input('Wi-Fi SSID: '), getpass.getpass('Wi-Fi password: '),
                      args.host, args.username, getpass.getpass('MQTT password: '),
                      args.port or (8883 if ca is not None else 1883), ca)
    import serial
    with serial.Serial(args.device, 115200, timeout=0.25, write_timeout=10) as device:
        # Opening USB may reset an ESP32. Allow boot, then discard pre-existing logs.
        time.sleep(2)
        device.reset_input_buffer()
        # Pace chunks so the ESP32 RX buffer can be drained while loop() runs.
        for i in range(0, len(message), 64):
            device.write(message[i:i + 64])
            device.flush()
            time.sleep(0.05)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            line = device.readline().decode('utf-8', errors='replace')
            # Never echo serial data that might contain credentials from other firmware.
            if 'Конфигурация сохранена' in line:
                print('Configuration saved; controller is restarting.')
                return
            if 'Некорректная конфигурация' in line or 'Ошибка сохранения' in line:
                raise SystemExit('Controller rejected configuration; previous settings retained.')
    raise SystemExit('No confirmation. Check service build, USB port and serial monitor; credentials were not printed.')


if __name__ == '__main__':
    main()
