"""Test generated broker config including paths with spaces on a real Mosquitto.
Uses a temporary test password file, never the user's runtime.
"""
from pathlib import Path
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import paho.mqtt.client as paho
from .windows_setup import create


def main():
    broker = shutil.which('mosquitto') or ('C:/Program Files/mosquitto/mosquitto.exe' if os.name == 'nt' else '/usr/sbin/mosquitto')
    password_tool = shutil.which('mosquitto_passwd') or str(Path(broker).with_name('mosquitto_passwd.exe' if os.name == 'nt' else 'mosquitto_passwd'))
    with tempfile.TemporaryDirectory(prefix='poliv broker ') as temporary:
        # Use a local bind address for both listeners to avoid depending on Wi-Fi in CI.
        root = create(Path(temporary)/'runtime with spaces', '192.0.2.10', ['5B2338'],
                      'test-admin-password', 'test-mqtt-password', Path(broker).parent)
        config = root/'broker/mosquitto.conf'
        with socket.socket() as free:
            free.bind(('127.0.0.1',0))
            port = free.getsockname()[1]
        text = config.read_text().replace('listener 1883 192.0.2.10\n', '').replace('listener 1883 127.0.0.1', f'listener {port} 127.0.0.1')
        config.write_text(text)
        subprocess.run([password_tool,'-b','-c',str(root/'broker/poliv.passwd'),'poliv_server','test-mqtt-password'],check=True)
        with (root/'broker/test.log').open('w') as log:
            process = subprocess.Popen([broker,'-c',str(config)],stdout=log,stderr=log)
            client = paho.Client(paho.CallbackAPIVersion.VERSION2)
            try:
                connected = threading.Event(); result = []
                def callback(_c,_u,_f,reason,_p):
                    result.append(not reason.is_failure);connected.set()
                client.on_connect = callback
                client.username_pw_set('poliv_server','test-mqtt-password')
                deadline = time.monotonic()+5
                while True:
                    try:
                        client.connect('127.0.0.1',port);break
                    except OSError:
                        if process.poll() is not None or time.monotonic()>deadline:
                            raise RuntimeError((root/'broker/test.log').read_text())
                        time.sleep(.05)
                client.loop_start()
                assert connected.wait(5) and result==[True]
                print('Generated Mosquitto configuration: paths with spaces and authentication OK')
            finally:
                client.disconnect();client.loop_stop()
                process.terminate();process.wait(timeout=5)


if __name__=='__main__':main()
