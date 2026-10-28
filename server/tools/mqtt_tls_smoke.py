"""Real private Mosquitto: TLS certificate checks and per-controller ACL isolation.
Requires mosquitto, openssl and locked Python dependencies. No hardware needed.
"""
import shutil
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import paho.mqtt.client as paho
from server.app.infra.mqtt_tls import configure_tls
from server.tools.broker_config import acl_for


def command(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait(event, label):
    if not event.wait(5):
        raise AssertionError(label)


def main():
    with tempfile.TemporaryDirectory(prefix='poliv-tls-') as directory:
        root = Path(directory)
        command('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                '-keyout', str(root / 'broker.key'), '-out', str(root / 'broker.crt'),
                '-days', '1', '-subj', '/CN=localhost',
                '-addext', 'subjectAltName=DNS:localhost', '-addext', 'basicConstraints=critical,CA:TRUE')
        # Test broker runs as current user; a private temporary directory remains mode 0700.
        command('mosquitto_passwd', '-b', '-c', str(root / 'passwd'), 'poliv_server', 'test-server')
        for box in ['BOX1', 'BOX2']:
            command('mosquitto_passwd', '-b', str(root / 'passwd'), 'box_' + box, 'test-device')
        (root / 'acl').write_text(acl_for(['BOX1', 'BOX2']))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        (root / 'broker.conf').write_text(
            f'listener {port} 127.0.0.1\nallow_anonymous false\n'
            f'password_file {root}/passwd\nacl_file {root}/acl\n'
            f'certfile {root}/broker.crt\nkeyfile {root}/broker.key\n'
            'persistence false\n')
        with (root / 'broker.log').open('w') as log:
            broker = subprocess.Popen([shutil.which('mosquitto') or '/usr/sbin/mosquitto', '-c', str(root / 'broker.conf')], stdout=log, stderr=log)
            clients = []
            try:
                deadline = time.monotonic() + 5
                while True:
                    try:
                        with socket.create_connection(('localhost', port), timeout=.2):
                            break
                    except OSError:
                        if time.monotonic() > deadline or broker.poll() is not None:
                            raise RuntimeError((root / 'broker.log').read_text())
                        time.sleep(.05)
                def client(username, password):
                    c = paho.Client(paho.CallbackAPIVersion.VERSION2, protocol=paho.MQTTv5)
                    configure_tls(c, {'tls_enabled': True, 'tls_ca_file': str(root / 'broker.crt')}, root)
                    c.username_pw_set(username, password)
                    connected = threading.Event()
                    result = []
                    def connected_callback(_c, _u, _f, reason, _p):
                        result.append(reason)
                        connected.set()
                    c.on_connect = connected_callback
                    c.connect('localhost', port)
                    c.loop_start()
                    clients.append(c)
                    wait(connected, 'TLS MQTT connect timeout')
                    assert not result[0].is_failure, result[0]
                    return c
                rejected = threading.Event()
                reasons = []
                unauthorized = paho.Client(paho.CallbackAPIVersion.VERSION2)
                configure_tls(unauthorized, {'tls_enabled': True, 'tls_ca_file': str(root / 'broker.crt')}, root)
                unauthorized.username_pw_set('box_BOX1', 'wrong-password')
                def auth_result(_c, _u, _f, reason, _p):
                    reasons.append(reason)
                    rejected.set()
                unauthorized.on_connect = auth_result
                unauthorized.connect('localhost', port)
                unauthorized.loop_start()
                clients.append(unauthorized)
                wait(rejected, 'missing authentication rejection')
                assert reasons[0].is_failure, reasons[0]
                server = client('poliv_server', 'test-server')
                device = client('box_BOX1', 'test-device')
                seen = []
                received = threading.Event()
                subscribed = threading.Event()
                server.on_subscribe = lambda *_: subscribed.set()
                def on_message(_c, _u, message):
                    seen.append(message.topic)
                    received.set()
                server.on_message = on_message
                server.subscribe('poliv/+/event', qos=1)
                wait(subscribed, 'subscribe timeout')
                info = device.publish('poliv/BOX1/event', '{"test":true}', qos=1)
                info.wait_for_publish(timeout=5)
                wait(received, 'own event did not arrive')
                assert seen == ['poliv/BOX1/event']
                # MQTT5 PUBACK explicitly reports denied cross-device writes.
                denied = threading.Event()
                codes = []
                def on_publish(_c, _u, _mid, reason, _properties):
                    codes.append(reason)
                    denied.set()
                device.on_publish = on_publish
                device.publish('poliv/BOX2/event', '{}', qos=1)
                wait(denied, 'missing denied PUBACK')
                assert codes[-1].is_failure, codes[-1]
                assert seen == ['poliv/BOX1/event']
                # The certificate is valid only for localhost; an IP must fail hostname verification.
                bad = paho.Client(paho.CallbackAPIVersion.VERSION2)
                configure_tls(bad, {'tls_enabled': True, 'tls_ca_file': str(root / 'broker.crt')}, root)
                try:
                    bad.connect('127.0.0.1', port)
                except ssl.SSLCertVerificationError:
                    pass
                else:
                    raise AssertionError('Wrong hostname accepted')
                finally:
                    bad.disconnect()
                bad_ca = paho.Client(paho.CallbackAPIVersion.VERSION2)
                configure_tls(bad_ca, {'tls_enabled': True}, root)
                try:
                    bad_ca.connect('localhost', port)
                except ssl.SSLCertVerificationError:
                    pass
                else:
                    raise AssertionError('Untrusted CA accepted')
                finally:
                    bad_ca.disconnect()
                print('Real Mosquitto TLS: trusted CA, hostname rejection, unknown CA rejection, own publish and cross-device ACL denial OK')
            finally:
                for c in clients:
                    c.disconnect()
                    c.loop_stop()
                broker.terminate()
                broker.wait(timeout=5)


if __name__ == '__main__':
    main()
