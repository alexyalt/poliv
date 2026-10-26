"""Isolated real-Mosquitto smoke. Starts only a private loopback broker.

Requires `mosquitto` in PATH and requirements.lock. No hardware, production
configuration or production database is used. Run: python -m server.tools.mqtt_smoke
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import tomllib

import paho.mqtt.client as paho

from server.app.infra.config import CODE_ROOT, Config
from server.app.infra.connection_scope import ScopedConnection, database_scope
from server.app.infra.db import init_db
from server.app.infra.mqtt_client import MqttServerClient
from server.app.services.catalog_service import CatalogService
from server.app.services.event_service import EventService
from server.app.services.mqtt_command_service import MqttCommandService
from server.app.services.archive_service import ArchiveService


def wait_until(predicate, label, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        threading.Event().wait(0.02)
    raise RuntimeError('Timed out: ' + label)


def main():
    executable = shutil.which('mosquitto')
    if not executable:
        raise SystemExit('Install mosquitto before running this isolated smoke')
    with tempfile.TemporaryDirectory(prefix='poliv-real-mqtt-') as folder:
        runtime = Path(folder)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        broker_cfg = runtime / 'mosquitto.conf'
        broker_cfg.write_text(f'listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n')
        with (runtime / 'broker.log').open('w') as broker_log:
            broker = subprocess.Popen([executable, '-c', str(broker_cfg)],
                                      stdout=broker_log, stderr=subprocess.STDOUT)
            client = server = commands = owned = None
            try:
                def listening():
                    if broker.poll() is not None:
                        raise RuntimeError('Private Mosquitto failed to start')
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=0.1):
                            return True
                    except OSError:
                        return False
                wait_until(listening, 'private broker startup')
                raw = tomllib.loads((CODE_ROOT / 'config/config.default.toml').read_text())
                raw['mqtt'].update(host='127.0.0.1', port=port, username='', password='',
                                   command_timeout_sec=0.4, command_retries=1)
                cfg = Config(raw=raw, project_root=runtime)
                bootstrap = init_db(cfg)
                bootstrap.close()
                owned = ScopedConnection(cfg.db_path)
                with database_scope():
                    catalog = CatalogService(owned)
                    for box in ('SMOKE-A', 'SMOKE-B'):
                        catalog.create_controller({'name': box, 'box_id': box})
                server = MqttServerClient(cfg, db_path=str(cfg.db_path))
                server.attach_stage6_services(event_service=EventService(owned))
                commands = MqttCommandService(cfg, owned, mqtt=server, db_path=str(cfg.db_path))
                server.start()
                wait_until(lambda: server.connected, 'server MQTT connect')
                ready, received = threading.Event(), threading.Event()
                published, callback_errors = [], []
                mode = {'value': 'retry'}
                client = paho.Client(paho.CallbackAPIVersion.VERSION2,
                                     client_id='poliv-smoke-controller')
                def on_connect(c, userdata, flags, reason, props):
                    c.subscribe('poliv/+/command', qos=1)
                def on_subscribe(*args):
                    ready.set()
                def on_message(c, userdata, message):
                    try:
                        payload = json.loads(message.payload)
                        box = message.topic.split('/')[1]
                        published.append((box, payload))
                        received.set()
                        if mode['value'] == 'block-a' and box == 'SMOKE-A':
                            return
                        attempts = sum(p['command_id'] == payload['command_id'] for _, p in published)
                        if mode['value'] == 'retry' and attempts == 1:
                            return
                        ack = {'protocol_version': '1.0', 'box_id': box, 'ts': time.time(),
                               'command_id': payload['command_id'], 'command': payload['command'],
                               'status': 'completed', 'message': 'isolated smoke ACK'}
                        c.publish(f'poliv/{box}/command_ack', json.dumps(ack), qos=1)
                    except Exception as exc:
                        callback_errors.append(str(exc))
                client.on_connect, client.on_subscribe, client.on_message = on_connect, on_subscribe, on_message
                client.connect('127.0.0.1', port)
                client.loop_start()
                if not ready.wait(5):
                    raise RuntimeError('Controller subscription not ready')
                for box in ('SMOKE-A', 'SMOKE-B'):
                    hello = {'protocol_version': '1.0', 'box_id': box, 'ts': time.time(),
                             'online': True, 'firmware_version': 'smoke-1', 'mode': 'idle',
                             'phase': 'idle', 'active_zones': [], 'buffer_size': 0,
                             'flow_enabled': False, 'service_mode': False, 'schedule_version': 0}
                    client.publish(f'poliv/{box}/hello', json.dumps(hello), qos=1).wait_for_publish(3)
                def both_online():
                    with database_scope():
                        return owned.execute("SELECT count(*) FROM controllers WHERE connection_status='online'").fetchone()[0] == 2
                wait_until(both_online, 'real hello persisted')
                def send(box, command, params=None):
                    with database_scope():
                        return commands.send_command(box, command, params or {})
                result = send('SMOKE-A', 'ping')
                assert result['status'] == 'completed' and result['attempts'] == 2
                attempts = [p for box, p in published if p['command_id'] == result['command_id']]
                assert len(attempts) == 2
                mode['value'] = 'block-a'
                received.clear()
                with ThreadPoolExecutor(1) as pool:
                    pending = pool.submit(send, 'SMOKE-A', 'ping')
                    assert received.wait(2)
                    stopped = send('SMOKE-B', 'stop_all')
                    assert stopped['status'] == 'completed'
                    assert not pending.done(), 'stop waited for the other command timeout'
                    assert pending.result(timeout=3)['status'] == 'timeout'
                from server.app.services.block_service import BlockService
                from server.app.services.mqtt_command_service import CommandError
                with database_scope():
                    cid=owned.execute("SELECT id FROM controllers WHERE box_id='SMOKE-A'").fetchone()[0]
                    zone=CatalogService(owned).create_zone({'controller_id':cid,'zone_number':1,'name':'Smoke zone','base_duration_minutes':10})
                    block=BlockService(owned).quick(zone['id'],{'duration_hours':1,'kind':'all','reason':'real broker smoke'},'smoke')
                mode['value']='ack'
                before=len(published)
                try:
                    send('SMOKE-A','zone_open',{'zone':1,'duration_sec':60})
                    raise AssertionError('Manual block did not reject command')
                except CommandError:
                    pass
                assert len(published)==before
                with database_scope():BlockService(owned).release(block['id'],'smoke')
                assert send('SMOKE-A','zone_open',{'zone':1,'duration_sec':60})['status']=='completed'
                event = {'protocol_version': '1.0', 'box_id': 'SMOKE-A', 'event_uid': 'durable-smoke',
                         'status': 'finished', 'source': 'manual', 'ts': time.time(),
                         'start_ts': 1, 'end_ts': 61, 'water_sec': 60, 'volume_liters': 5,
                         'active_zones': [], 'aborted': False, 'buffered': True}
                def count_events():
                    with database_scope():
                        return owned.execute('SELECT count(*) FROM watering_events').fetchone()[0]
                client.publish('poliv/SMOKE-A/event', json.dumps(event), qos=1).wait_for_publish(3)
                wait_until(lambda: count_events() == 1, 'event persisted')
                with database_scope():
                    assert ArchiveService(owned, cfg.data_dir).rotate('events')['row_count'] == 1
                client.publish('poliv/SMOKE-A/event', json.dumps(event), qos=1).wait_for_publish(3)
                # A later QoS1 ping/ACK proves the subscriber processed prior messages.
                mode['value'] = 'ack'
                assert send('SMOKE-A', 'ping')['status'] == 'completed'
                assert count_events() == 0, 'archived event was replayed'
                assert not callback_errors, callback_errors
                print('Real Mosquitto: hello, QoS1 retry/same ID, ACK, independent stop, stage9 block/release, archive replay OK')
            finally:
                if commands is not None:
                    commands.close()
                if server is not None:
                    server.stop()
                if client is not None:
                    client.disconnect()
                    client.loop_stop()
                if owned is not None:
                    owned.close()
                broker.terminate()
                try:
                    broker.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    broker.kill()
                    broker.wait(timeout=5)


if __name__ == '__main__':
    main()
