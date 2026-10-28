import time
import pytest
from server.tests.test_stage6 import _Harness6
from server.app.services.mqtt_command_service import ControllerOffline, MqttUnavailable


@pytest.fixture
def h():
    instance=_Harness6()
    yield instance
    instance.close()


@pytest.mark.parametrize('outcome',['rejected','error','timeout','ignored_duplicate','offline','unavailable'])
def test_reset_does_not_clear_server_lock_before_completed_ack(h,monkeypatch,outcome):
    c=h.controller('RESET')
    with h.conn:
        h.conn.execute("UPDATE controllers SET emergency_lock_until_ts=?,device_error_code='no_flow',current_mode='error' WHERE id=?",(int(time.time())+3600,c['id']))
    def send(*args,**kwargs):
        if outcome=='offline':raise ControllerOffline('RESET')
        if outcome=='unavailable':raise MqttUnavailable('test unavailable')
        return {'status':outcome}
    monkeypatch.setattr(h.cmd,'send_command_for_controller',send)
    client=h.login_as('operator')
    r=client.post(f"/api/controllers/{c['id']}/clear-error",json={'reason':'verified'})
    assert r.status_code in (409,503)
    row=h.conn.execute('SELECT * FROM controllers WHERE id=?',(c['id'],)).fetchone()
    assert row['emergency_lock_until_ts']>time.time()
    assert row['device_error_code']=='no_flow' and row['current_mode']=='error'


def test_windows_runtime_generation_and_readonly_doctor(tmp_path):
    import tomllib
    from server.tools.windows_setup import create
    from server.tools.preflight import inspect_runtime
    root=tmp_path/'runtime with spaces'
    create(root,'192.168.1.20',['5B2338'],'long-admin-password','long-mqtt-password',tmp_path)
    cfg,rows=inspect_runtime(root)
    assert cfg['mqtt']['password']=='long-mqtt-password'
    assert any(not r['ok'] and r['check']=='Database initialized' for r in rows)
    text=(root/'broker/mosquitto.conf').read_text()
    assert 'allow_anonymous false' in text and 'listener 1883 192.168.1.20' in text
    assert 'long-mqtt-password' not in text
    with pytest.raises(ValueError):create(root,'192.168.1.20',['5B2338'],'x'*12,'y'*12,tmp_path)
    assert tomllib.loads((root/'config.local.toml').read_text())['test_data']['enabled'] is False


def test_weather_thread_stop_can_join():
    from server.app.services.weather_service import WeatherUpdater
    thread=WeatherUpdater('unused',initial_delay_sec=30)
    thread.start()
    thread.stop()
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_windows_lockfile_markers():
    from pathlib import Path
    from packaging.requirements import Requirement
    lines=Path('requirements.lock').read_text().splitlines()
    requirements=[Requirement(l.strip()) for l in lines if l.strip() and not l.strip().startswith('#')]
    windows=[r.name for r in requirements if not r.marker or r.marker.evaluate({'sys_platform':'win32'})]
    assert 'uvloop' not in windows and 'colorama' in windows
