"""Regression tests for the stage-8 audit; no real MQTT or weather requests."""
import hashlib
import json
import threading
import time
from pathlib import Path

import pytest

from server.tests.test_stage8_archive import h6, mk_controller, mk_zone, add_event, svc
from server.tests.test_stage7 import _mk_program, _link, _add_obs
from server.app.api.routes_weather import register_api_routes_weather
from server.app.api.routes_reports import register_api_routes_reports
from server.app.services import weather_service as wx
from server.app.services import report_service as reports
from server.app.services import archive_integrity
from server.app.infra.db import connect_path


@pytest.mark.parametrize('global_enabled', [False, True])
@pytest.mark.parametrize('rule_enabled', [False, True])
def test_weather_preview_never_changes_settings(h6, global_enabled, rule_enabled):
    c = mk_controller(h6)
    z = mk_zone(h6, c['id'], 1)
    program = _mk_program(h6.conn)
    _link(h6.conn, program, z['id'])
    _add_obs(h6.conn, precip_mm=20)
    wx.set_weather_setting(h6.conn, 'weather.enabled', True)
    wx.set_weather_setting(h6.conn, 'weather.rules_enabled', global_enabled)
    rule = wx.create_rule(h6.conn, {'name': 'preview', 'enabled': rule_enabled,
                                  'condition': 'rain', 'threshold': 1, 'action': 'skip'})
    register_api_routes_weather(h6.app, h6.cfg, h6.conn)
    client = h6.login_as('operator')
    before = [tuple(r) for r in h6.conn.execute('SELECT * FROM settings ORDER BY key')]
    result = client.post(f'/api/v1/weather/rules/{rule["id"]}/test', json={'controller_id': c['id']})
    assert result.status_code == 200
    assert result.json()['would_apply']
    assert result.json()['runs_before'] > result.json()['runs_after']
    assert [tuple(r) for r in h6.conn.execute('SELECT * FROM settings ORDER BY key')] == before


def test_preview_does_not_attribute_other_rules(h6):
    c = mk_controller(h6)
    z = mk_zone(h6, c['id'], 1)
    _link(h6.conn, _mk_program(h6.conn), z['id'])
    _add_obs(h6.conn, precip_mm=20)
    wx.set_weather_setting(h6.conn, 'weather.enabled', True)
    wx.create_rule(h6.conn, {'name': 'fires', 'condition': 'rain', 'threshold': 1, 'action': 'skip'})
    chosen = wx.create_rule(h6.conn, {'name': 'never fires', 'condition': 'rain', 'threshold': 100, 'action': 'skip'})
    register_api_routes_weather(h6.app, h6.cfg, h6.conn)
    response = h6.login_as('operator').post(f'/api/v1/weather/rules/{chosen["id"]}/test', json={'controller_id': c['id']})
    assert response.status_code == 200
    assert response.json()['would_apply'] is False


def test_preview_exception_keeps_global_state(h6, monkeypatch):
    from server.app.core.schedule_compiler import compiler
    c = mk_controller(h6)
    rule = wx.create_rule(h6.conn, {'name': 'error', 'enabled': False, 'condition': 'rain', 'threshold': 1, 'action': 'skip'})
    wx.set_weather_setting(h6.conn, 'weather.rules_enabled', True)
    register_api_routes_weather(h6.app, h6.cfg, h6.conn)
    def fail(*args, **kwargs):
        raise RuntimeError('injected compile failure')
    monkeypatch.setattr(compiler, 'compile_schedule', fail)
    with pytest.raises(RuntimeError):
        h6.login_as('operator').post(f'/api/v1/weather/rules/{rule["id"]}/test', json={'controller_id': c['id']})
    assert wx.get_weather_setting(h6.conn, 'weather.rules_enabled', None) is True


@pytest.mark.parametrize('fmt', ['csv', 'json'])
def test_archive_typed_relational_roundtrip_and_immutable_batches(h6, tmp_path, fmt):
    c = mk_controller(h6)
    z = mk_zone(h6, c['id'], 1)
    wx.set_weather_setting(h6.conn, 'archive.events_format', fmt)
    eid = add_event(h6, cid=c['id'], uid='one', zone_id=z['id'])
    original = dict(h6.conn.execute('SELECT * FROM watering_events WHERE id=?', (eid,)).fetchone())
    children = [dict(r) for r in h6.conn.execute('SELECT * FROM watering_event_zones')]
    service = svc(h6, tmp_path)
    first = service.rotate('events')
    saved = Path(first['file_path']).read_bytes()
    add_event(h6, cid=c['id'], uid='two')
    second = service.rotate('events')
    assert first['file_path'] != second['file_path']
    assert Path(first['file_path']).read_bytes() == saved
    if fmt == 'json':
        assert json.loads(saved)[0]['event_uid'] == 'one'
        assert json.loads(Path(second['file_path']).read_bytes())[0]['event_uid'] == 'two'
    export = next(e for e in service.list_exports() if e['file_path'] == first['file_path'])
    assert service.restore_export(export['id'])['restored'] == 1
    assert dict(h6.conn.execute('SELECT * FROM watering_events WHERE id=?', (eid,)).fetchone()) == original
    assert [dict(r) for r in h6.conn.execute('SELECT * FROM watering_event_zones')] == children
    assert service.restore_export(export['id'])['restored'] == 0


@pytest.mark.parametrize('damage_manifest', [False, True])
def test_archive_corruption_rejected_before_restore(h6, tmp_path, damage_manifest):
    c = mk_controller(h6)
    add_event(h6, cid=c['id'], uid='integrity')
    service = svc(h6, tmp_path)
    result = service.rotate('events')
    path = Path(result['file_path'])
    if damage_manifest:
        path = path.with_suffix(path.suffix + '.manifest.json')
    path.write_bytes(path.read_bytes() + b' ')
    with pytest.raises(ValueError, match='checksum'):
        service.restore_export(service.list_exports()[0]['id'])
    assert h6.conn.execute('SELECT count(*) FROM watering_events').fetchone()[0] == 0


def test_archive_write_failure_keeps_source_and_journal(h6, tmp_path, monkeypatch):
    c = mk_controller(h6)
    add_event(h6, cid=c['id'], uid='preserve')
    original = archive_integrity.durable_write
    calls = []
    def fail_on_manifest(path, data):
        calls.append(path)
        if len(calls) == 2:
            raise OSError('injected manifest failure')
        return original(path, data)
    monkeypatch.setattr(archive_integrity, 'durable_write', fail_on_manifest)
    service = svc(h6, tmp_path)
    with pytest.raises(OSError):
        service.rotate('events')
    assert h6.conn.execute('SELECT count(*) FROM watering_events').fetchone()[0] == 1
    assert service.list_exports() == []
    monkeypatch.setattr(archive_integrity, 'durable_write', original)
    assert service.rotate('events')['row_count'] == 1
    assert len(service.list_exports()) == 1


def test_zone_report_counts_fact_once(h6):
    c = mk_controller(h6)
    z = mk_zone(h6, c['id'], 1)
    now = int(time.time())
    with h6.conn:
        h6.conn.execute("""INSERT INTO watering_runs(controller_id,box_id,run_id,source,status,
            actual_start_ts,water_sec,volume_liters,zones_json,created_at,updated_at)
            VALUES(?,?,?,'schedule','completed',?,600,50,'[1]',datetime('now'),datetime('now'))""",
            (c['id'], c['box_id'], 'one-run', now))
    eid = add_event(h6, cid=c['id'], uid='one-finish', zone_id=z['id'], start_ts=now, volume=50)
    with h6.conn:
        h6.conn.execute('UPDATE watering_events SET run_id=? WHERE id=?', ('one-run', eid))
    assert reports.report_by_zones(h6.conn, None, None)[0]['total_liters'] == 50


@pytest.mark.parametrize('same_box', [False, True])
def test_stop_bypasses_ack_wait_and_cancels_earlier_retries(h6, same_box):
    # Let the harness maintenance iteration finish before synthetic online state.
    for worker in h6.mqtt._threads:
        worker.join(2)
    for box in ('AUDIT-A', 'AUDIT-B'):
        h6.controller(box)
        with h6.conn:
            h6.conn.execute("UPDATE controllers SET connection_status='online' WHERE box_id=?", (box,))
    h6.cmd._db_path = str(h6.db_path)
    h6.cfg.raw['mqtt'].update(command_timeout_sec=0.5, command_retries=1)
    started, stopped = threading.Event(), threading.Event()
    original = h6.mqtt.publish
    records, errors = [], []
    def publish(topic, payload, **kwargs):
        records.append((topic, payload['command']))
        result = original(topic, payload, **kwargs)
        (stopped if payload['command'] == 'stop_all' else started).set()
        return result
    h6.mqtt.publish = publish
    def send(box, command):
        try:
            h6.cmd.send_command(box, command, {})
        except Exception as exc:
            errors.append(exc)
    first = threading.Thread(target=send, args=('AUDIT-A', 'ping'))
    second = threading.Thread(target=send, args=('AUDIT-A' if same_box else 'AUDIT-B', 'stop_all'))
    try:
        first.start()
        assert started.wait(1)
        second.start()
        assert stopped.wait(0.3), 'Stop waited behind another command ACK'
    finally:
        first.join(3)
        if second.ident is not None:
            second.join(3)
    assert not first.is_alive() and not second.is_alive()
    assert not errors
    if same_box:
        assert len([r for r in records if r[1] == 'ping']) == 1


@pytest.mark.parametrize('payload', [None, [], 1, {'dry_run': 'false'}, {'target_date': 42}, {'entity_type': []}, {'extra': True}])
def test_archive_api_rejects_invalid_input_without_deletion(h6, payload):
    c = mk_controller(h6)
    add_event(h6, cid=c['id'], uid='api-preserve')
    register_api_routes_reports(h6.app, h6.cfg, h6.conn)
    response = h6.login_as('operator').post('/api/v1/archive/rotate', content=json.dumps(payload), headers={'content-type': 'application/json'})
    assert response.status_code == 422
    assert h6.conn.execute('SELECT count(*) FROM watering_events').fetchone()[0] == 1
    assert h6.conn.execute('SELECT count(*) FROM archive_exports').fetchone()[0] == 0


def test_connection_factory_enforces_foreign_keys(h6):
    conn = connect_path(h6.db_path)
    try:
        assert conn.execute('PRAGMA foreign_keys').fetchone()[0] == 1
        assert conn.execute('PRAGMA busy_timeout').fetchone()[0] == 5000
        import sqlite3
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute("INSERT INTO watering_event_zones(event_id,zone_id) VALUES(-123,1)")
    finally:
        conn.close()
