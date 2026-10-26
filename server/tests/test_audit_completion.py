"""Completion regressions: transaction ownership, persistent replay protection."""
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.app.infra.connection_scope import ScopedConnection, database_scope, DatabaseScopeMiddleware
from server.app.infra.db import connect_path
from server.tests.test_stage8_archive import h6, mk_controller, mk_zone, svc
from server.app.services import report_service
from server.app.api.routes_reports import register_api_routes_reports
from server.app.services.mqtt_command_service import MqttUnavailable


def test_rollback_cannot_undo_another_task(h6):
    connection = ScopedConnection(h6.db_path)
    started, finished = threading.Event(), threading.Event()
    def rollback():
        with database_scope():
            with pytest.raises(RuntimeError):
                with connection:
                    connection.execute("INSERT INTO settings(key,value_json) VALUES('rollback', '1')")
                    started.set()
                    # Other task's writer waits for rollback, never commits our tx.
                    raise RuntimeError('injected rollback')
    def commit():
        assert started.wait(2)
        with database_scope(), connection:
            connection.execute("INSERT INTO settings(key,value_json) VALUES('committed', '2')")
        finished.set()
    with ThreadPoolExecutor(2) as pool:
        futures=[pool.submit(rollback),pool.submit(commit)]
        for f in futures: f.result(timeout=5)
    assert finished.is_set()
    assert h6.conn.execute("SELECT 1 FROM settings WHERE key='rollback'").fetchone() is None
    assert h6.conn.execute("SELECT value_json FROM settings WHERE key='committed'").fetchone()[0]=='2'
    assert not connection._fallback


def test_nested_context_does_not_commit_caller(h6):
    conn=ScopedConnection(h6.db_path)
    with database_scope(), pytest.raises(RuntimeError):
        with conn:
            conn.execute("INSERT INTO settings(key,value_json) VALUES('outer','1')")
            with conn:
                conn.execute("INSERT INTO settings(key,value_json) VALUES('inner','1')")
            raise RuntimeError('rollback both')
    assert h6.conn.execute("SELECT count(*) FROM settings WHERE key IN ('outer','inner')").fetchone()[0]==0


def test_request_connections_close_and_are_distinct(h6):
    conn=ScopedConnection(h6.db_path)
    app=FastAPI()
    app.add_middleware(DatabaseScopeMiddleware)
    barrier=threading.Barrier(2)
    natives=[]
    @app.get('/read')
    def read():
        native=conn.native()
        natives.append(native)
        barrier.wait(timeout=3)
        return {'count':conn.execute('SELECT count(*) FROM users').fetchone()[0]}
    def call():
        with TestClient(app) as client:
            return client.get('/read')
    with ThreadPoolExecutor(2) as pool:
        responses=list(pool.map(lambda _:call(),range(2)))
    assert all(r.status_code==200 for r in responses)
    assert natives[0] is not natives[1]
    for native in natives:
        with pytest.raises(sqlite3.ProgrammingError): native.execute('SELECT 1')


def test_replayed_event_after_archive_does_not_reapply(h6,tmp_path):
    c=mk_controller(h6)
    mk_zone(h6,c['id'],1)
    payload=h6.event_payload(c['box_id'],status='finished',event_uid='durable-replay',
                              start_ts=int(time.time())-400*86400,volume_liters=50)
    h6.inject_event(c['box_id'],payload)
    before=[tuple(r) for r in h6.conn.execute('SELECT * FROM flow_daily')]
    assert before
    s=svc(h6,tmp_path)
    assert s.rotate('events')['row_count']==1
    assert h6.conn.execute('SELECT count(*) FROM watering_events').fetchone()[0]==0
    h6.inject_event(c['box_id'],payload)
    assert h6.conn.execute('SELECT count(*) FROM watering_events').fetchone()[0]==0
    assert [tuple(r) for r in h6.conn.execute('SELECT * FROM flow_daily')]==before


def test_legacy_parallel_event_volume_is_not_multiplied(h6):
    c=mk_controller(h6)
    zones=[mk_zone(h6,c['id'],n) for n in (1,2)]
    with h6.conn:
        cur=h6.conn.execute("INSERT INTO watering_events(event_uid,controller_id,source,status,start_ts,volume_liters) VALUES('parallel',?,'manual','finished',?,60)",(c['id'],int(time.time())))
        for zone in zones:
            h6.conn.execute('INSERT INTO watering_event_zones(event_id,zone_id,volume_liters) VALUES(?,?,60)',(cur.lastrowid,zone['id']))
    data=report_service.report_by_zones(h6.conn,None,None)
    assert [r['total_liters'] for r in data]==[30,30]


def test_missed_report_json_and_csv(h6):
    c=mk_controller(h6)
    with h6.conn:
        h6.conn.execute("""INSERT INTO watering_runs(controller_id,box_id,run_id,source,status,reason_code,
                       planned_start_ts,created_at,updated_at) VALUES(?,?,'missed-test','schedule','skipped',
                       'weather_skip',?,datetime('now'),datetime('now'))""",(c['id'],c['box_id'],int(time.time())))
    register_api_routes_reports(h6.app,h6.cfg,h6.conn)
    client=h6.login_as('viewer')
    data=client.get('/api/v1/reports/missed')
    assert data.status_code==200
    assert data.json()['data'][0]['reason']=='weather_skip'
    csv=client.get('/api/v1/reports/missed.csv')
    assert csv.status_code==200 and 'weather_skip' in csv.text


def test_async_capacity_reserved_for_stop(h6,monkeypatch):
    c=h6.controller('CAPACITY')
    with h6.conn:
        h6.conn.execute("UPDATE controllers SET connection_status='online' WHERE id=?",(c['id'],))
    for worker in h6.mqtt._threads: worker.join(2)
    with h6.conn:
        h6.conn.execute("UPDATE controllers SET connection_status='online' WHERE id=?",(c['id'],))
    # Exhaust ordinary capacity without launching any thread.
    for _ in range(16): assert h6.cmd._worker_slots.acquire(False)
    with pytest.raises(MqttUnavailable): h6.cmd.send_command_async(c['id'],'ping',{})
    h6.cfg.raw['mqtt'].update(command_timeout_sec=0.05,command_retries=0)
    assert h6.cmd.send_command_async(c['id'],'stop_all',{})
    h6.cmd.close()
    assert not h6.cmd._workers
    for _ in range(16): h6.cmd._worker_slots.release()


@pytest.mark.parametrize('entity', ['flow','weather'])
def test_all_archived_entities_restore(h6,tmp_path,entity):
    from server.tests.test_stage8_archive import add_flow_daily
    c=mk_controller(h6)
    if entity=='flow':
        add_flow_daily(h6,date='2020-01-01',cid=c['id'],liters=17.5)
        table='flow_daily'
    else:
        with h6.conn:
            h6.conn.execute("INSERT INTO weather_observations(ts,observed_at,temp_c,source) VALUES(1,'2020-01-01',12.5,'open_meteo')")
        table='weather_observations'
    before=[tuple(r) for r in h6.conn.execute('SELECT * FROM '+table)]
    service=svc(h6,tmp_path)
    service.rotate(entity)
    assert service.restore_export(service.list_exports(entity)[0]['id'])['restored']==1
    assert [tuple(r) for r in h6.conn.execute('SELECT * FROM '+table)]==before


def test_slow_report_does_not_block_event_loop(h6,monkeypatch):
    import asyncio
    import httpx
    started, release=threading.Event(),threading.Event()
    h6.login_as('viewer')
    # Use explicit scoped service/route connection as in production main.
    scoped=ScopedConnection(h6.db_path)
    h6.app.state.auth.conn=scoped
    h6.app.add_middleware(DatabaseScopeMiddleware)
    register_api_routes_reports(h6.app,h6.cfg,scoped)
    def slow(conn, a, b):
        started.set()
        assert release.wait(3)
        return []
    monkeypatch.setitem(report_service.REPORTS,'zones',slow)
    @h6.app.get('/ping-independent')
    async def ping(): return {'ok':True}
    token,_,_=h6.auth.login('user_viewer','test-pw-123','test','')
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=h6.app),base_url='http://testserver',cookies={'poliv_session':token}) as client:
            task=asyncio.create_task(client.get('/api/v1/reports/zones'))
            try:
                assert await asyncio.to_thread(started.wait,2)
                response=await asyncio.wait_for(client.get('/ping-independent'),timeout=0.5)
                assert response.status_code==200
            finally:
                release.set()
                await task
    try:
        asyncio.run(exercise())
    finally:
        scoped.close()


def test_prepare_runtime_preserves_source(h6,tmp_path):
    from server.tools.prepare_runtime import prepare
    source=tmp_path/'checkout'
    (source/'data').mkdir(parents=True)
    (source/'data/schedules').mkdir()
    (source/'data/schedules/example.json').write_text('{}')
    destination=tmp_path/'runtime'
    result=prepare(source,destination,h6.db_path)
    assert result['source_preserved']
    assert h6.db_path.exists()
    assert (destination/'data/schedules/example.json').exists()
    with sqlite3.connect(destination/'data/poliv.db') as copy:
        assert copy.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    with pytest.raises(ValueError): prepare(source,destination,h6.db_path)


def test_nested_first_write_still_rolls_back_outer(h6):
    conn=ScopedConnection(h6.db_path)
    with database_scope(), pytest.raises(RuntimeError):
        with conn:
            with conn:
                conn.execute("INSERT INTO settings(key,value_json) VALUES('nested-first','1')")
            raise RuntimeError('rollback outer')
    assert h6.conn.execute("SELECT 1 FROM settings WHERE key='nested-first'").fetchone() is None


def test_ledger_migration_backfills_and_is_idempotent(h6):
    import importlib.util
    c=mk_controller(h6)
    with h6.conn:
        h6.conn.execute("INSERT INTO watering_events(event_uid,controller_id,source,status,start_ts) VALUES('backfill',?,'manual','finished',1)",(c['id'],))
    path=Path(__file__).resolve().parents[1]/'migrations/0012_processed_events.py'
    spec=importlib.util.spec_from_file_location('ledger_upgrade',path)
    migration=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with h6.conn:
        migration.upgrade(h6.conn)
        migration.upgrade(h6.conn)
    assert h6.conn.execute("SELECT count(*) FROM processed_events WHERE event_uid='backfill'").fetchone()[0]==3


def test_archive_rotation_drains_bounded_batches(h6,tmp_path):
    c=mk_controller(h6)
    with h6.conn:
        h6.conn.executemany("INSERT INTO watering_events(event_uid,controller_id,source,status,start_ts) VALUES(?,?,'manual','finished',1)",
                            [(f'batch-{i}',c['id']) for i in range(1002)])
    service=svc(h6,tmp_path)
    results=service.rotate_all()
    batches=[r for r in results if r['entity_type']=='events']
    assert [r['row_count'] for r in batches]==[1000,2]
    assert h6.conn.execute('SELECT count(*) FROM watering_events').fetchone()[0]==0


def test_replay_resumes_flow_consumer_after_interruption(h6):
    c=mk_controller(h6)
    mk_zone(h6,c['id'],1)
    payload=h6.event_payload(c['box_id'],status='finished',event_uid='resume-flow',volume_liters=23)
    h6.inject_event(c['box_id'],payload)
    original=[tuple(r) for r in h6.conn.execute('SELECT date,controller_id,zone_id,attribution,total_volume_l,watering_count,water_sec,anomaly_count FROM flow_daily')]
    assert original
    with h6.conn:
        h6.conn.execute('DELETE FROM flow_daily')
        h6.conn.execute("DELETE FROM processed_events WHERE consumer='flow' AND event_uid='resume-flow'")
    h6.inject_event(c['box_id'],payload)
    assert [tuple(r) for r in h6.conn.execute('SELECT date,controller_id,zone_id,attribution,total_volume_l,watering_count,water_sec,anomaly_count FROM flow_daily')]==original
    h6.inject_event(c['box_id'],payload)
    assert [tuple(r) for r in h6.conn.execute('SELECT date,controller_id,zone_id,attribution,total_volume_l,watering_count,water_sec,anomaly_count FROM flow_daily')]==original


def test_coroutines_on_same_thread_have_separate_connections(h6):
    import asyncio
    conn = ScopedConnection(h6.db_path)
    async def exercise():
        entered, inspected = asyncio.Event(), asyncio.Event()
        natives = []
        async def first():
            with database_scope():
                with pytest.raises(RuntimeError):
                    with conn:
                        natives.append(conn.native())
                        conn.execute("INSERT INTO settings(key,value_json) VALUES('async-owner','1')")
                        entered.set()
                        await inspected.wait()
                        raise RuntimeError('rollback first task')
        async def second():
            await entered.wait()
            try:
                with database_scope(), conn:
                    natives.append(conn.native())
                    assert conn.execute("SELECT 1 FROM settings WHERE key='async-owner'").fetchone() is None
            finally:
                inspected.set()
        await asyncio.wait_for(asyncio.gather(first(),second()),timeout=3)
        assert natives[0] is not natives[1]
    asyncio.run(exercise())
    assert h6.conn.execute("SELECT 1 FROM settings WHERE key='async-owner'").fetchone() is None


def test_child_tasks_in_one_scope_do_not_share_transaction(h6):
    import asyncio
    conn = ScopedConnection(h6.db_path)
    async def exercise():
        natives=[]
        async def read():
            with conn:
                natives.append(conn.native())
                await asyncio.sleep(0)
                assert conn.execute('SELECT 1').fetchone()[0] == 1
        with database_scope():
            await asyncio.gather(read(),read())
        assert natives[0] is not natives[1]
        for native in natives:
            with pytest.raises(sqlite3.ProgrammingError): native.execute('SELECT 1')
    asyncio.run(exercise())


def test_runtime_copy_remaps_local_archive_and_schedule(h6,tmp_path):
    import json, hashlib
    from server.tools.prepare_runtime import prepare
    source, destination = tmp_path/'checkout', tmp_path/'runtime'
    (source/'data/schedules/B').mkdir(parents=True)
    schedule=source/'data/schedules/B/v1.json'
    schedule.write_text('{}')
    (source/'data/archive/events').mkdir(parents=True)
    archive=source/'data/archive/events/one.json'
    archive.write_text('[]')
    manifest=archive.with_suffix('.json.manifest.json')
    manifest.write_text('{"schema_version":2}')
    db=source/'data/poliv.db'
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE controller_schedules(id INTEGER PRIMARY KEY,payload_path TEXT)')
        conn.execute('CREATE TABLE archive_exports(id INTEGER PRIMARY KEY,file_path TEXT,format_version INTEGER,file_hash TEXT,manifest_hash TEXT)')
        conn.execute('INSERT INTO controller_schedules VALUES(1,?)',('data/schedules/B/v1.json',))
        conn.execute('INSERT INTO archive_exports VALUES(1,?,2,?,?)',
                     (str(archive),hashlib.sha256(archive.read_bytes()).hexdigest(),hashlib.sha256(manifest.read_bytes()).hexdigest()))
    result=prepare(source,destination)
    assert result['ready'] and len(result['relocated_files'])==2
    assert result['external_files']==[]
    with sqlite3.connect(destination/'data/poliv.db') as conn:
        copied_schedule=Path(conn.execute('SELECT payload_path FROM controller_schedules').fetchone()[0])
        copied_archive=Path(conn.execute('SELECT file_path FROM archive_exports').fetchone()[0])
    assert copied_schedule==destination/'data/schedules/B/v1.json'
    assert copied_archive==destination/'data/archive/events/one.json'
    # Detaching source demonstrates that copied DB uses only the new local paths.
    source.rename(tmp_path/'detached-source')
    assert copied_schedule.is_file() and copied_archive.is_file()
    assert copied_archive.with_suffix('.json.manifest.json').is_file()


def test_runtime_copy_marks_missing_referenced_file_incomplete(tmp_path):
    import json
    from server.tools.prepare_runtime import prepare
    source, destination = tmp_path/'checkout', tmp_path/'runtime'
    (source/'data').mkdir(parents=True)
    with sqlite3.connect(source/'data/poliv.db') as conn:
        conn.execute('CREATE TABLE controller_schedules(id INTEGER PRIMARY KEY,payload_path TEXT)')
        conn.execute("INSERT INTO controller_schedules VALUES(1,'data/schedules/missing.json')")
    with pytest.raises(ValueError,match='not copied'):
        prepare(source,destination)
    assert json.loads((destination/'runtime-copy.json').read_text())['ready'] is False
    assert (source/'data/poliv.db').is_file()
