"""Account security, archive upgrade and concurrent writer regression tests."""
import importlib.util
import json
import sqlite3
import threading
from pathlib import Path
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from server.tests.test_stage8_archive import h6, mk_controller, add_event, svc
from server.app.web.account_security import register_account_security
from server.app.web.routes import register_web_routes
from server.app.infra.db import connect_path
from server.app.services.archive_service import ArchiveService
from server.app.services import archive_integrity


def test_password_change_guard_and_other_sessions_revoked(h6):
    client = h6.login_as('operator')
    token, _, _ = h6.auth.login('user_operator', 'test-pw-123', '127.0.0.2', '')
    app = h6.app
    register_account_security(app, h6.cfg, str(h6.db_path))
    with TestClient(app) as guarded:
        # Mirror a browser host-only login cookie, not the harness global-domain cookie.
        guarded.cookies.set('poliv_session', client.cookies.get('poliv_session'),
                            domain='testserver.local', path='/')
        response = guarded.get('/api/controllers', follow_redirects=False)
        assert response.status_code == 403
        assert response.json()['code'] == 'password_change_required'
        assert guarded.get('/account/password').status_code == 200
        assert guarded.post('/account/password', data={'current_password':'bad', 'new_password':'new-password-123', 'confirm_password':'new-password-123'}).status_code == 422
        response = guarded.post('/account/password', data={'current_password':'test-pw-123', 'new_password':'new-password-123', 'confirm_password':'new-password-123'}, follow_redirects=False)
        assert response.status_code == 303
        assert h6.auth.get_user_by_token(token, touch=False) is None
        assert guarded.get('/api/controllers').status_code == 200
        assert h6.auth.get_user_by_token(guarded.cookies.get('poliv_session'), touch=False)['must_change_password'] == 0


def test_origin_rejected_before_mutation(h6):
    register_account_security(h6.app, h6.cfg, str(h6.db_path))
    @h6.app.post('/test-write')
    def write():
        return {'ok': True}
    with TestClient(h6.app) as client:
        assert client.post('/test-write', headers={'origin':'https://untrusted.example'}).status_code == 403
        assert client.post('/test-write', headers={'origin':'null'}).status_code == 403
        assert client.post('/test-write', headers={'origin':'http://testserver'}).status_code == 200
        assert client.post('/test-write', headers={'sec-fetch-site':'cross-site'}).status_code == 403


def test_secure_cookie_with_https_and_local_http(h6):
    h6.login_as('operator')
    h6.app.state.app_dir = str(Path(__file__).resolve().parents[1] / 'app')
    register_web_routes(h6.app, h6.cfg, h6.conn, h6.auth)
    for scheme, secure in [('https', True), ('http', False)]:
        with TestClient(h6.app, base_url=scheme+'://testserver') as client:
            response = client.post('/login', data={'username':'user_operator', 'password':'test-pw-123'}, follow_redirects=False)
            assert response.status_code == 302
            assert ('Secure' in response.headers['set-cookie']) is secure


def test_upgrade_preserves_legacy_journal_and_reader(h6, tmp_path):
    c = mk_controller(h6)
    eid = add_event(h6, cid=c['id'], uid='legacy')
    rec = dict(h6.conn.execute('SELECT * FROM watering_events WHERE id=?', (eid,)).fetchone())
    archive = tmp_path/'legacy.json'
    archive.write_text(json.dumps([rec]))
    with h6.conn:
        h6.conn.execute('DELETE FROM watering_events WHERE id=?', (eid,))
        h6.conn.execute("INSERT INTO archive_exports(entity_type,date,file_path,row_count) VALUES('events','2025-01-01',?,1)", (str(archive),))
    service = svc(h6,tmp_path)
    export=service.list_exports()[0]
    assert export['format_version']==1
    result=service.restore_export(export['id'])
    assert result['restored']==1 and result['legacy_incomplete']
    assert archive.read_text()==json.dumps([rec])
    # Migration 0011 is additive/idempotent on a real 0010 schema.
    old=sqlite3.connect(tmp_path/'old.db')
    old.execute('CREATE TABLE archive_exports(id INTEGER PRIMARY KEY, file_path TEXT)')
    old.execute("INSERT INTO archive_exports VALUES(1,'unchanged.csv')")
    old.commit()
    path=Path(__file__).resolve().parents[1]/'migrations/0011_archive_integrity.py'
    spec=importlib.util.spec_from_file_location('audit_migration',path)
    migration=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with old:
        migration.upgrade(old)
        migration.upgrade(old)
    assert old.execute('SELECT file_path,format_version FROM archive_exports').fetchone()==('unchanged.csv',1)
    old.close()


def test_concurrent_rotation_selects_each_source_once(h6,tmp_path):
    c=mk_controller(h6)
    for n in range(10):
        add_event(h6,cid=c['id'],uid=f'parallel-{n}')
    barrier=threading.Barrier(2)
    results,errors=[],[]
    def rotate():
        conn=connect_path(h6.db_path)
        try:
            barrier.wait(timeout=3)
            results.append(ArchiveService(conn,tmp_path).rotate('events'))
        except Exception as exc:
            errors.append(exc)
        finally:
            conn.close()
    workers=[threading.Thread(target=rotate) for _ in range(2)]
    for worker in workers: worker.start()
    for worker in workers: worker.join(8)
    assert all(not worker.is_alive() for worker in workers)
    assert not errors
    assert sorted(r['row_count'] for r in results)==[0,10]
    assert h6.conn.execute('SELECT count(*) FROM archive_exports').fetchone()[0]==1


def test_journal_failure_rolls_back_source_deletion(h6,tmp_path):
    c=mk_controller(h6)
    add_event(h6,cid=c['id'],uid='journal-failure')
    with h6.conn:
        h6.conn.execute("CREATE TRIGGER fail_archive BEFORE INSERT ON archive_exports BEGIN SELECT RAISE(ABORT,'injected journal failure'); END")
    with pytest.raises(sqlite3.IntegrityError):
        svc(h6,tmp_path).rotate('events')
    assert h6.conn.execute('SELECT count(*) FROM watering_events').fetchone()[0]==1
    assert h6.conn.execute('SELECT count(*) FROM archive_exports').fetchone()[0]==0


def test_restore_conflict_is_atomic(h6,tmp_path):
    c=mk_controller(h6)
    eid=add_event(h6,cid=c['id'],uid='original')
    service=svc(h6,tmp_path)
    service.rotate('events')
    export=service.list_exports()[0]
    with h6.conn:
        h6.conn.execute("INSERT INTO watering_events(id,event_uid,controller_id,source,status,start_ts) VALUES(?, 'different',?,'manual','finished',1)",(eid,c['id']))
    with pytest.raises(ValueError,match='conflict'):
        service.restore_export(export['id'])
    assert h6.conn.execute('SELECT event_uid FROM watering_events').fetchone()[0]=='different'
