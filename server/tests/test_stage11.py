from pathlib import Path
import pytest
from server.tools.broker_config import acl_for, generate
from server.tests.test_stage5 import _Harness5


def test_broker_acl_isolates_controllers(tmp_path):
    text = acl_for(['5B2338', 'TESTBOX-0001'])
    device = text.split('user box_5B2338\n')[1].split('\nuser ')[0]
    assert 'topic read poliv/5B2338/command' in device
    assert 'topic write poliv/5B2338/event' in device
    assert '+' not in device and '#' not in device
    assert 'poliv/TESTBOX-0001' not in device
    for bad in ['a\nuser admin', '../x', '+', '', 'lowercase']:
        with pytest.raises(ValueError):
            acl_for([bad])
    out = tmp_path / 'broker'
    generate(out, ['5B2338'])
    assert 'allow_anonymous false' in (out / 'poliv.conf').read_text()
    with pytest.raises(FileExistsError):
        generate(out, ['5B2338'])


def test_two_dashboards_render_and_require_login():
    h = _Harness5()
    try:
        for url in ['/', '/active-zones']:
            assert h.client.get(url, follow_redirects=False).status_code == 302
        client = h.login_as('admin')
        for url in ['/', '/active-zones']:
            r = client.get(url)
            assert r.status_code == 200, r.text[:1000]
            assert 'dashboard-tiles' in r.text
            assert 'start-dialog' in r.text
            assert '/active-zones' in r.text
    finally:
        h.close()


def test_runtime_backup_restore_and_corruption(tmp_path):
    import sqlite3
    from server.app.infra.db import migrate
    from server.tools.runtime_snapshot import snapshot, restore, verify
    source = tmp_path / 'runtime'
    (source / 'data').mkdir(parents=True)
    with sqlite3.connect(source / 'data/poliv.db') as conn:
        conn.row_factory = sqlite3.Row
        migrate(conn)
    backup, restored = tmp_path / 'backup', tmp_path / 'restored'
    snapshot(source, backup)
    verify(backup)
    restore(backup, restored)
    verify(restored)
    assert (source / 'data/poliv.db').exists()
    with pytest.raises(ValueError, match="must not exist"):
        snapshot(source, backup)
    (backup / 'data/extra.txt').write_text('unexpected')
    with pytest.raises(ValueError, match='mismatch'):
        restore(backup, tmp_path / 'bad-restore')
    assert not (tmp_path / 'bad-restore').exists()
