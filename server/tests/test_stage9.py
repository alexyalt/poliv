"""Stage-9A regressions with isolated SQLite and fake MQTT."""
import time
from datetime import date,datetime,timezone
import pytest
from server.tests.test_stage8_archive import h6, mk_controller, mk_zone
from server.tests.test_stage7 import _mk_program, _link
from server.app.services.block_service import BlockService,BlockError
from server.app.core.blocks.policy import blocked
from server.app.core.schedule_compiler.compiler import compile_schedule
from server.app.services.mqtt_command_service import CommandError


def setup_program(h):
    c=mk_controller(h);z=mk_zone(h,c['id'],1)
    p=_mk_program(h.conn,start_time='06:00');_link(h.conn,p,z['id'])
    return c,z,p


def test_block_half_open_release_and_audit(h6):
    c,z,_=setup_program(h6)
    service=BlockService(h6.conn)
    row=service.create({'zone_id':z['id'],'start_ts':100,'end_ts':200,'kind':'all','reason':'repair'},'operator')
    assert not blocked(h6.conn,z['id'],50,100)
    assert blocked(h6.conn,z['id'],199,201,manual=True)
    assert not blocked(h6.conn,z['id'],200,201)
    service.release(row['id'],'admin')
    assert not blocked(h6.conn,z['id'],150,180)
    assert h6.conn.execute('SELECT count(*) FROM zone_locks').fetchone()[0]==1
    assert h6.conn.execute("SELECT count(*) FROM logs WHERE action IN ('zone.blocked','zone.unblocked')").fetchone()[0]==2


@pytest.mark.parametrize('bad',[{'end_ts':10},{'block_manual':'false'},{'kind':'bad'},{'reason':''},{'start_ts':False}])
def test_block_validation(h6,bad):
    _,z,_=setup_program(h6)
    with pytest.raises(BlockError):BlockService(h6.conn).create({'zone_id':z['id'],'start_ts':100,'end_ts':200,'reason':'x',**bad},'admin')


def test_compiler_uses_occurrence_day_and_time(h6):
    c,z,p=setup_program(h6)
    day=date.today()
    lo=int(datetime.combine(day,datetime.min.time(),timezone.utc).timestamp())
    BlockService(h6.conn).create({'zone_id':z['id'],'start_ts':lo+6*3600,'end_ts':lo+7*3600,'reason':'morning'},'op')
    compiled=compile_schedule(h6.conn,c,today=day,timezone_offset_min=0,days_ahead=2)
    assert all(r['date']!=day.isoformat() for r in compiled.payload['runs'])
    assert len(compiled.payload['runs'])==2
    assert any(s['reason_code']=='zone_locked' for s in compiled.skipped_steps)
    # A lock later in the day must not delete the 06:00 run.
    with h6.conn:h6.conn.execute('DELETE FROM zone_locks')
    BlockService(h6.conn).create({'zone_id':z['id'],'start_ts':lo+12*3600,'end_ts':lo+13*3600,'reason':'noon'},'op')
    assert len(compile_schedule(h6.conn,c,today=day,timezone_offset_min=0,days_ahead=1).payload['runs'])==2


def test_manual_auto_only_allowed_all_rejected(h6):
    c,z,_=setup_program(h6)
    with h6.conn:h6.conn.execute("UPDATE controllers SET connection_status='online' WHERE id=?",(c['id'],))
    now=int(time.time());s=BlockService(h6.conn)
    row=s.create({'zone_id':z['id'],'start_ts':now-1,'end_ts':now+3600,'reason':'auto','kind':'auto'},'op')
    from server.app.services.block_service import check_manual
    check_manual(h6.conn,c['id'],'zone_open',{'zone':1,'duration_sec':60})
    s.release(row['id'],'op')
    s.create({'zone_id':z['id'],'start_ts':now-1,'end_ts':now+3600,'reason':'all','kind':'all'},'op')
    with pytest.raises(CommandError):h6.cmd.send_command(c['box_id'],'zone_open',{'zone':1,'duration_sec':60})
    with pytest.raises(CommandError):h6.cmd.send_command_async(c['id'],'zone_open',{'zone':1,'duration_sec':60})
