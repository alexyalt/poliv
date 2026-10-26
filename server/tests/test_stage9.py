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
    p=_mk_program(h.conn,name=f'Program-{c["id"]}',start_time='06:00');_link(h.conn,p,z['id'])
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


def normalization_fixture(h):
    from server.tests.test_stage8_archive import add_event
    from server.app.services.schedule_service import ScheduleService
    from server.app.services.normalization_service import NormalizationService
    controllers=[]
    for _ in range(2):
        c,z,p=setup_program(h)
        for i in range(3):add_event(h,cid=c['id'],uid=f"stats-{c['id']}-{i}",zone_id=z['id'],start_ts=int(time.time())-86400*(i+1))
        controllers.append(c['id'])
    schedules=ScheduleService(h.cfg,h.conn,db_path=str(h.db_path))
    return NormalizationService(h.conn,h.cfg,schedules),controllers


def test_normalization_propose_confirm_preserves_duration(h6):
    from datetime import timedelta
    from server.app.services.normalization_service import NormalizationError
    service,ids=normalization_fixture(h6)
    day=(date.today()+timedelta(days=1)).isoformat()
    job=service.create({'scope':'day','date':day,'controller_ids':ids},'operator')
    assert job['status']=='ready',job
    proposal=service.proposal(job['id'])['payload']
    assert proposal['metrics_after']['peak_lpm']<proposal['metrics_before']['peak_lpm']
    assert proposal['metrics_after']['estimated_liters']==proposal['metrics_before']['estimated_liters']
    assert h6.conn.execute('SELECT count(*) FROM normalization_overrides').fetchone()[0]==0
    with pytest.raises(NormalizationError):service.apply(job['id'],'operator')
    applied=service.apply(job['id'],'operator',True)
    assert len(applied['schedule_ids'])==2
    assert h6.conn.execute('SELECT count(*) FROM normalization_overrides').fetchone()[0]==2
    assert service.apply(job['id'],'operator',True)['already_applied']
    assert h6.conn.execute('SELECT count(*) FROM controller_schedules').fetchone()[0]==2
    service.schedules.close()


def test_normalization_stale_reject_and_insufficient(h6):
    from datetime import timedelta
    from server.app.services.normalization_service import StaleProposal,NormalizationError
    service,ids=normalization_fixture(h6)
    data={'date':(date.today()+timedelta(days=1)).isoformat(),'controller_ids':ids}
    job=service.create(data,'op')
    with h6.conn:h6.conn.execute("UPDATE zones SET name=name||' changed'")
    with pytest.raises(StaleProposal):service.apply(job['id'],'op',True)
    service.reject(job['id'],'op')
    with pytest.raises(NormalizationError):service.apply(job['id'],'op',True)
    with h6.conn:h6.conn.execute('DELETE FROM watering_event_zones');h6.conn.execute('DELETE FROM watering_events')
    job=service.create(data,'op')
    assert job['status']=='error' and 'Недостаточно' in job['error_message']
    service.schedules.close()


def test_normalization_week_deterministic(h6):
    from datetime import timedelta
    service,ids=normalization_fixture(h6)
    data={'scope':'week','target':'smooth_consumption','date':(date.today()+timedelta(days=1)).isoformat(),'controller_ids':ids}
    a=service.create(data,'op');b=service.create(data,'op')
    assert a['status']==b['status']=='ready'
    pa,pb=service.proposal(a['id']),service.proposal(b['id'])
    assert pa['after_hash']==pb['after_hash']
    assert len(pa['payload']['after'])==14
    service.schedules.close()


def test_stage9_api_roles_blocks_jobs(h6):
    from server.app.api.routes_stage9 import register_api_routes_stage9
    service,ids=normalization_fixture(h6)
    h6.app.state.app_dir=str(__import__('pathlib').Path(__file__).resolve().parents[1]/'app')
    register_api_routes_stage9(h6.app,h6.cfg,h6.conn,service.schedules,h6.cmd)
    viewer=h6.login_as('viewer');op=h6.login_as('operator')
    assert viewer.get('/api/v1/normalization/jobs').status_code==403
    assert viewer.post('/api/v1/zone-blocks',json={}).status_code==403
    assert op.post('/api/v1/zone-blocks',json=[]).status_code==422
    zid=h6.conn.execute('SELECT id FROM zones ORDER BY id').fetchone()[0]
    response=op.post(f'/api/v1/zones/{zid}/quick-block',json={'duration_hours':1,'kind':'all','reason':'test'})
    assert response.status_code==201,response.text
    lock=response.json()['block']['id']
    assert op.get('/api/v1/zone-blocks').json()['items'][0]['active']==1
    assert op.delete(f'/api/v1/zone-blocks/{lock}').status_code==200
    assert op.get('/stage9').status_code==200
    from datetime import timedelta
    response=op.post('/api/v1/normalization/jobs',json={'date':(date.today()+timedelta(days=1)).isoformat(),'controller_ids':ids})
    assert response.status_code==201,response.text
    assert response.json()['status']=='ready',response.text
    identity=response.json()['id']
    assert op.get(f'/api/v1/normalization/jobs/{identity}/proposal').status_code==200
    assert op.post(f'/api/v1/normalization/jobs/{identity}/apply',json={}).status_code==422
    assert op.post(f'/api/v1/normalization/jobs/{identity}/reject',json={}).status_code==200
    service.schedules.close()
