"""Stage-9 browser integration smoke: Chromium UI -> real FastAPI/SQLite services.

Uses the project's isolated fake-MQTT harness; no production server/hardware.
Run after installing optional Playwright/Chromium: python -m server.tools.stage9_smoke
"""
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import time
from urllib.parse import urlsplit


def main():
    with tempfile.TemporaryDirectory(prefix='poliv-stage9-browser-') as folder:
        local=Path(folder)/'local.toml'
        local.write_text('[server]\nsecret_key="stage9-browser-isolated-test-key"\n')
        for key,value in {'POLIV_CONFIG_LOCAL':str(local),'POLIV_DATA_ROOT':folder}.items():os.environ[key]=value
        from server.tests.test_stage6 import _Harness6
        from server.tests.test_stage8_archive import mk_controller,mk_zone,add_event
        from server.tests.test_stage7 import _mk_program,_link
        from server.app.services.schedule_service import ScheduleService
        from server.app.api.routes_stage9 import register_api_routes_stage9
        from server.app.services.heatmap_service import build_heatmap
        from playwright.sync_api import sync_playwright
        with contextlib.redirect_stdout(io.StringIO()):h=_Harness6()
        schedules=ScheduleService(h.cfg,h.conn,db_path=str(h.db_path))
        try:
            schedules.attach(h.mqtt);h.mqtt.attach_schedule_service(schedules)
            h.app.state.app_dir=str(Path(__file__).resolve().parents[1]/'app')
            register_api_routes_stage9(h.app,h.cfg,h.conn,schedules,h.cmd)
            ids=[]
            for n in range(2):
                c=mk_controller(h);z=mk_zone(h,c['id'],1);ids.append(c['id'])
                _link(h.conn,_mk_program(h.conn,name='Browser-'+str(n),start_time='06:00'),z['id'])
                for i in range(3):add_event(h,cid=c['id'],uid=f'browser-{n}-{i}',zone_id=z['id'],start_ts=int(time.time())-86400*(i+1))
            client=h.login_as('operator')
            with sync_playwright() as p:
                browser=p.chromium.launch(headless=True,args=['--no-sandbox'])
                page=browser.new_page();errors=[]
                page.on('pageerror',lambda exc:errors.append(str(exc)))
                def forward(route):
                    request=route.request;url=urlsplit(request.url)
                    if url.path.startswith('/static/'):
                        route.fulfill(body='');return
                    response=client.request(request.method,url.path+('?' +url.query if url.query else ''),content=request.post_data,
                                            headers={'content-type':request.headers.get('content-type','application/json')})
                    route.fulfill(status=response.status_code,body=response.content,
                                  headers={'content-type':response.headers.get('content-type','text/html')})
                page.route('http://stage9.test/**',forward)
                page.goto('http://stage9.test/stage9')
                page.fill('#block-zone','1');page.fill('#block-reason','browser repair')
                page.click('#block-hour')
                page.wait_for_function("document.querySelector('#blocks').textContent.includes('browser repair')")
                page.locator('#blocks button').first.click()
                page.wait_for_function("document.querySelector('#blocks').textContent.includes('снята')")
                page.fill('#norm-controllers',','.join(map(str,ids)))
                page.locator('#normalize-form button').click()
                page.wait_for_selector('#norm-confirm')
                assert h.conn.execute('SELECT count(*) FROM normalization_overrides').fetchone()[0]==0
                page.locator('#proposal button').filter(has_text='Применить').click()
                page.wait_for_function("document.querySelector('#stage9-error').textContent.includes('Подтвердите')")
                assert h.conn.execute('SELECT count(*) FROM normalization_overrides').fetchone()[0]==0
                page.check('#norm-confirm')
                page.locator('#proposal button').filter(has_text='Применить').click()
                page.wait_for_function("document.querySelector('#jobs').textContent.includes('applied')")
                assert h.conn.execute('SELECT count(*) FROM normalization_overrides').fetchone()[0]==2
                for row in h.conn.execute('SELECT cs.*,c.box_id FROM controller_schedules cs JOIN controllers c ON c.id=cs.controller_id WHERE cs.status=\'sent\'').fetchall():
                    schedules.on_schedule_ack(row['box_id'],{'schedule_version':row['schedule_version'],'status':'applied','schedule_hash':row['schedule_hash']})
                day=page.input_value('#norm-date')
                heatmap=build_heatmap(h.conn,day,interval_min=15)
                assert any('water' in z['cells'] for c in heatmap['rows'] for z in c['zones'])
                assert not errors,errors
                browser.close()
                print('Stage9 Chromium/FastAPI/SQLite: quick block/release, proposal, explicit confirmation, apply/ACK/heatmap OK')
        finally:
            h.cmd.close();schedules.close();h.close();h.conn.close()


if __name__=='__main__':main()
