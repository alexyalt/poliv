"""Real Chromium + FastAPI/SQLite: forecast controls, settings and monitor failures."""
import argparse
import os
from pathlib import Path
import tempfile
from playwright.sync_api import sync_playwright
from server.tests.test_stage5 import _Harness5
from server.tests.test_weather_monitor import fixture
from server.app.api.routes_weather import register_api_routes_weather
from server.app.services import weather_outlook


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--screenshots')
    args=parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='poliv-weather-ui-') as directory:
        local=Path(directory)/'test.toml'
        local.write_text('[server]\nsecret_key="weather-monitor-browser-isolated-key12345678"\n')
        os.environ['POLIV_CONFIG_LOCAL']=str(local)
        h=_Harness5()
        old_fetch=weather_outlook.meteo._http_get
        try:
            register_api_routes_weather(h.app,h.cfg,h.conn)
            weather_outlook.meteo._http_get=lambda *a:fixture()
            weather_outlook.update(h.conn)
            client=h.login_as('admin')
            for i in range(50):
                box=f'MONITOR{i:02d}'
                c=h.catalog.create_controller({'box_id':box,'name':f'Участок {i+1:02d}'})
                h.add_zone(c['id'],1)
                h.set_live(box,connection_status='online' if i%7 else 'offline',
                           current_mode='error' if i%11==0 else 'manual' if i%3==0 else 'idle',
                           current_phase='water' if i%3==0 else 'idle',
                           active_zones_json='[1]' if i%3==0 else '[]',time_valid=1,flow_enabled=1,instant_lpm=8.5)
            with sync_playwright() as p:
                browser=p.chromium.launch(headless=True,args=['--no-sandbox'])
                page=browser.new_page(viewport={'width':1920,'height':1080})
                errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                fail_live=False
                def route(req):
                    path=req.request.url.split('http://testserver',1)[1]
                    if path.startswith('/static/'):
                        file=Path(__file__).resolve().parents[1]/'app'/path.split('?')[0].lstrip('/')
                        req.fulfill(status=200,body=file.read_bytes(),content_type='text/javascript' if file.suffix=='.js' else 'text/css');return
                    if path=='/api/controllers/live' and fail_live:
                        req.fulfill(status=503,body='unavailable');return
                    response=client.request(req.request.method,path,content=req.request.post_data,
                                            headers={'content-type':req.request.headers.get('content-type','')},follow_redirects=True)
                    req.fulfill(status=response.status_code,body=response.content,headers={k:v for k,v in response.headers.items() if k.lower() not in ('content-length','set-cookie')})
                page.route('http://testserver/**',route)
                page.goto('http://testserver/weather')
                page.wait_for_selector('.weather-chart svg')
                assert page.locator('.weather-chart').count()==6
                assert '44.423062' in page.locator('#outlook-meta').inner_text()
                assert page.locator('.weather-notice.danger').count()==1
                page.get_by_role('button',name='7 дней',exact=True).click()
                assert page.locator('.weather-chart').count()==5
                assert 'ET₀' in page.locator('#weather-charts').inner_text()
                page.locator('.weather-table summary').click()
                assert page.locator('#weather-values tbody tr').count()==7
                if args.screenshots:
                    folder=Path(args.screenshots);folder.mkdir(parents=True,exist_ok=True)
                    page.screenshot(path=str(folder/'weather-7days.png'),full_page=True)
                page.get_by_role('button',name='24 часа',exact=True).click()
                assert page.locator('#weather-values tbody tr').count()==24
                if args.screenshots:page.screenshot(path=str(folder/'weather-24hours.png'),full_page=True)
                page.goto('http://testserver/settings')
                latitude=page.locator('input[name="setting:weather.latitude"]')
                assert latitude.input_value()=='44.423062'
                latitude.fill('45')
                with page.expect_navigation():
                    page.locator('form').filter(has=latitude).get_by_role('button',name='Сохранить',exact=True).click()
                assert weather_outlook.weather_config(h.conn)['latitude']==45
                page.goto('http://testserver/weather')
                page.wait_for_function("document.querySelector('#outlook-meta').textContent.includes('45,')")
                assert page.locator('.weather-chart').count()==0
                assert 'недоступен' in page.locator('#weather-alerts').inner_text()
                page.goto('http://testserver/')
                assert page.locator('.controller-tile').count()==50
                assert 'state-error' in page.locator('.controller-tile').first.get_attribute('class')
                if args.screenshots:page.screenshot(path=str(folder/'monitor-50.png'),full_page=True)
                page.locator('#problems-only').check()
                assert page.locator('.controller-tile').count()<50
                assert page.locator('.state-watering').count()==0
                page.locator('#problems-only').uncheck()
                page.locator('#monitor-mode').click()
                assert page.locator('body').evaluate("e=>e.classList.contains('monitor-mode')")
                page.locator('#monitor-mode').click()
                fail_live=True
                page.wait_for_selector('#net-banner:not([hidden])',timeout=12000)
                assert page.locator('#dashboard-tiles button:not(:disabled)').count()==0
                fail_live=False
                page.wait_for_selector('#net-banner[hidden]',state='attached',timeout=12000)
                assert page.locator('#dashboard-tiles button:not(:disabled)').count()>0
                for width in (390,768,1366,1920):
                    page.set_viewport_size({'width':width,'height':900})
                    assert page.evaluate('document.documentElement.scrollWidth<=innerWidth'),width
                client=h.login_as('viewer')
                page.goto('http://testserver/')
                assert page.locator('#dashboard-tiles button').count()==0
                page.goto('http://testserver/settings')
                assert page.locator('.settings-form button[type=submit]').count()==0
                assert not errors,errors
                browser.close()
            print('Weather/monitor Chromium: 24h/7d charts, risks, settings save/location cache, 50 controllers, priority/filter/fullscreen, offline recovery, viewer, 4 viewport widths OK')
        finally:
            weather_outlook.meteo._http_get=old_fetch
            h.close()


if __name__=='__main__':main()
