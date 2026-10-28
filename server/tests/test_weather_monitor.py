import json
import math
from datetime import datetime, timezone
import pytest
from server.tests.test_stage5 import _Harness5
from server.app.services import weather_outlook as outlook
from server.app.services.catalog_service import ValidationError


def fixture(now=None):
    now = now or datetime.now(timezone.utc)
    start = int(now.timestamp()) // 3600 * 3600
    hourly = {'time': [start+i*3600 for i in range(192)]}
    for key in outlook.HOURLY:
        hourly[key] = [0.2 if key=='soil_moisture_0_to_1cm' else 2 for _ in range(192)]
    hourly['temperature_2m'][0] = -1
    hourly['temperature_2m'][1] = 35
    hourly['wind_gusts_10m'][2] = 15
    daily = {'time': [start - start % 86400 + i*86400 for i in range(8)]}
    for key in outlook.DAILY:
        daily[key] = [4 for _ in range(8)]
    return {'hourly':hourly, 'daily':daily, 'timezone':'Europe/Simferopol', 'utc_offset_seconds':10800}


@pytest.fixture
def h():
    harness=_Harness5()
    yield harness
    harness.close()


def test_defaults_preserve_custom_coordinates_and_validate(h):
    cfg=outlook.weather_config(h.conn)
    assert cfg['latitude']==44.423062 and cfg['longitude']==34.06452
    for key, value in [('weather.latitude',91),('weather.longitude',float('nan')),
                       ('dashboard.columns',1),('dashboard.refresh_sec',3.5),
                       ('weather.wind_warning_ms',True)]:
        with pytest.raises(ValidationError):h.catalog.set_setting(key,value)
    h.catalog.set_setting('weather.latitude',45)
    from server.app.infra.db import migrate
    migrate(h.conn)
    assert outlook.weather_config(h.conn)['latitude']==45


def test_outlook_fresh_stale_location_and_risks(h,monkeypatch):
    now=datetime.now(timezone.utc)
    request=[]
    def fetch(url,params):request.append(params);return fixture(now)
    monkeypatch.setattr(outlook.meteo,'_http_get',fetch)
    assert outlook.update(h.conn)
    data=outlook.view(h.conn,now)
    assert data['status']=='fresh' and len(data['hourly'])==24 and len(data['daily'])==7
    assert len(data['advisories'])>=4
    assert request[0]['wind_speed_unit']=='ms' and request[0]['timeformat']=='unixtime'
    assert request[0]['latitude']==44.423062
    h.conn.execute("UPDATE weather_outlooks SET fetched_at='2020-01-01T00:00:00+00:00'")
    assert outlook.view(h.conn,now)['status']=='stale'
    h.catalog.set_setting('weather.latitude',40)
    assert outlook.view(h.conn)['status']=='unavailable'


def test_null_short_and_failure_preserve_snapshot(h,monkeypatch):
    raw=fixture();raw['hourly']['temperature_2m']=[None,float('nan')]
    data=outlook.normalize(raw)
    assert data['hourly'][0]['temperature_2m'] is None
    assert data['hourly'][1]['temperature_2m'] is None
    assert data['hourly'][2]['temperature_2m'] is None
    assert outlook.normalize({'hourly':{}}) is None
    monkeypatch.setattr(outlook.meteo,'_http_get',lambda *args:raw)
    assert outlook.update(h.conn)
    old=h.conn.execute('SELECT payload_json FROM weather_outlooks').fetchone()[0]
    monkeypatch.setattr(outlook.meteo,'_http_get',lambda *args:None)
    assert not outlook.update(h.conn)
    assert h.conn.execute('SELECT payload_json FROM weather_outlooks').fetchone()[0]==old
    h.catalog.set_setting('weather.enabled',False)
    assert outlook.view(h.conn)['status']=='disabled'


def test_outlook_route_auth_and_page(h,monkeypatch):
    from server.app.api.routes_weather import register_api_routes_weather
    register_api_routes_weather(h.app,h.cfg,h.conn)
    assert h.client.get('/api/v1/weather/outlook').status_code==401
    viewer=h.login_as('viewer')
    assert viewer.get('/api/v1/weather/outlook').json()['status']=='unavailable'
    r=viewer.get('/weather')
    assert r.status_code==200 and 'weather-outlook' in r.text
    assert '24 часа' in r.text and '7 дней' in r.text


def test_settings_form_prevalidates_all_fields_and_viewer_cannot_save(h):
    admin=h.login_as('admin')
    old=outlook.weather_config(h.conn)['latitude']
    response=admin.post('/settings/save',data={'setting:weather.latitude':'40','setting:weather.longitude':'999'})
    assert 'error=' in response.headers.get('location','') or 'диапазон' in response.text
    assert outlook.weather_config(h.conn)['latitude']==old
    viewer=h.login_as('viewer')
    viewer.post('/settings/save',data={'setting:weather.latitude':'40'})
    assert outlook.weather_config(h.conn)['latitude']==old


def test_upgrade_only_replaces_unset_coordinates(h):
    from server.app.infra.db import _load_module,MIGRATIONS_DIR
    module=_load_module(MIGRATIONS_DIR/'0014_weather_monitor.py')
    h.conn.execute("UPDATE settings SET value_json='null' WHERE key='weather.latitude'")
    h.conn.execute("UPDATE settings SET value_json='41' WHERE key='weather.longitude'")
    module.upgrade(h.conn)
    assert outlook.weather_config(h.conn)['latitude']==44.423062
    assert outlook.weather_config(h.conn)['longitude']==41
