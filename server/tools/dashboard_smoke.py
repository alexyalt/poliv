"""Chromium checks against FastAPI routes and an isolated SQLite test database.
Run: python -m server.tools.dashboard_smoke (requires playwright + chromium).
"""
from pathlib import Path
import os
import tempfile
from playwright.sync_api import sync_playwright
from server.tests.test_stage5 import _Harness5


def main():
    temporary = tempfile.TemporaryDirectory(prefix='poliv-dash-')
    local = Path(temporary.name) / 'test.toml'
    local.write_text('[server]\nsecret_key="dashboard-smoke-isolated-test-key-1234567890"\n')
    os.environ['POLIV_CONFIG_LOCAL'] = str(local)
    h = _Harness5()
    try:
        client = h.login_as('operator')
        c = h.catalog.create_controller({'box_id': 'DASHBOARD', 'name': 'Теплица'})
        h.add_zone(c['id'], 1)
        h.set_live('DASHBOARD', connection_status='online', current_mode='manual',
                   current_phase='water', active_zones_json='[1]', flow_enabled=1,
                   instant_lpm=12.5, time_valid=1)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
            page = browser.new_page(viewport={'width': 1280, 'height': 850})
            errors = []
            page.on('pageerror', lambda exc: errors.append(str(exc)))
            def route(req):
                path = req.request.url.split('http://testserver', 1)[1]
                if path.startswith('/static/'):
                    f = Path(__file__).resolve().parents[1] / 'app' / path.split('?', 1)[0].lstrip('/')
                    req.fulfill(status=200, body=f.read_bytes(), content_type='text/css')
                    return
                response = client.request(req.request.method, path,
                                          content=req.request.post_data, follow_redirects=False)
                req.fulfill(status=response.status_code, body=response.content,
                            headers={k: v for k, v in response.headers.items()
                                     if k.lower() not in ('content-length', 'set-cookie')})
            page.route('http://testserver/**', route)
            page.goto('http://testserver/')
            card = page.locator('[data-box="DASHBOARD"]')
            assert '12,5 л/мин' in card.inner_text()
            assert 'state-watering' in card.get_attribute('class')
            card.get_by_role('button', name='Старт', exact=True).click()
            assert page.locator('#start-dialog').is_visible()
            assert page.locator('#start-zone').input_value() == '1'
            page.get_by_role('button', name='Отмена').click()
            page.goto('http://testserver/active-zones')
            assert page.locator('[data-box="DASHBOARD"]').count() == 1
            h.set_live('DASHBOARD', connection_status='offline')
            page.wait_for_timeout(5500)
            assert page.locator('[data-box="DASHBOARD"]').count() == 0
            page.goto('http://testserver/')
            assert page.locator('[data-box="DASHBOARD"] button:disabled').count() == 3
            page.set_viewport_size({'width': 390, 'height': 844})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            viewer = h.login_as('viewer')
            client = viewer
            page.goto('http://testserver/')
            assert page.locator('#dashboard-tiles .tile-controls button').count() == 0
            assert not errors, errors
            browser.close()
        print('Dashboard Chromium/FastAPI/SQLite: flow, start dialog, active filtering, polling, offline, viewer, mobile OK')
    finally:
        h.close()
        temporary.cleanup()


if __name__ == '__main__':
    main()
