"""Optional UI smoke: install playwright and chromium before running. No real server required."""
from playwright.sync_api import sync_playwright
import json,re,sys
from pathlib import Path
from jinja2 import Environment,FileSystemLoader
root=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parents[2]
env=Environment(loader=FileSystemLoader(root/'server/app/templates'),autoescape=True)
env.globals['app_version']='test'
html=env.get_template('heatmap.html').render(user={'username':'audit','role':'admin'},request=None,nav='heatmap')
with sync_playwright() as p:
 browser=p.chromium.launch(headless=True,args=['--no-sandbox'])
 page=browser.new_page()
 errors=[]
 page.on('pageerror',lambda e:errors.append(str(e)))
 def respond(route):
  url=route.request.url
  if '/api/v1/heatmap?' in url:
   from urllib.parse import urlparse,parse_qs
   day=parse_qs(urlparse(url).query)['date'][0]
   route.fulfill(json={'date':day,'interval_min':15,'columns':['00:00'],'rows':[{'controller_id':1,'controller_name':'test','connection_status':'online','cells':['idle'],'zones':[{'zone_number':1,'name':'zone','cells':['idle'],'run_info':{}}]}]})
  elif url.endswith('/heatmap'):
   route.fulfill(content_type='text/html',body=html)
  else: route.fulfill(body='')
 page.route('**/*',respond)
 page.goto('http://testserver/heatmap')
 page.wait_for_selector('table.heatmap')
 assert page.locator('table.heatmap').count()==1
 page.select_option('#hm-period','7')
 page.wait_for_function("document.querySelectorAll('table.heatmap').length===7")
 page.locator('tr.ctrl-row .rowname').first.click()
 assert page.locator('tr.ctrl-row').first.get_attribute('class').find('expanded')>=0
 assert not errors,errors
 print('Browser: day/week=7 tables; expand works; no JavaScript errors')
 report_html=env.get_template('reports.html').render(user={'username':'audit','role':'admin'},request=None,nav='reports')
 def report_response(route):
  if '/api/v1/reports/' in route.request.url:
   route.fulfill(json={'report':'missed','data':[{'run_ref':1,'run_id':'missed','controller_name':'test','program_id':1,'status':'skipped','reason':'weather_skip','planned_start_ts':1,'zones_json':'[1]'}]})
  elif route.request.url.endswith('/reports'):
   route.fulfill(content_type='text/html',body=report_html)
  else: route.fulfill(body='')
 page.unroute('**/*')
 page.route('**/*',report_response)
 page.goto('http://testserver/reports')
 page.click('[data-tab="missed"]')
 page.wait_for_function("document.body.textContent.includes('weather_skip')")
 assert not errors,errors
 print('Browser: missed report tab renders reason; no JavaScript errors')
 browser.close()
