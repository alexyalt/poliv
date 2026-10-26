"""Stage-9 REST contracts, ACL and nonblocking service execution."""
import json
import time
from functools import wraps
from pathlib import Path

from fastapi import Request
from fastapi.responses import JSONResponse,HTMLResponse,RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool
from ..services.block_service import BlockService,BlockError
from ..services.normalization_service import NormalizationService,NormalizationError,StaleProposal,config
from ..services.flow_service import set_setting
from .routes_stage3 import _user


def register_api_routes_stage9(app,cfg,conn,schedules,commands):
    blocks=BlockService(conn);normal=NormalizationService(conn,cfg,schedules)

    def route(method,path,role='operator'):
        def decorate(handler):
            @wraps(handler)
            async def wrapped(request:Request,**kwargs):
                user=_user(request)
                if user is None:return JSONResponse({'detail':'Требуется вход'},status_code=401)
                if {'viewer':0,'operator':1,'admin':2}.get(user['role'],0)<{'viewer':0,'operator':1,'admin':2}[role]:return JSONResponse({'detail':'Недостаточно прав'},status_code=403)
                body={}
                if request.method in ('POST','PUT'):
                    try:
                        body=await request.json()
                        if not isinstance(body,dict):raise ValueError()
                    except Exception:return JSONResponse({'detail':'Требуется JSON объект'},status_code=422)
                try:return await run_in_threadpool(handler,request,dict(user),body,**kwargs)
                except StaleProposal as exc:return JSONResponse({'detail':str(exc)},status_code=409)
                except (BlockError,NormalizationError,ValueError) as exc:return JSONResponse({'detail':str(exc)},status_code=422)
            # Preserve Request/path signature for FastAPI without exposing internal params.
            import inspect
            parameters=[inspect.Parameter('request',inspect.Parameter.POSITIONAL_OR_KEYWORD,annotation=Request)]
            for name in ('identity','zone_id'):
                if '{'+name+'}' in path:parameters.append(inspect.Parameter(name,inspect.Parameter.POSITIONAL_OR_KEYWORD,annotation=int))
            wrapped.__signature__=inspect.Signature(parameters)
            app.add_api_route(path,wrapped,methods=[method])
            return handler
        return decorate

    def update_controller(cid):
        try:
            result=schedules.compile_and_send(cid,source='manual')
            return result
        except Exception as exc:
            schedules.record_compile_error(cid,str(exc),'stage9')
            return {'sent':False,'error':str(exc),'controller_id':cid}

    def enforce(row):
        zone=conn.execute('SELECT controller_id,zone_number FROM zones WHERE id=?',(row['zone_id'],)).fetchone()
        cid=zone['controller_id']
        stop=None
        if row.get('start_ts') is not None and row['start_ts']<=time.time() and (row['end_ts'] is None or row['end_ts']>time.time()):
            ctrl=conn.execute('SELECT * FROM controllers WHERE id=?',(cid,)).fetchone()
            active=json.loads(ctrl['active_zones_json'] or '[]')
            if zone['zone_number'] in active:
                try:stop=commands.send_command_async(cid,'stop_all',{'reason':'zone_block'})
                except Exception as exc:stop={'error':str(exc)}
        return {'stop':stop,'schedule':update_controller(cid)}

    @route('GET','/api/v1/zone-blocks',role='viewer')
    def list_blocks(request,user,body):return {'items':blocks.list()}

    @route('GET','/api/v1/zones/{zone_id}/blocks',role='viewer')
    def zone_blocks(request,user,body,zone_id):return {'items':blocks.list(zone_id)}

    @route('POST','/api/v1/zone-blocks')
    def create_block(request,user,body):
        row=blocks.create(body,user['username'])
        return JSONResponse({'block':row,'delivery':enforce(row)},status_code=201)

    @route('POST','/api/v1/zones/{zone_id}/quick-block')
    def quick_block(request,user,body,zone_id):
        row=blocks.quick(zone_id,body,user['username'])
        return JSONResponse({'block':row,'delivery':enforce(row)},status_code=201)

    @route('DELETE','/api/v1/zone-blocks/{identity}')
    def release_block(request,user,body,identity):
        row=blocks.release(identity,user['username'])
        cid=conn.execute('SELECT controller_id FROM zones WHERE id=?',(row['zone_id'],)).fetchone()[0]
        return {'released':True,'delivery':update_controller(cid)}

    @route('POST','/api/v1/normalization/jobs')
    def create_job(request,user,body):return JSONResponse(normal.create(body,user['username']),status_code=201)

    @route('GET','/api/v1/normalization/jobs')
    def list_jobs(request,user,body):return {'items':normal.list()}

    @route('GET','/api/v1/normalization/jobs/{identity}')
    def get_job(request,user,body,identity):return normal.get(identity)

    @route('GET','/api/v1/normalization/jobs/{identity}/proposal')
    def proposal(request,user,body,identity):return normal.proposal(identity)

    @route('POST','/api/v1/normalization/jobs/{identity}/apply')
    def apply(request,user,body,identity):return normal.apply(identity,user['username'],body.get('confirm',False))

    @route('POST','/api/v1/normalization/jobs/{identity}/reject')
    def reject(request,user,body,identity):return normal.reject(identity,user['username'])

    @route('GET','/api/v1/settings/normalization')
    def settings(request,user,body):return config(conn)

    @route('PUT','/api/v1/settings/normalization')
    def set_settings(request,user,body):
        allowed={'enabled','min_events','default_mode','target','require_confirmation'}
        if set(body)-allowed:raise NormalizationError('Неизвестная настройка')
        for k,v in body.items():
            if k in ('enabled','require_confirmation') and type(v) is not bool:raise NormalizationError('Требуется boolean')
            if k=='require_confirmation' and v is not True:raise NormalizationError('Подтверждение всегда обязательно')
            if k=='min_events' and (type(v) is not int or not 3<=v<=10000):raise NormalizationError('min_events: 3..10000')
            if k=='default_mode' and v not in ('day','week'):raise NormalizationError('Неверный режим')
            if k=='target' and v not in ('reduce_peaks','smooth_consumption'):raise NormalizationError('Неверная цель')
        with conn:
            for k,v in body.items():set_setting(conn,'normalization.'+k,v)
        return config(conn)

    templates=Jinja2Templates(directory=str(Path(app.state.app_dir)/'templates'))
    templates.env.globals['app_version']=app.version
    @app.get('/stage9',response_class=HTMLResponse)
    def stage9_page(request:Request):
        user=_user(request)
        if user is None:return RedirectResponse('/login',status_code=302)
        if user['role']=='viewer':return HTMLResponse('Недостаточно прав',status_code=403)
        return templates.TemplateResponse(request,'stage9.html',{'user':dict(user),'nav':'stage9'})
