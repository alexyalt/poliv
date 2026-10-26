"""Stage 9B proposal lifecycle: immutable snapshots, explicit confirmation, audit."""
import copy
import hashlib
import json
import math
import time
from datetime import date,datetime,timedelta,timezone

from ..core.blocks.policy import blocked
from ..core.normalization.optimizer import optimize
from ..core.schedule_compiler.compiler import compile_schedule,persist_schedule,_canonical_hash
from .archive_integrity import transaction
from .flow_service import get_setting


class NormalizationError(ValueError):pass
class StaleProposal(NormalizationError):pass


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()).hexdigest()


def config(conn):
    return {key:get_setting(conn,'normalization.'+key,default) for key,default in
            [('enabled',True),('min_events',3),('default_mode','day'),('target','reduce_peaks'),('require_confirmation',True)]}


def snapshot_hash(conn):
    # Exclude jobs/logs/sessions: normal user reads cannot invalidate a proposal.
    tables=['controllers','zones','programs','program_zones','zone_locks','settings',
            'watering_events','watering_event_zones','weather_observations',
            'weather_forecasts','normalization_overrides']
    return digest({table:[dict(r) for r in conn.execute(f'SELECT * FROM {table} ORDER BY rowid')]
                   for table in tables})


class NormalizationService:
    def __init__(self,conn,cfg,schedules=None):
        self.conn,self.cfg,self.schedules=conn,cfg,schedules

    def list(self):
        return [dict(r) for r in self.conn.execute('SELECT * FROM normalization_jobs ORDER BY id DESC LIMIT 100')]

    def get(self,identity):
        row=self.conn.execute('SELECT * FROM normalization_jobs WHERE id=?',(identity,)).fetchone()
        if row is None:raise NormalizationError('Задача не найдена')
        return dict(row)

    def proposal(self,identity):
        row=self.conn.execute('SELECT * FROM normalization_proposals WHERE job_id=?',(identity,)).fetchone()
        if row is None:raise NormalizationError('Предложение не найдено')
        return {**dict(row),'payload':json.loads(row['payload_json'])}

    def _validate(self,data):
        if not isinstance(data,dict):raise NormalizationError('Ожидается объект')
        cfg=config(self.conn)
        if not cfg['enabled']:raise NormalizationError('Нормализация отключена')
        scope=data.get('scope',cfg['default_mode']);target=data.get('target',cfg['target'])
        if scope not in ('day','week') or target not in ('reduce_peaks','smooth_consumption'):raise NormalizationError('Неверный scope/target')
        try:day=date.fromisoformat(data.get('date',date.today().isoformat()))
        except (ValueError,TypeError):raise NormalizationError('Требуется дата YYYY-MM-DD')
        if day<date.today() or day>date.today()+timedelta(days=365):raise NormalizationError('Дата должна быть в пределах следующего года')
        for key in ('respect_zone_blocks','respect_season','respect_weather'):
            if data.get(key,True) is not True:raise NormalizationError('Отключение защитных ограничений не поддерживается')
        lo=data.get('window_start_minute',0);hi=data.get('window_end_minute',1440)
        if type(lo) is not int or type(hi) is not int or not 0<=lo<hi<=1440:raise NormalizationError('Неверное окно полива')
        ids=data.get('controller_ids')
        if ids is None:ids=[r[0] for r in self.conn.execute('SELECT id FROM controllers WHERE deleted_at IS NULL AND enabled=1')]
        if not isinstance(ids,list) or not ids or len(ids)>50 or any(type(i) is not int or i<=0 for i in ids):raise NormalizationError('Укажите 1..50 controller_ids')
        return scope,target,day,sorted(set(ids)),lo,hi

    def _compiled(self,conn,ids,day,days):
        output={}
        for cid in ids:
            ctrl=conn.execute('SELECT * FROM controllers WHERE id=? AND deleted_at IS NULL',(cid,)).fetchone()
            if ctrl is None:raise NormalizationError('Контроллер не найден')
            result=compile_schedule(conn,dict(ctrl),today=day,days_ahead=max(1,days-1),source='manual')
            end=day+timedelta(days=days-1)
            result.payload['runs']=[r for r in result.payload['runs'] if r['date']<=end.isoformat()]
            result.payload['valid_to_date']=end.isoformat();result.valid_to=end
            output[cid]=result
        return output

    def create(self,data,actor):
        scope,target,day,ids,lo,hi=self._validate(data)
        days=1 if scope=='day' else 7
        with transaction(self.conn) as conn:
            cur=conn.execute('''INSERT INTO normalization_jobs(scope,date_from,date_to,controller_ids_json,target,status,created_by)
                VALUES(?,?,?,?,?,'processing',?)''',(scope,day.isoformat(),(day+timedelta(days=days-1)).isoformat(),json.dumps(ids),target,actor))
            identity=cur.lastrowid
            try:
                compiled=self._compiled(conn,ids,day,days)
                minimum=int(config(conn)['min_events'])
                all_runs=[];statistics={};zone_ids={};offsets={}
                for cid,item in compiled.items():
                    offsets[cid]=item.payload['timezone_offset_min']
                    for run in item.payload['runs']:
                        run=copy.deepcopy(run);run['controller_id']=cid
                        # Do not propose changing already started runs for today's date.
                        start=int(datetime.combine(date.fromisoformat(run['date']),datetime.min.time(),timezone(timedelta(minutes=offsets[cid]))).timestamp())+run['start_minute_local']*60
                        if start<=time.time():raise NormalizationError('Выберите будущую дату: некоторые запуски уже начались')
                        for step in run['steps']:
                            flow=0.0
                            for number in step['active_zones']:
                                z=conn.execute('SELECT id FROM zones WHERE controller_id=? AND zone_number=?',(cid,number)).fetchone()
                                if z is None:raise NormalizationError('Зона не найдена')
                                zid=z['id'];zone_ids[(cid,number)]=zid
                                if zid not in statistics:
                                    stats=conn.execute('''SELECT COUNT(*) n,SUM(e.volume_liters)*60.0/SUM(e.water_sec) lpm
                                        FROM watering_events e JOIN watering_event_zones ez ON ez.event_id=e.id
                                        WHERE ez.zone_id=? AND e.status='finished' AND COALESCE(e.aborted,0)=0
                                          AND e.water_sec>0 AND e.volume_liters>0
                                          AND e.start_ts>=? AND e.start_ts<=?
                                          AND (SELECT COUNT(*) FROM watering_event_zones x WHERE x.event_id=e.id)=1
                                          AND COALESCE(e.reason_code,'') NOT IN ('no_flow','over_flow','under_flow','emergency_stop')''',
                                          (zid,int(time.time())-90*86400,int(time.time()))).fetchone()
                                    if stats['n']<minimum or not stats['lpm'] or not math.isfinite(stats['lpm']):raise NormalizationError(f'Недостаточно корректных одиночных событий зоны {zid}: требуется {minimum}')
                                    statistics[zid]=dict(stats)
                                flow+=statistics[zid]['lpm']
                            step['estimated_flow_lpm']=flow
                        all_runs.append(run)
                if not all_runs:raise NormalizationError('Нет запусков для нормализации')
                if len(all_runs)>1000:raise NormalizationError('Слишком много запусков; уменьшите область')
                def allowed(run,start):
                    cursor=int(datetime.combine(date.fromisoformat(run['date']),datetime.min.time(),timezone(timedelta(minutes=offsets[run['controller_id']]))).timestamp())+start*60
                    for step in run['steps']:
                        end=cursor+step['duration_sec']
                        for z in step['active_zones']:
                            if blocked(conn,zone_ids[(run['controller_id'],z)],cursor,end):return False
                        cursor=end
                    return True
                proposal=optimize(all_runs,target=target,window_start=lo,window_end=hi,allowed=allowed)
                proposal.update(statistics=statistics,source_snapshot=snapshot_hash(conn),
                                window_start_minute=lo,window_end_minute=hi,
                                warnings=['Оценка по одиночным событиям; давление сети не измеряется. Новая параллельность не создаётся.',
                                          'Переносы только внутри исходного дня; суммарные water/soak длительности сохранены.'])
                conn.execute('''INSERT INTO normalization_proposals(job_id,payload_json,before_hash,after_hash,warnings_json)
                    VALUES(?,?,?,?,?)''',(identity,json.dumps(proposal,ensure_ascii=False),digest(proposal['before']),digest(proposal['after']),json.dumps(proposal['warnings'],ensure_ascii=False)))
                conn.execute("UPDATE normalization_jobs SET status='ready',finished_at=datetime('now') WHERE id=?",(identity,))
                self._audit(conn,'normalization.proposed',identity,actor,{'statistics':statistics,'target':target})
            except (NormalizationError,ValueError) as exc:
                conn.execute("UPDATE normalization_jobs SET status='error',finished_at=datetime('now'),error_message=? WHERE id=?",(str(exc),identity))
                self._audit(conn,'normalization.error',identity,actor,{'error':str(exc)})
        return self.get(identity)

    def apply(self,identity,actor,confirm=False):
        if confirm is not True:raise NormalizationError('Требуется явное confirm=true')
        if self.schedules is None:raise NormalizationError('Сервис расписаний недоступен')
        schedule_ids=[]
        with self.schedules._lock, transaction(self.conn) as conn:
            service=NormalizationService(conn,self.cfg)
            job=service.get(identity);row=service.proposal(identity);payload=row['payload']
            if job['status']=='applied':return {'applied':True,'already_applied':True}
            if job['status']!='ready' or row['status']!='proposed':raise NormalizationError('Предложение не готово к применению')
            if payload['source_snapshot']!=snapshot_hash(conn):raise StaleProposal('Данные изменились; создайте новое предложение')
            if not config(conn)['enabled']:raise NormalizationError('Нормализация отключена')
            for run in payload['after']:
                start=int(datetime.combine(date.fromisoformat(run['date']),datetime.min.time()).timestamp())+run['start_minute_local']*60
                if start<=time.time():raise StaleProposal('Время предложения истекло')
                conn.execute('''INSERT INTO normalization_overrides(controller_id,program_id,date,start_minute,job_id)
                    VALUES(?,?,?,?,?) ON CONFLICT(controller_id,program_id,date) DO UPDATE SET
                    start_minute=excluded.start_minute,job_id=excluded.job_id''',
                    (run['controller_id'],run['program_id'],run['date'],run['start_minute_local'],identity))
            day=date.fromisoformat(job['date_from']);days=1 if job['scope']=='day' else 7
            compiled=service._compiled(conn,json.loads(job['controller_ids_json']),day,days)
            # Preserve exact proposed steps (estimated_flow_lpm is audit-only).
            for cid,item in compiled.items():
                expected=[r for r in payload['after'] if r['controller_id']==cid]
                actual=item.payload['runs']
                key=lambda r:(r['program_id'],r['date'])
                if {key(r):r['start_minute_local'] for r in expected}!={key(r):r['start_minute_local'] for r in actual}:raise StaleProposal('Состав расписания изменился')
                for run in actual:
                    old=next(r for r in expected if key(r)==key(run))
                    steps=[{k:v for k,v in s.items() if k!='estimated_flow_lpm'} for s in old['steps']]
                    if steps!=run['steps']:raise StaleProposal('Длительности или ограничения изменились')
                item.payload['normalization']={'job_id':identity,'mode':job['scope'],'target':job['target']}
                item.schedule_hash=_canonical_hash(item.payload);item.payload['schedule_hash']=item.schedule_hash
                sid,_=persist_schedule(conn,item,data_dir=self.cfg.data_dir,status='compiled',created_by=actor)
                schedule_ids.append(sid)
            conn.execute("UPDATE normalization_jobs SET status='applied' WHERE id=?",(identity,))
            conn.execute("UPDATE normalization_proposals SET status='applied',applied_by=?,applied_at=datetime('now') WHERE job_id=?",(actor,identity))
            self._audit(conn,'normalization.applied',identity,actor,{'schedule_ids':schedule_ids,'before_hash':row['before_hash'],'after_hash':row['after_hash']})
        delivery=[]
        for sid in schedule_ids:
            try:delivery.append(self.schedules.send_schedule(sid))
            except Exception as exc:delivery.append({'schedule_id':sid,'sent':False,'error':str(exc)})
        return {'applied':True,'schedule_ids':schedule_ids,'delivery':delivery}

    def reject(self,identity,actor):
        with transaction(self.conn) as conn:
            job=NormalizationService(conn,self.cfg).get(identity)
            if job['status']=='rejected':return {'rejected':True}
            if job['status']!='ready':raise NormalizationError('Отклонить можно только готовое предложение')
            conn.execute("UPDATE normalization_jobs SET status='rejected' WHERE id=?",(identity,))
            conn.execute("UPDATE normalization_proposals SET status='rejected' WHERE job_id=?",(identity,))
            self._audit(conn,'normalization.rejected',identity,actor,{})
        return {'rejected':True}

    def _audit(self,conn,action,identity,actor,detail):
        conn.execute("INSERT INTO logs(ts,username,action,object_type,object_id,details_json,source) VALUES(datetime('now'),?,?,'normalization',?,?,'web')",(actor,action,str(identity),json.dumps(detail,ensure_ascii=False)))
