"""Validated zone blocks and auditable release, with no physical deletion."""
import json
import math
import time
from datetime import datetime, timezone

from ..core.blocks.policy import blocked


class BlockError(ValueError):
    pass


def _integer(v,name):
    if type(v) is not int or v<0:raise BlockError(f'{name}: требуется целое неотрицательное число')
    return v


class BlockService:
    def __init__(self,conn):self.conn=conn

    def list(self,zone_id=None):
        q='SELECT l.*,z.name AS zone_name,z.controller_id FROM zone_locks l JOIN zones z ON z.id=l.zone_id'
        args=()
        if zone_id is not None:q+=' WHERE l.zone_id=?';args=(zone_id,)
        return [dict(r) for r in self.conn.execute(q+' ORDER BY l.id DESC LIMIT 1000',args)]

    def create(self,data,actor):
        zid=_integer(data.get('zone_id'),'zone_id')
        zone=self.conn.execute('SELECT * FROM zones WHERE id=? AND deleted_at IS NULL',(zid,)).fetchone()
        if zone is None:raise BlockError('Зона не найдена')
        start=_integer(data.get('start_ts',int(time.time())),'start_ts')
        end=data.get('end_ts')
        if end is not None:
            end=_integer(end,'end_ts')
            if end<=start:raise BlockError('Окончание должно быть позже начала')
            if end>253402300799:raise BlockError('Дата вне диапазона')
        if start>253402300799:raise BlockError('Дата вне диапазона')
        kind=data.get('kind','auto')
        if kind not in ('auto','all','service'):raise BlockError('Тип: auto/all/service')
        manual=data.get('block_manual',kind in ('all','service'))
        if type(manual) is not bool:raise BlockError('block_manual: требуется boolean')
        manual=manual or kind in ('all','service')
        reason=data.get('reason','')
        if not isinstance(reason,str) or not reason.strip() or len(reason)>500:raise BlockError('Укажите причину до 500 символов')
        iso=lambda ts:datetime.fromtimestamp(ts,timezone.utc).isoformat()
        with self.conn:
            cur=self.conn.execute('''INSERT INTO zone_locks(zone_id,kind,reason,locked_by,locked_at,unlock_at,
                  active,start_ts,end_ts,block_manual,block_type) VALUES(?,?,?,?,?,?,1,?,?,?,?)''',
                  (zid,'maintenance' if kind=='service' else 'other',reason.strip(),actor,iso(start),iso(end) if end is not None else None,start,end,int(manual),kind))
            identity=cur.lastrowid
            self.audit('zone.blocked',identity,actor,{'zone_id':zid,'start_ts':start,'end_ts':end,'kind':kind})
            self.conn.execute('''INSERT INTO notifications(type,severity,controller_id,zone_id,message,dedupe_key)
                VALUES('zone_block','info',?,?,?,?)''',(zone['controller_id'],zid,reason.strip(),f'zone-block:{identity}'))
        return dict(self.conn.execute('SELECT * FROM zone_locks WHERE id=?',(identity,)).fetchone())

    def quick(self,zid,data,actor):
        hours=data.get('duration_hours',1)
        if type(hours) not in (int,float) or not math.isfinite(hours) or not 0<hours<=8760:raise BlockError('duration_hours: 0..8760')
        now=int(time.time())
        return self.create({**data,'zone_id':zid,'start_ts':now,'end_ts':now+max(1,int(hours*3600))},actor)

    def release(self,identity,actor):
        row=self.conn.execute('SELECT * FROM zone_locks WHERE id=?',(identity,)).fetchone()
        if row is None:raise BlockError('Блокировка не найдена')
        with self.conn:
            self.conn.execute("UPDATE zone_locks SET active=0,released_by=?,released_at=datetime('now') WHERE id=?",(actor,identity))
            if row['active']:self.audit('zone.unblocked',identity,actor,{'zone_id':row['zone_id']})
        return dict(row)

    def audit(self,action,identity,actor,detail):
        self.conn.execute("INSERT INTO logs(ts,username,action,object_type,object_id,details_json,source) VALUES(datetime('now'),?,?,'zone_lock',?,?,'web')",(actor,action,str(identity),json.dumps(detail)))


def check_manual(conn,controller_id,command,params):
    if command!='zone_open':return
    zone_number=params.get('zone') or params.get('zone_number')
    row=conn.execute('SELECT id FROM zones WHERE controller_id=? AND zone_number=? AND deleted_at IS NULL',(controller_id,zone_number)).fetchone()
    if row is None:return  # existing validation owns missing zone behavior
    now=int(time.time())
    duration=int(params.get('duration_sec') or params.get('duration_seconds') or 60)
    if blocked(conn,row['id'],now,now+duration,manual=True):
        raise BlockError('Зона заблокирована для ручного полива')
