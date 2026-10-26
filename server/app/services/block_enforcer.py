"""Online enforcement of zone locks, including future start boundaries.

This is best effort: an offline controller cannot be stopped over the network.
The API/UI must expose undelivered schedules; local firmware safety is separate.
"""
import json
import logging
import threading
import time

from ..infra.connection_scope import ScopedConnection,database_scope
from ..core.blocks.policy import blocked

log=logging.getLogger('poliv.blocks')


class BlockEnforcer(threading.Thread):
    def __init__(self,db_path,commands,schedules):
        super().__init__(name='zone-block-enforcer',daemon=True)
        self.conn=ScopedConnection(db_path)
        self.commands,self.schedules=commands,schedules
        self.stopping=threading.Event();self.last_attempt={};self.expired=set()

    def tick(self):
        now=int(time.time())
        with database_scope():
            for ctrl in self.conn.execute("SELECT * FROM controllers WHERE deleted_at IS NULL AND connection_status='online'"):
                try:active=json.loads(ctrl['active_zones_json'] or '[]')
                except (ValueError,TypeError):continue
                manual=ctrl['current_mode'] in ('manual','service')
                denied=False
                for number in active:
                    zone=self.conn.execute('SELECT id FROM zones WHERE controller_id=? AND zone_number=?',(ctrl['id'],number)).fetchone()
                    if zone and blocked(self.conn,zone['id'],now,now+1,manual=manual):denied=True
                if denied and time.monotonic()-self.last_attempt.get(ctrl['id'],-10)>=5:
                    try:
                        self.commands.send_command_async(ctrl['id'],'stop_all',{'reason':'zone_block'},source='server_emergency')
                        self.last_attempt[ctrl['id']]=time.monotonic()
                    except Exception:log.exception('Zone-block stop could not be published for %s',ctrl['box_id'])
            # Once an end boundary passes, rebuild the current horizon to recover
            # watering skipped by the earlier version. History is not deleted.
            for row in self.conn.execute('''SELECT l.id,z.controller_id FROM zone_locks l
                JOIN zones z ON z.id=l.zone_id WHERE l.active=1 AND l.end_ts<=?''',(now,)):
                if row['id'] in self.expired:continue
                try:
                    self.schedules.compile_and_send(row['controller_id'],source='manual')
                    self.expired.add(row['id'])
                except Exception:log.exception('Zone-block expiry recompile failed')

    def run(self):
        try:
            while not self.stopping.is_set():
                try:self.tick()
                except Exception:log.exception('Zone-block enforcement tick failed')
                self.stopping.wait(1)
        finally:self.conn.close()

    def stop(self):
        self.stopping.set()
        self.join(timeout=5)
