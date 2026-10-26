"""Deterministic start-time optimizer, separate from schedule compilation (ADR-13).

Only moves whole runs inside their original local date/window. Never changes
water/soak durations, group membership, weather factors or calendar frequency.
"""
import copy
import math

SLOT_SECONDS=900


def segments(run,start=None):
    cursor=(run['start_minute_local'] if start is None else start)*60
    for step in run['steps']:
        end=cursor+int(step['duration_sec'])
        if step['phase']=='water':yield cursor,end,float(step.get('estimated_flow_lpm',0))
        cursor=end


def contribution(run,start):
    """Average L/min for each 15-min slot, conserving the estimated volume."""
    result={}
    for lo,hi,flow in segments(run,start):
        for slot in range(lo//900,(hi-1)//900+1):
            overlap=max(0,min(hi,(slot+1)*900)-max(lo,slot*900))
            result[slot]=result.get(slot,0)+flow*overlap/900
    return result


def metrics(runs):
    loads={}
    for run in runs:
        for slot,value in contribution(run,run['start_minute_local']).items():
            key=(run['date'],slot);loads[key]=loads.get(key,0)+value
    peak=max(loads.values(),default=0)
    return {'peak_lpm':round(peak,4),'squared_load':round(sum(v*v for v in loads.values()),4),
            'estimated_liters':round(sum(loads.values())*15,4)}


def optimize(runs,*,target,window_start=0,window_end=1440,allowed=None):
    before=copy.deepcopy(runs)
    loads={};occupied={};after=[]
    def volume(r):return sum((hi-lo)*flow for lo,hi,flow in segments(r))
    for run in sorted(before,key=lambda r:(-volume(r),r['controller_id'],r['date'],r['run_id'])):
        duration=sum(s['duration_sec'] for s in run['steps'])
        last=min(window_end-math.ceil(duration/60),1439)
        options=list(range(window_start,last+1,15))
        original=run['start_minute_local']
        if window_start<=original<=last:options.append(original)
        best=None
        for start in sorted(set(options)):
            lo,hi=start*60,start*60+duration
            key=(run['controller_id'],run['date'])
            if any(a<hi and lo<b for a,b in occupied.get(key,[])):continue
            if allowed and not allowed(run,start):continue
            part=contribution(run,start)
            peak=max([*loads.values(),*(loads.get((run['date'],s),0)+v for s,v in part.items())],default=0)
            delta=sum(2*loads.get((run['date'],s),0)*v+v*v for s,v in part.items())
            score=(peak,delta,abs(start-original),start) if target=='reduce_peaks' else (delta,peak,abs(start-original),start)
            if best is None or score<best[0]:best=(score,start,part)
        if best is None:raise ValueError('В заданном окне нельзя разместить все запуски без конфликтов и блокировок')
        _,start,part=best
        shifted=copy.deepcopy(run);shifted['start_minute_local']=start;after.append(shifted)
        occupied.setdefault((run['controller_id'],run['date']),[]).append((start*60,start*60+duration))
        for s,v in part.items():loads[(run['date'],s)]=loads.get((run['date'],s),0)+v
    order=lambda r:(r['controller_id'],r['date'],r['program_id'],r['run_id'])
    before.sort(key=order);after.sort(key=order)
    a,b=metrics(before),metrics(after)
    # Never replace a feasible original by a worse aggregate plan.
    key='peak_lpm' if target=='reduce_peaks' else 'squared_load'
    if b[key]>a[key]+1e-6:after=copy.deepcopy(before);b=a
    return {'before':before,'after':after,'metrics_before':a,'metrics_after':b,
            'changes':[{'controller_id':x['controller_id'],'program_id':x['program_id'],
                        'date':x['date'],'before_minute':x['start_minute_local'],
                        'after_minute':y['start_minute_local']}
                       for x,y in zip(before,after) if x['start_minute_local']!=y['start_minute_local']]}
