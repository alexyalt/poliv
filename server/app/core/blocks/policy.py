"""Stage-9 interval policy. Half-open UTC intervals; legacy rows stay readable."""
from datetime import datetime, timezone


def epoch(value, fallback):
    if value is None:
        return fallback
    if isinstance(value, (int,float)):
        return int(value)
    parsed=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    return int(parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp())


def blocked(conn, zone_id, start, end, *, manual=False):
    for row in conn.execute('SELECT * FROM zone_locks WHERE zone_id=? AND active=1',(zone_id,)):
        r=dict(row)
        if manual and not (r.get('block_manual') or r.get('block_type')=='service'):
            continue
        lo=epoch(r.get('start_ts') if r.get('start_ts') is not None else r.get('locked_at'),0)
        hi=epoch(r.get('end_ts') if r.get('end_ts') is not None else r.get('unlock_at'),253402300799)
        if lo < end and start < hi:
            return True
    return False


def filter_candidates(conn,candidates,controller_id,tz_offset):
    """Skip a whole zone in a run if any water interval overlaps its lock.

    Retain surviving step durations: removing earlier steps must not move other
    watering into a blocked interval. Empty removed steps become relay-off soak.
    """
    import copy
    from datetime import timedelta
    zones={r['zone_number']:r['id'] for r in conn.execute(
        'SELECT id,zone_number FROM zones WHERE controller_id=?',(controller_id,))}
    kept,skipped=[],[]
    for candidate in candidates:
        c=copy.deepcopy(candidate)
        start=int(datetime.combine(c['date'],datetime.min.time(),
                  timezone(timedelta(minutes=tz_offset))).timestamp())+c['start_minute']*60
        cursor=start
        excluded=set()
        for step in c['steps']:
            end=cursor+step['duration_sec']
            if step['phase']=='water':
                for number in step['active_zones']:
                    if number in zones and blocked(conn,zones[number],cursor,end):excluded.add(number)
            cursor=end
        for number in sorted(excluded):
            skipped.append({'program_id':c['program_id'],'date':c['date'].isoformat(),
                            'zone_number':number,'reason_code':'zone_locked'})
        for step in c['steps']:
            step['active_zones']=[z for z in step['active_zones'] if z not in excluded]
            if 'display_zones' in step:step['display_zones']=[z for z in step['display_zones'] if z not in excluded]
            if step['phase']=='water' and not step['active_zones']:
                step['phase']='soak';step['display_zones']=[];step.pop('expected_flow_lpm',None)
            elif step['phase']=='water':
                values=[conn.execute('SELECT expected_flow_lpm FROM zones WHERE id=?',(zones[z],)).fetchone()[0] for z in step['active_zones']]
                step['expected_flow_lpm']=sum(values) if all(v is not None for v in values) else None
        if any(s['phase']=='water' for s in c['steps']):kept.append(c)
    return kept,skipped
