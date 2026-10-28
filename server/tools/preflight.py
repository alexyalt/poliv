"""Read-only local preflight. No database migrations, no watering commands, no secrets in output."""
import argparse
import json
import os
from pathlib import Path
import platform
import sqlite3
import ssl
import threading


def inspect_runtime(root):
    root=Path(root).resolve()
    import tomllib
    config=tomllib.loads((root/'config.local.toml').read_text(encoding='utf-8-sig'))
    results=[]
    def check(name,ok,detail=''):
        results.append({'check':name,'ok':bool(ok),'detail':detail})
    check('Python 3.13',platform.python_version_tuple()[:2]==('3','13'),platform.python_version())
    check('Session secret',len(config.get('server',{}).get('secret_key',''))>=32)
    check('Writable runtime',os.access(root,os.W_OK))
    db=root/'data/poliv.db'
    if db.exists():
        try:
            with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
                check('SQLite integrity',conn.execute('PRAGMA integrity_check').fetchone()[0]=='ok')
                check('Foreign keys',not conn.execute('PRAGMA foreign_key_check').fetchall())
                from server.app.infra.db import MIGRATIONS_DIR
                applied={r[0] for r in conn.execute('SELECT version FROM schema_migrations')}
                check('Migrations',not ({p.stem for p in MIGRATIONS_DIR.glob('0*.py')}-applied))
                check('Controller registered',conn.execute('SELECT count(*) FROM controllers WHERE deleted_at IS NULL').fetchone()[0]>0)
        except sqlite3.Error:
            check('Database readable',False,'Database unavailable or not initialized')
    else:check('Database initialized',False,'Start server once, then create controller and zones')
    return config,results


def mqtt_check(config,root):
    import paho.mqtt.client as paho
    from server.app.infra.mqtt_tls import configure_tls
    c=paho.Client(paho.CallbackAPIVersion.VERSION2,client_id='poliv-readonly-doctor')
    cfg=config.get('mqtt',{})
    connected=threading.Event();result=[]
    def on_connect(_c,_u,_f,reason,_p):result.append(not reason.is_failure);connected.set()
    c.connect_timeout=5
    c.on_connect=on_connect
    try:
        configure_tls(c,cfg,root)
        c.username_pw_set(cfg.get('username',''),cfg.get('password',''))
        c.connect(cfg.get('host','127.0.0.1'),cfg.get('port',1883),keepalive=10)
        c.loop_start()
        return connected.wait(8) and bool(result and result[0])
    except (OSError,ValueError,ssl.SSLError):return False
    finally:
        c.disconnect();c.loop_stop()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime',type=Path,required=True)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    config,checks=inspect_runtime(args.runtime)
    checks.append({'check':'MQTT authentication/TLS','ok':mqtt_check(config,args.runtime),'detail':'No publish or watering command sent'})
    report={'os':platform.system(),'checks':checks,'ready':all(x['ok'] for x in checks)}
    for row in checks:print(('PASS' if row['ok'] else 'FAIL')+' '+row['check']+(' — '+row['detail'] if row['detail'] else ''))
    if args.output:
        with args.output.open('x',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2)
    raise SystemExit(0 if report['ready'] else 1)


if __name__=='__main__':main()
