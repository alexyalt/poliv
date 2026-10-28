"""Create an independent Windows test runtime and authenticated LAN broker config.
Run from the checkout; existing runtime/config files are never overwritten.
"""
import argparse
import getpass
import json
from pathlib import Path
import secrets
import subprocess
from server.tools.broker_config import acl_for


def create(root, host, boxes, admin_password, mqtt_password, mosquitto_dir):
    root=Path(root).resolve()
    if '\n' in str(root) or '\r' in str(root):raise ValueError('Runtime path contains a newline')
    if root.exists():raise ValueError('Runtime destination must not exist')
    if len(admin_password)<12 or len(mqtt_password)<12:
        raise ValueError('Passwords must contain at least 12 characters')
    if not host or any(c not in '0123456789.' for c in host) or host.startswith('127.') or host=='0.0.0.0':
        raise ValueError('Use the Windows PC LAN IPv4 address, e.g. 192.168.1.20')
    import ipaddress
    ipaddress.IPv4Address(host)
    acl=acl_for(boxes)
    root.mkdir(parents=True)
    for name in ('data','logs','backups','broker'):(root/name).mkdir()
    broker=root/'broker'
    quote=lambda value:json.dumps(str(value),ensure_ascii=False)
    (root/'config.local.toml').write_text(
        f'[server]\nsecret_key={quote(secrets.token_hex(32))}\ncookie_secure=false\n'
        f'[admin]\nusername="admin"\npassword={quote(admin_password)}\nmust_change_password=true\n'
        '[test_data]\nenabled=false\n'
        f'[mqtt]\nhost="127.0.0.1"\nport=1883\nusername="poliv_server"\noffline_threshold_min=1\npassword={quote(mqtt_password)}\n',encoding='utf-8')
    (broker/'poliv.acl').write_text(acl,encoding='utf-8')
    (broker/'mosquitto.conf').write_text(
        f'listener 1883 {host}\nlistener 1883 127.0.0.1\nallow_anonymous false\n'
        f'password_file {(broker/"poliv.passwd").as_posix()}\n'
        f'acl_file {(broker/"poliv.acl").as_posix()}\n'
        f'persistence true\npersistence_location {broker.as_posix()+"/"}\n'
        'log_dest stdout\nmax_packet_size 17408\n',encoding='utf-8')
    (root/'setup.json').write_text(json.dumps({'lan_host':host,'boxes':boxes,
        'mosquitto_dir':str(Path(mosquitto_dir).resolve()),'version':1},indent=2),encoding='utf-8')
    return root


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime',required=True,type=Path)
    p.add_argument('--host',required=True)
    p.add_argument('--box',action='append',required=True)
    p.add_argument('--mosquitto-dir',type=Path,default=Path('C:/Program Files/mosquitto'))
    args=p.parse_args()
    password_tool=args.mosquitto_dir/'mosquitto_passwd.exe'
    if not password_tool.is_file():p.error('Install Mosquitto for Windows first; mosquitto_passwd.exe not found')
    admin=getpass.getpass('Initial admin password (12+ characters): ')
    mqtt=getpass.getpass('Server MQTT password (12+ characters): ')
    root=create(args.runtime,args.host,args.box,admin,mqtt,args.mosquitto_dir)
    passwd=root/'broker/poliv.passwd'
    # mosquitto_passwd reads passwords directly from terminal, not subprocess arguments.
    print('Set MQTT password for poliv_server AGAIN: use exactly the server MQTT password above.')
    subprocess.run([str(password_tool),'-c',str(passwd),'poliv_server'],check=True)
    for box in sorted(set(args.box)):
        print(f'Set a separate device password for box_{box}. Keep it for USB configuration.')
        subprocess.run([str(password_tool),str(passwd),'box_'+box],check=True)
    print('Created runtime:',root)
    print('Restrict folder permissions to your Windows account; see WINDOWS_MANUAL_TEST.md.')
    print('If interrupted, keep this directory for diagnosis and retry with a NEW runtime directory.')


if __name__=='__main__':main()
