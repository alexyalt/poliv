"""Generate a local MQTT broker config/ACL, without passwords or overwrites."""
import argparse
from pathlib import Path
import re


def acl_for(boxes):
    ids = sorted(set(boxes))
    if not ids or any(not re.fullmatch(r'[A-Z0-9][A-Z0-9_-]{0,63}', x) for x in ids):
        raise ValueError('box_id: use uppercase letters, digits, underscore or hyphen')
    lines = ['user poliv_server', 'topic read poliv/+/hello',
             'topic read poliv/+/status', 'topic read poliv/+/lwt',
             'topic read poliv/+/command_ack', 'topic read poliv/+/schedule_ack',
             'topic read poliv/+/event', 'topic read poliv/+/flow',
             'topic read poliv/+/schedule_request', 'topic write poliv/+/command',
             'topic write poliv/+/schedule', 'topic write poliv/+/time',
             'topic write poliv/server/heartbeat']
    for box in ids:
        lines += ['', f'user box_{box}']
        lines += [f'topic read poliv/{box}/{t}' for t in ('command', 'schedule', 'time')]
        lines += [f'topic write poliv/{box}/{t}' for t in
                  ('hello', 'status', 'lwt', 'command_ack', 'schedule_ack', 'event', 'flow', 'schedule_request')]
    return '\n'.join(lines) + '\n'


def generate(destination, boxes, tls=False):
    acl = acl_for(boxes)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    (destination / 'poliv.acl').write_text(acl, encoding='utf-8')
    (destination / 'poliv.conf').write_text(
        ('listener 8883 0.0.0.0\ncertfile /etc/mosquitto/certs/broker.crt\nkeyfile /etc/mosquitto/certs/broker.key\n' if tls else 'listener 1883 0.0.0.0\n') +
        'allow_anonymous false\n'
        'password_file /etc/mosquitto/poliv.passwd\n'
        'acl_file /etc/mosquitto/poliv.acl\n'
        'persistence true\npersistence_location /var/lib/mosquitto/\n'
        'max_packet_size 17408\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--tls', action='store_true', help='Generate a TLS-only listener on 8883')
    parser.add_argument('--box', action='append', required=True)
    args = parser.parse_args()
    generate(args.destination, args.box, tls=args.tls)
    print('Created broker config and ACL. Create passwords with mosquitto_passwd; see deployment guide.')


if __name__ == '__main__':
    main()
