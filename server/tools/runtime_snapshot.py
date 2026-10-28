"""Copy a stopped Poliv runtime into a new verified backup or restore directory.
Never overwrites an existing destination. Stop the server before both operations.
Secrets in /etc/poliv and firmware/include/secrets.h need separate secure backup.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
from server.tools.prepare_runtime import prepare


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file() and
            p.name not in ('snapshot.json',) and not p.name.endswith(('-wal', '-shm'))}


def verify(source):
    source = Path(source).resolve()
    manifest = json.loads((source / 'snapshot.json').read_text())
    expected = manifest['sha256']
    if hashes(source) != expected:
        raise ValueError('Snapshot contents/hash mismatch')
    with sqlite3.connect((source / 'data/poliv.db').as_uri() + '?mode=ro', uri=True) as conn:
        if conn.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('Snapshot database is corrupt')
        if conn.execute('PRAGMA foreign_key_check').fetchone() is not None:
            raise ValueError('Snapshot foreign key check failed')
    return manifest


def snapshot(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    result = prepare(source, destination)
    if result['external_files']:
        raise ValueError('Referenced files outside runtime: copy retained for diagnosis; do not use as full backup')
    manifest = {'format': 1, 'source': str(source), 'sha256': hashes(destination)}
    (destination / 'snapshot.json').write_text(json.dumps(manifest, indent=2))
    verify(destination)
    return manifest


def restore(source, destination):
    verify(source)
    return snapshot(source, destination)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['backup', 'restore', 'verify'])
    p.add_argument('--source', required=True)
    p.add_argument('--destination')
    args = p.parse_args()
    if args.action == 'verify':
        verify(args.source)
    else:
        if not args.destination:
            p.error('--destination is required')
        (snapshot if args.action == 'backup' else restore)(args.source, args.destination)
    print('Snapshot verified. Source preserved; existing destinations are never overwritten.')


if __name__ == '__main__':
    main()
