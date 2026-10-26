"""Prepare an independent runtime root WITHOUT deleting checkout data.

Usage (stop the application first to keep schedules/files consistent):
 python -m server.tools.prepare_runtime --destination /srv/poliv-runtime
Then set POLIV_DATA_ROOT to the printed destination BEFORE updating checkout.
The command refuses an existing destination and never rewrites Git history.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import sqlite3


def prepare(source, destination, db_path=None):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if destination.exists():
        raise ValueError('Destination must not exist')
    if destination == source or source in destination.parents:
        raise ValueError('Runtime destination must be outside checkout')
    db_path = Path(db_path or source / 'data/poliv.db').resolve()
    if not db_path.is_file():
        raise ValueError('Source database not found')
    destination.mkdir(parents=True)
    # On failure retain this incomplete copy for diagnosis; never delete source.
    for name in ('data', 'logs', 'backups', 'archive'):
        path = source / name
        if path.exists():
            shutil.copytree(path, destination / name, ignore=shutil.ignore_patterns(
                '*.db', '*.db-wal', '*.db-shm'))
    target_db = destination / 'data/poliv.db'
    target_db.parent.mkdir(exist_ok=True)
    with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True) as src, sqlite3.connect(target_db) as dst:
        src.backup(dst)
        if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise RuntimeError('Copied database integrity check failed')
        # Do not change legacy archive paths: original files must remain available
        # until operator verifies/customizes absolute paths and performs restore.
    manifest = {'source': str(source), 'destination': str(destination),
                'database': str(target_db), 'source_preserved': True,
                'notes': 'Custom absolute paths/config and archive files outside checkout require separate backup.'}
    (destination / 'runtime-copy.json').write_text(json.dumps(manifest, indent=2))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--database', help='Source SQLite file; default data/poliv.db')
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[2]
    manifest = prepare(source, args.destination, args.database)
    print('Copy prepared:', manifest['destination'])
    print('Set POLIV_DATA_ROOT before updating the checkout; verify custom config paths.')


if __name__ == '__main__':
    main()
