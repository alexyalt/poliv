"""Prepare an independent runtime root WITHOUT deleting checkout data.

Usage (stop the application first to keep schedules/files consistent):
 python -m server.tools.prepare_runtime --destination /srv/poliv-runtime
Then set POLIV_DATA_ROOT to the printed destination BEFORE updating checkout.
The command refuses an existing destination and never rewrites Git history.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3


def _relocate_paths(conn, source, destination):
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    relocated, external = [], []
    for table, column in (("controller_schedules", "payload_path"),
                          ("archive_exports", "file_path")):
        if table not in tables:
            continue
        for identity, stored in conn.execute(f'SELECT id,{column} FROM {table}').fetchall():
            if not stored:
                continue
            original = Path(stored)
            if not original.is_absolute():
                original = source / original
            original = original.resolve()
            try:
                relative = original.relative_to(source)
            except ValueError:
                external.append(str(original))
                continue
            copied = destination / relative
            if not copied.is_file():
                raise ValueError(f'Referenced file was not copied: {relative}')
            if table == 'archive_exports':
                columns = {r[1] for r in conn.execute('PRAGMA table_info(archive_exports)')}
                if 'format_version' in columns:
                    version, file_hash, manifest_hash = conn.execute(
                        'SELECT format_version,file_hash,manifest_hash FROM archive_exports WHERE id=?',
                        (identity,)).fetchone()
                    if version == 2:
                        meta = copied.with_suffix(copied.suffix + '.manifest.json')
                        if (hashlib.sha256(copied.read_bytes()).hexdigest() != file_hash or
                                not meta.is_file() or
                                hashlib.sha256(meta.read_bytes()).hexdigest() != manifest_hash):
                            raise ValueError(f'Copied archive integrity check failed: {relative}')
            conn.execute(f'UPDATE {table} SET {column}=? WHERE id=?',
                         (str(copied), identity))
            relocated.append(str(relative))
    return relocated, sorted(set(external))


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
    marker = destination / 'runtime-copy.json'
    marker.write_text(json.dumps({'ready': False, 'source': str(source)}))
    # On failure retain this incomplete copy for diagnosis; never delete source.
    for name in ('data', 'logs', 'backups', 'archive'):
        path = source / name
        if path.exists():
            shutil.copytree(path, destination / name, ignore=shutil.ignore_patterns(
                '*.db', '*.db-wal', '*.db-shm'))
    target_db = destination / 'data/poliv.db'
    target_db.parent.mkdir(exist_ok=True)
    with closing(sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True)) as src, \
            closing(sqlite3.connect(target_db)) as dst:
        src.backup(dst)
        if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise RuntimeError('Copied database integrity check failed')
        if dst.execute('PRAGMA foreign_key_check').fetchone() is not None:
            raise RuntimeError('Copied database has foreign key violations')
        with dst:
            relocated, external = _relocate_paths(dst, source, destination)
    manifest = {'ready': True, 'relocated_files': relocated, 'external_files': external, 'source': str(source), 'destination': str(destination),
                'database': str(target_db), 'source_preserved': True,
                'notes': 'Custom absolute paths/config and archive files outside checkout require separate backup.'}
    marker.write_text(json.dumps(manifest, indent=2))
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
