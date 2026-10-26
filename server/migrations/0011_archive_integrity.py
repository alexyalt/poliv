"""Archive v2: immutable batches and a checksum for the typed manifest.

Existing exports remain legacy (format_version=1); their paths are unchanged.
No data files are moved or removed by this migration.
"""
import sqlite3


def upgrade(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute('PRAGMA table_info(archive_exports)')}
    if 'format_version' not in columns:
        conn.execute('ALTER TABLE archive_exports ADD COLUMN format_version INTEGER NOT NULL DEFAULT 1')
    if 'manifest_hash' not in columns:
        conn.execute('ALTER TABLE archive_exports ADD COLUMN manifest_hash TEXT')


def downgrade(conn: sqlite3.Connection) -> None:
    # Additive metadata is intentionally retained: older readers ignore columns.
    # Do not drop the journal or archive files on rollback.
    pass
