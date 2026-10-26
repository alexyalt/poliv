"""Stage 9: additive lock intervals and persisted normalization proposals."""
import json


def upgrade(conn):
    columns = {r[1] for r in conn.execute('PRAGMA table_info(zone_locks)')}
    for name, kind in [('start_ts','INTEGER'), ('end_ts','INTEGER'),
                       ('block_manual','INTEGER NOT NULL DEFAULT 0'),
                       ('released_by','TEXT'), ('released_at','TEXT'), ('block_type',"TEXT NOT NULL DEFAULT 'auto'")]:
        if name not in columns:
            conn.execute(f'ALTER TABLE zone_locks ADD COLUMN {name} {kind}')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_zone_locks_interval ON zone_locks(zone_id,active,start_ts,end_ts)')
    conn.execute('''CREATE TABLE IF NOT EXISTS normalization_jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL,
        date_from TEXT NOT NULL, date_to TEXT NOT NULL, controller_ids_json TEXT NOT NULL,
        target TEXT NOT NULL, respect_zone_blocks INTEGER NOT NULL DEFAULT 1,
        respect_season INTEGER NOT NULL DEFAULT 1, respect_weather INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL, created_by TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT (datetime('now')),
        finished_at TEXT, error_message TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS normalization_proposals(
        id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL UNIQUE REFERENCES normalization_jobs(id),
        payload_path TEXT, payload_json TEXT NOT NULL, before_hash TEXT NOT NULL, after_hash TEXT NOT NULL,
        warnings_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'proposed',
        applied_by TEXT, applied_at TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS normalization_overrides(
        controller_id INTEGER NOT NULL REFERENCES controllers(id), program_id INTEGER NOT NULL REFERENCES programs(id),
        date TEXT NOT NULL, start_minute INTEGER NOT NULL CHECK(start_minute BETWEEN 0 AND 1439),
        job_id INTEGER NOT NULL REFERENCES normalization_jobs(id),
        PRIMARY KEY(controller_id,program_id,date))''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_normalization_status ON normalization_jobs(status,created_at)')
    defaults={'normalization.enabled':True, 'normalization.min_events':3,
              'normalization.default_mode':'day', 'normalization.target':'reduce_peaks',
              'normalization.require_confirmation':True}
    for k,v in defaults.items():
        conn.execute("INSERT OR IGNORE INTO settings(key,value_json,description) VALUES(?,?,'Stage 9')",(k,json.dumps(v)))


def downgrade(conn):
    # Keep audit, accepted proposals and locks on rollback. No destructive downgrade.
    pass
