"""Durable per-consumer MQTT deduplication survives event archive rotation."""

def upgrade(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS processed_events (
        controller_id INTEGER NOT NULL,
        event_uid TEXT NOT NULL,
        consumer TEXT NOT NULL CHECK(consumer IN ('event','flow','schedule')),
        processed_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY(controller_id,event_uid,consumer)
    )''')
    for consumer in ('event', 'flow', 'schedule'):
        conn.execute('''INSERT OR IGNORE INTO processed_events(controller_id,event_uid,consumer)
            SELECT controller_id,event_uid,? FROM watering_events''', (consumer,))
    conn.execute('CREATE INDEX IF NOT EXISTS idx_events_controller_run ON watering_events(controller_id,run_id,status)')


def downgrade(conn):
    # Retain the ledger: dropping it would allow archived events to be replayed.
    pass
