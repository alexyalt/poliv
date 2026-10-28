"""Persist device error and pending event count for operator diagnostics."""
def upgrade(conn):
    columns={r[1] for r in conn.execute('PRAGMA table_info(controllers)')}
    for name,kind in [('device_error_code','TEXT'),('event_buffer_size','INTEGER')]:
        if name not in columns:
            conn.execute(f'ALTER TABLE controllers ADD COLUMN {name} {kind}')
