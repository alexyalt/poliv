"""Archive v2 durability helpers. Primary CSV/JSON remains human-readable.

A checksummed JSON manifest preserves CSV nulls and event-zone relationships.
The DB journal is authoritative: unregistered files are harmless orphan copies.
"""
from contextlib import contextmanager
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import uuid

from ..infra.db import connect_path

TABLES = {"events": "watering_events", "flow": "flow_daily",
          "notifications": "notifications", "weather": "weather_observations",
          "logs": "logs"}


@contextmanager
def transaction(source):
    path = source.execute("PRAGMA database_list").fetchone()[2]
    conn = connect_path(path) if path else source
    if conn.in_transaction:
        raise RuntimeError("Archive requires an independent committed connection")
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        if conn is not source:
            conn.close()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def durable_write(path, data):
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with tmp.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        # Flush directory entries before the DB is allowed to delete source rows.
        if os.name == "posix":
            # Include newly created parent directories in the durability boundary.
            for directory in (path.parent, *path.parent.parents):
                fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
    finally:
        tmp.unlink(missing_ok=True)


def read_records(conn, export):
    path = Path(export["file_path"])
    data = path.read_bytes()
    version = export["format_version"]
    if version not in (1, 2):
        raise ValueError("Unsupported archive version")
    manifest = None
    if version == 2:
        if digest(data) != export["file_hash"]:
            raise ValueError("Archive checksum mismatch")
        meta = path.with_suffix(path.suffix + ".manifest.json").read_bytes()
        if digest(meta) != export["manifest_hash"]:
            raise ValueError("Archive manifest checksum mismatch")
        manifest = json.loads(meta)
        if (manifest["schema_version"] != 2 or
                manifest["entity_type"] != export["entity_type"] or
                manifest["file_hash"] != export["file_hash"]):
            raise ValueError("Archive manifest identity mismatch")
    # Legacy hashes referred to appended fragments, so cannot validate the file.
    records = (json.loads(data) if path.suffix == ".json" else
               list(csv.DictReader(io.StringIO(data.decode("utf-8")))))
    if not isinstance(records, list) or any(not isinstance(r, dict) for r in records):
        raise ValueError("Expected archive records")
    if manifest and len(records) != export["row_count"]:
        raise ValueError("Archive row count mismatch")
    table = TABLES[export["entity_type"]]
    columns = {r[1]: r[2] for r in conn.execute(f'PRAGMA table_info("{table}")')}
    for i, record in enumerate(records):
        if not record or not set(record) <= columns.keys():
            raise ValueError("Unknown archive columns")
        nulls = set(manifest["nulls"][i]) if manifest else set()
        for key, value in record.items():
            kind = columns[key].upper()
            if key in nulls or (manifest is None and value == "" and
                                ("INT" in kind or "REAL" in kind)):
                record[key] = None
            elif value is not None and "INT" in kind:
                record[key] = int(value)
            elif value is not None and "REAL" in kind:
                record[key] = float(value)
                if not math.isfinite(record[key]):
                    raise ValueError("Non-finite archive number")
    return records, manifest


def insert_record(conn, table, record):
    allowed = {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}
    if not record or not set(record) <= allowed:
        raise ValueError("Unknown archive columns")
    cols = list(record)
    quoted = ",".join('"' + c + '"' for c in cols)
    conn.execute(f'INSERT INTO "{table}" ({quoted}) VALUES ({",".join("?" for _ in cols)})',
                 tuple(record[c] for c in cols))


def restore_records(conn, entity, records, manifest):
    table = TABLES[entity]
    restored = 0
    for rec in records:
        existing = conn.execute(f'SELECT * FROM "{table}" WHERE id=?',
                                (rec["id"],)).fetchone()
        if existing:
            if any(existing[k] != v for k, v in rec.items()):
                # Silent OR IGNORE would hide an ID collision and lose history.
                raise ValueError("Archive ID conflicts with different live data")
        else:
            insert_record(conn, table, rec)
            restored += 1
    if entity == "events" and manifest:
        event_ids = {r["id"] for r in records}
        for child in manifest["event_zones"]:
            if child["event_id"] not in event_ids:
                raise ValueError("Orphan event-zone archive record")
            existing = conn.execute(
                "SELECT * FROM watering_event_zones WHERE event_id=? AND zone_id=?",
                (child["event_id"], child["zone_id"])).fetchone()
            if existing:
                if any(existing[k] != v for k, v in child.items()):
                    raise ValueError("Conflicting event-zone archive record")
            else:
                insert_record(conn, "watering_event_zones", child)
    return restored
