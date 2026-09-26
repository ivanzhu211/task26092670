"""Bidirectional identity sync engine. All engine state lives in sqlite3,
so an instance can be dropped and rebuilt on the same DB file between syncs
with no behaviour change.

Cached per-connector state mirrors what the engine has *read* from each
directory, plus the revision of the engine's own last write per field
(engine_rev). A field offers an assertion iff its current revision differs
from engine_rev (revisions are never reused, so this identifies engine
writes without comparing values).
"""
from __future__ import annotations

import sqlite3
import threading

from connector import CursorExpired
from mapping import ATTRS, apply_chain

SCHEMA = """
CREATE TABLE IF NOT EXISTS central(
    employee_id TEXT NOT NULL,
    attr        TEXT NOT NULL,
    value       TEXT NOT NULL,
    PRIMARY KEY(employee_id, attr));
CREATE TABLE IF NOT EXISTS fields(
    connector   TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    field       TEXT NOT NULL,
    value       TEXT NOT NULL,
    rev         INTEGER NOT NULL,
    engine_rev  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connector, employee_id, field));
CREATE TABLE IF NOT EXISTS records(
    connector   TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    PRIMARY KEY(connector, employee_id));
CREATE TABLE IF NOT EXISTS cursors(
    connector   TEXT PRIMARY KEY,
    rev         INTEGER NOT NULL);
"""

_CHUNK = 400


class SyncEngine:
    def __init__(self, db_path: str, connectors: dict, mappings: dict,
                 priorities: dict) -> None:
        self.connectors = dict(connectors)
        self.mappings = {name: dict(fm) for name, fm in mappings.items()}
        self.priorities = {a: list(priorities.get(a, ())) for a in ATTRS}
        for name in self.mappings:
            if name not in self.connectors:
                raise ValueError(f"mapping for unknown connector {name!r}")
        for attr, conns in self.priorities.items():
            for name in conns:
                if name not in self.connectors:
                    raise ValueError(f"priority list names unknown connector {name!r}")
        self._sync_lock = threading.Lock()
        self.db = sqlite3.connect(db_path)
        self.db.executescript(SCHEMA)

    def close(self) -> None:
        self.db.close()

    # ------------------------------------------------------------------ API
    def sync_full(self):
        with self._sync_lock:
            emp_ids = set()
            for name in self.connectors:
                emp_ids |= self._full_refresh_connector(name)
            emp_ids |= self._all_known_emps()
            result = self._recompute(emp_ids)
            self.db.commit()
            return result

    def sync_delta(self):
        with self._sync_lock:
            emp_ids = set()
            for name, directory in self.connectors.items():
                cursor = self._get_cursor(name)
                try:
                    changes, new_cursor = directory.list_changes(cursor)
                except CursorExpired:
                    emp_ids |= self._full_refresh_connector(name)
                    continue
                for _rev, emp, field, value in changes:
                    emp_ids.add(emp)
                    if field is None:
                        self.db.execute(
                            "DELETE FROM fields WHERE connector=? AND employee_id=?",
                            (name, emp))
                        self.db.execute(
                            "DELETE FROM records WHERE connector=? AND employee_id=?",
                            (name, emp))
                    else:
                        self.db.execute(
                            "INSERT OR IGNORE INTO records VALUES (?,?)", (name, emp))
                        self.db.execute(
                            "INSERT INTO fields VALUES (?,?,?,?,?,0) "
                            "ON CONFLICT(connector, employee_id, field) "
                            "DO UPDATE SET value=excluded.value, rev=excluded.rev",
                            (name, emp, field, value, _rev))
                self._set_cursor(name, new_cursor)
            result = self._recompute(emp_ids)
            self.db.commit()
            return result

    def get(self, employee_id: str) -> dict:
        out = {a: "" for a in ATTRS}
        for attr, value in self.db.execute(
                "SELECT attr, value FROM central WHERE employee_id=?",
                (employee_id,)):
            out[attr] = value
        return out

    # ------------------------------------------------------- cache handling
    def _get_cursor(self, name: str) -> int:
        row = self.db.execute(
            "SELECT rev FROM cursors WHERE connector=?", (name,)).fetchone()
        return row[0] if row else 0

    def _set_cursor(self, name: str, rev: int) -> None:
        self.db.execute(
            "INSERT INTO cursors VALUES (?,?) "
            "ON CONFLICT(connector) DO UPDATE SET rev=excluded.rev",
            (name, rev))

    def _all_known_emps(self) -> set:
        emps = set()
        for (e,) in self.db.execute("SELECT DISTINCT employee_id FROM central"):
            emps.add(e)
        for (e,) in self.db.execute("SELECT DISTINCT employee_id FROM records"):
            emps.add(e)
        return emps

    def _full_refresh_connector(self, name: str) -> set:
        """Replace cached state for one connector with a fresh snapshot.
        engine_rev is preserved per (emp, field): revisions are never
        reused, so a stale engine_rev can never collide with a live one."""
        snapshot, max_rev = self.connectors[name].list_all()
        old_engine = {}
        for emp, field, eng in self.db.execute(
                "SELECT employee_id, field, engine_rev FROM fields "
                "WHERE connector=?", (name,)):
            old_engine[(emp, field)] = eng
        self.db.execute("DELETE FROM fields WHERE connector=?", (name,))
        self.db.execute("DELETE FROM records WHERE connector=?", (name,))
        self.db.executemany(
            "INSERT INTO records VALUES (?,?)",
            [(name, emp) for emp in snapshot])
        rows = []
        for emp, flds in snapshot.items():
            for field, (value, rev) in flds.items():
                rows.append((name, emp, field, value, rev,
                             old_engine.get((emp, field), 0)))
        self.db.executemany("INSERT INTO fields VALUES (?,?,?,?,?,?)", rows)
        self._set_cursor(name, max_rev)
        return set(snapshot.keys())

    def _load_state(self, emps):
        exists = set()
        fstate = {}   # (conn, emp, field) -> (value, rev, engine_rev)
        central = {}  # emp -> {attr: value}
        emps = sorted(emps)
        for i in range(0, len(emps), _CHUNK):
            chunk = emps[i:i + _CHUNK]
            ph = ",".join("?" * len(chunk))
            for conn, emp in self.db.execute(
                    f"SELECT connector, employee_id FROM records "
                    f"WHERE employee_id IN ({ph})", chunk):
                exists.add((conn, emp))
            for conn, emp, field, val, rev, eng in self.db.execute(
                    f"SELECT connector, employee_id, field, value, rev, "
                    f"engine_rev FROM fields WHERE employee_id IN ({ph})",
                    chunk):
                fstate[(conn, emp, field)] = (val, rev, eng)
            for emp, attr, val in self.db.execute(
                    f"SELECT employee_id, attr, value FROM central "
                    f"WHERE employee_id IN ({ph})", chunk):
                central.setdefault(emp, {})[attr] = val
        return exists, fstate, central

    # ------------------------------------------------------------- recompute
    def _assertion(self, conn, emp, attr, exists, fstate):
        """The connector's assertion for (emp, attr), or None."""
        if (conn, emp) not in exists:
            return None
        for field, fm in self.mappings.get(conn, {}).items():
            if fm.attr != attr or fm.inbound is None:
                continue
            val, rev, eng = fstate.get((conn, emp, field), ("", 0, 0))
            if eng and rev == eng:
                return None  # last write to this field was the engine's
            value = apply_chain(val, fm.inbound)
            return value if value != "" else None
        return None

    def _recompute(self, emp_ids):
        if not emp_ids:
            return (0, 0)
        exists, fstate, central = self._load_state(emp_ids)
        central_writes = 0
        connector_writes = 0
        central_upserts = []
        field_upserts = []
        for emp in sorted(emp_ids):
            winners = {}
            new_values = {}
            for attr in ATTRS:
                value, winner = "", None
                for conn in self.priorities[attr]:
                    assertion = self._assertion(conn, emp, attr, exists, fstate)
                    if assertion is not None:
                        value, winner = assertion, conn
                        break
                new_values[attr] = value
                winners[attr] = winner
                old = central.get(emp, {}).get(attr, "")
                if old != value:
                    central_upserts.append((emp, attr, value))
                    central_writes += 1
            for conn, fmap in self.mappings.items():
                if (conn, emp) not in exists:
                    continue
                for field, fm in fmap.items():
                    if fm.outbound is None:
                        continue
                    if winners[fm.attr] == conn:
                        continue
                    desired = apply_chain(new_values[fm.attr], fm.outbound)
                    cur_val, cur_rev, _ = fstate.get((conn, emp, field),
                                                     ("", 0, 0))
                    if cur_val == desired:
                        continue
                    new_rev = self.connectors[conn].write_field(
                        emp, field, desired, cur_rev)
                    if new_rev is None:
                        continue  # concurrent external change; next sync sees it
                    fstate[(conn, emp, field)] = (desired, new_rev, new_rev)
                    field_upserts.append(
                        (conn, emp, field, desired, new_rev, new_rev))
                    connector_writes += 1
        self.db.executemany(
            "INSERT INTO central VALUES (?,?,?) "
            "ON CONFLICT(employee_id, attr) DO UPDATE SET value=excluded.value",
            central_upserts)
        self.db.executemany(
            "INSERT INTO fields VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(connector, employee_id, field) DO UPDATE SET "
            "value=excluded.value, rev=excluded.rev, "
            "engine_rev=excluded.engine_rev",
            field_upserts)
        return (central_writes, connector_writes)
