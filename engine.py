"""身份同步引擎：外部目录（连接器）与中心身份库（sqlite3）之间的双向属性同步。

回写识别：目录计数器单调递增且永不复用，引擎把自己每次写拿到的字段修订号
持久化；字段当前修订号等于该值即视为引擎所留，否则视为外部断言。不做值比较。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from directory import CursorExpired, Directory, WriteConflict

ATTRIBUTES: Tuple[str, ...] = ("displayName", "email", "department", "jobTitle")

# 变换链元素： "upper" | "trim" | ("default", v)
Transform = object

_DIRECTIONS = ("in", "out", "both")
# 冲突追赶的重试上限（外部写入有限时必然在此之前收敛）
_MAX_CATCH_UPS = 10000


@dataclass(frozen=True)
class FieldMapping:
    field: str
    attribute: str
    direction: str                      # "in" | "out" | "both"
    inbound: Tuple[Transform, ...] = ()
    outbound: Tuple[Transform, ...] = ()


def apply_chain(chain: Sequence[Transform], value: str) -> str:
    for item in chain:
        if item == "upper":
            value = value.upper()
        elif item == "trim":
            value = value.strip()
        elif (isinstance(item, tuple) and len(item) == 2
              and item[0] == "default" and isinstance(item[1], str)):
            if value == "":
                value = item[1]
        else:
            raise ValueError(f"unknown transform: {item!r}")
    return value


class _Conflict(Exception):
    """出站条件写冲突，携带连接器名。"""

    def __init__(self, connector: str) -> None:
        super().__init__(connector)
        self.connector = connector


_SCHEMA = """
CREATE TABLE IF NOT EXISTS cursors(
    connector TEXT PRIMARY KEY,
    rev INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS records(
    connector TEXT NOT NULL,
    employee TEXT NOT NULL,
    PRIMARY KEY(connector, employee)
);
CREATE TABLE IF NOT EXISTS fields(
    connector TEXT NOT NULL,
    employee TEXT NOT NULL,
    field TEXT NOT NULL,
    value TEXT NOT NULL,
    rev INTEGER NOT NULL,
    engine_rev INTEGER,
    PRIMARY KEY(connector, employee, field)
);
CREATE TABLE IF NOT EXISTS central(
    employee TEXT PRIMARY KEY,
    displayName TEXT NOT NULL,
    email TEXT NOT NULL,
    department TEXT NOT NULL,
    jobTitle TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fields_employee ON fields(employee);
CREATE INDEX IF NOT EXISTS idx_records_employee ON records(employee);
"""


class SyncEngine:
    """同一时刻至多一个 Sync；全部状态持久化在 db_path 的 sqlite 库中。"""

    def __init__(self, db_path: str,
                 connectors: Dict[str, Directory],
                 mappings: Dict[str, List[FieldMapping]],
                 priorities: Dict[str, List[str]]) -> None:
        if not connectors:
            raise ValueError("at least one connector is required")
        self._connectors = dict(connectors)
        self._inbound: Dict[str, Dict[str, FieldMapping]] = {
            name: {} for name in connectors}
        self._outbound: Dict[str, List[FieldMapping]] = {
            name: [] for name in connectors}
        self._mapped_fields: Dict[str, set] = {name: set() for name in connectors}
        for conn, maps in (mappings or {}).items():
            if conn not in connectors:
                raise ValueError(f"mapping for unknown connector {conn!r}")
            seen_fields = set()
            for m in maps:
                if m.direction not in _DIRECTIONS:
                    raise ValueError(f"bad direction {m.direction!r}")
                if m.attribute not in ATTRIBUTES:
                    raise ValueError(f"bad attribute {m.attribute!r}")
                if m.field in seen_fields:
                    raise ValueError(
                        f"field {m.field!r} mapped twice in {conn!r}")
                seen_fields.add(m.field)
                self._mapped_fields[conn].add(m.field)
                if m.direction in ("in", "both"):
                    if m.attribute in self._inbound[conn]:
                        raise ValueError(
                            f"duplicate inbound for {m.attribute!r} in {conn!r}")
                    self._inbound[conn][m.attribute] = m
                if m.direction in ("out", "both"):
                    self._outbound[conn].append(m)
        self._priorities: Dict[str, List[str]] = {}
        self._prio_sets: Dict[str, set] = {}
        for attr in ATTRIBUTES:
            plist = list((priorities or {}).get(attr, []))
            for name in plist:
                if name not in connectors:
                    raise ValueError(
                        f"priority list for {attr!r} names unknown connector {name!r}")
            self._priorities[attr] = plist
            self._prio_sets[attr] = set(plist)
        self._db = sqlite3.connect(db_path, isolation_level=None)
        self._db.executescript(_SCHEMA)
        self._in_tx = False

    # ---------------- 公共接口 ----------------
    def sync_full(self) -> Tuple[int, int]:
        """全量同步，返回 (中心写入数, 连接器写入数)。"""
        affected = set()
        for name in self._connectors:
            affected |= self._full_load(name)
        affected |= {r[0] for r in self._db.execute("SELECT employee FROM central")}
        return self._process(affected)

    def sync_delta(self) -> Tuple[int, int]:
        """增量同步，返回 (中心写入数, 连接器写入数)。游标过期自动退化为全量。"""
        affected = set()
        for name, directory in self._connectors.items():
            cursor = self._cursor(name)
            if cursor is None:
                affected |= self._full_load(name)
                continue
            try:
                new_max, changes = directory.list_changes(cursor)
            except CursorExpired:
                affected |= self._full_load(name)
                continue
            self._begin()
            try:
                affected |= self._apply_changes(name, changes)
                self._set_cursor(name, new_max)
                self._commit()
            except BaseException:
                self._rollback()
                raise
        return self._process(affected)

    def get(self, employee_id: str) -> Dict[str, str]:
        row = self._db.execute(
            "SELECT displayName, email, department, jobTitle "
            "FROM central WHERE employee=?", (employee_id,)).fetchone()
        if row is None:
            return {attr: "" for attr in ATTRIBUTES}
        return dict(zip(ATTRIBUTES, row))

    def close(self) -> None:
        self._db.close()

    # ---------------- 可重入事务 ----------------
    def _begin(self) -> None:
        if not self._in_tx:
            self._db.execute("BEGIN")
            self._in_tx = True

    def _commit(self) -> None:
        if self._in_tx:
            self._db.execute("COMMIT")
            self._in_tx = False

    def _rollback(self) -> None:
        if self._in_tx:
            self._db.execute("ROLLBACK")
            self._in_tx = False

    # ---------------- 游标 ----------------
    def _cursor(self, connector: str) -> Optional[int]:
        row = self._db.execute(
            "SELECT rev FROM cursors WHERE connector=?", (connector,)).fetchone()
        return row[0] if row else None

    def _set_cursor(self, connector: str, rev: int) -> None:
        self._db.execute(
            "INSERT INTO cursors(connector, rev) VALUES (?,?) "
            "ON CONFLICT(connector) DO UPDATE SET rev=excluded.rev",
            (connector, rev))

    # ---------------- 缓存装载 ----------------
    def _full_load(self, connector: str) -> set:
        """用 list_all 快照重建该连接器的缓存，返回受影响身份集合。"""
        snap = self._connectors[connector].list_all()
        mapped = self._mapped_fields[connector]
        self._begin()
        try:
            old = {r[0] for r in self._db.execute(
                "SELECT employee FROM records WHERE connector=?", (connector,))}
            saved = self._db.execute(
                "SELECT employee, field, rev, engine_rev FROM fields "
                "WHERE connector=? AND engine_rev IS NOT NULL",
                (connector,)).fetchall()
            self._db.execute("DELETE FROM fields WHERE connector=?", (connector,))
            self._db.execute("DELETE FROM records WHERE connector=?", (connector,))
            self._db.executemany(
                "INSERT INTO records(connector, employee) VALUES (?,?)",
                [(connector, emp) for emp in snap.records])
            rows = []
            for emp, fields in snap.records.items():
                for fname, (value, rev) in fields.items():
                    if fname in mapped:
                        rows.append((connector, emp, fname, value, rev))
            self._db.executemany(
                "INSERT INTO fields(connector, employee, field, value, rev,"
                " engine_rev) VALUES (?,?,?,?,?,NULL)", rows)
            # 修订号永不复用：快照里修订号未变的字段仍是我们当初写的那次
            self._db.executemany(
                "UPDATE fields SET engine_rev=? WHERE connector=? AND employee=?"
                " AND field=? AND rev=?",
                [(erev, connector, emp, fname, rev)
                 for emp, fname, rev, erev in saved])
            self._set_cursor(connector, snap.max_rev)
            self._commit()
        except BaseException:
            self._rollback()
            raise
        return old | set(snap.records)

    def _apply_changes(self, connector: str, changes) -> set:
        """把增量变更应用到缓存（须在事务内调用），返回受影响身份集合。"""
        affected = set()
        mapped = self._mapped_fields[connector]
        for change in changes:
            emp = change.employee_id
            affected.add(emp)
            if change.field is None:
                self._db.execute(
                    "DELETE FROM fields WHERE connector=? AND employee=?",
                    (connector, emp))
                self._db.execute(
                    "DELETE FROM records WHERE connector=? AND employee=?",
                    (connector, emp))
                continue
            self._db.execute(
                "INSERT OR IGNORE INTO records(connector, employee) VALUES (?,?)",
                (connector, emp))
            if change.field not in mapped:
                continue
            # 若缓存修订号与变更相同，说明这就是引擎自己那笔写，保留 engine_rev
            self._db.execute(
                "INSERT INTO fields(connector, employee, field, value, rev,"
                " engine_rev) VALUES (?,?,?,?,?,NULL) "
                "ON CONFLICT(connector, employee, field) DO UPDATE SET"
                " value=excluded.value, rev=excluded.rev,"
                " engine_rev=CASE WHEN fields.rev=excluded.rev"
                " THEN fields.engine_rev ELSE NULL END",
                (connector, emp, change.field, change.value, change.rev))
        return affected

    # ---------------- 语义计算 ----------------
    def _employee_state(self, emp: str):
        records = {r[0] for r in self._db.execute(
            "SELECT connector FROM records WHERE employee=?", (emp,))}
        fields = {}
        for conn, fname, value, rev, erev in self._db.execute(
                "SELECT connector, field, value, rev, engine_rev FROM fields"
                " WHERE employee=?", (emp,)):
            fields[(conn, fname)] = (value, rev, erev)
        return records, fields

    def _assertion(self, connector: str, emp: str, attr: str,
                   records, fields) -> Optional[str]:
        mapping = self._inbound[connector].get(attr)
        if mapping is None or connector not in self._prio_sets[attr]:
            return None
        if connector not in records:
            return None
        value, rev, engine_rev = fields.get(
            (connector, mapping.field), ("", 0, None))
        if engine_rev is not None and rev == engine_rev:
            return None
        result = apply_chain(mapping.inbound, value)
        return result if result != "" else None

    def _compute_central(self, emp: str, records, fields):
        values = {}
        winners = {}
        for attr in ATTRIBUTES:
            value, winner = "", None
            for conn in self._priorities[attr]:
                assertion = self._assertion(conn, emp, attr, records, fields)
                if assertion is not None:
                    value, winner = assertion, conn
                    break
            values[attr] = value
            winners[attr] = winner
        return values, winners

    def _persist_central(self, emp: str, values: Dict[str, str]) -> int:
        row = self._db.execute(
            "SELECT displayName, email, department, jobTitle FROM central"
            " WHERE employee=?", (emp,)).fetchone()
        old = dict(zip(ATTRIBUTES, row)) if row else {a: "" for a in ATTRIBUTES}
        changed = sum(1 for a in ATTRIBUTES if old[a] != values[a])
        if not changed:
            return 0
        if row is None:
            self._db.execute(
                "INSERT INTO central VALUES (?,?,?,?,?)",
                (emp,) + tuple(values[a] for a in ATTRIBUTES))
        else:
            self._db.execute(
                "UPDATE central SET displayName=?, email=?, department=?,"
                " jobTitle=? WHERE employee=?",
                tuple(values[a] for a in ATTRIBUTES) + (emp,))
        return changed

    def _write_outbound(self, emp: str, values, winners, records, fields) -> int:
        writes = 0
        for conn, out_maps in self._outbound.items():
            if not out_maps or conn not in records:
                continue
            directory = self._connectors[conn]
            for m in out_maps:
                if winners.get(m.attribute) == conn:
                    continue
                desired = apply_chain(m.outbound, values[m.attribute])
                cur_value, cur_rev, _ = fields.get((conn, m.field), ("", 0, None))
                if cur_value == desired:
                    continue
                try:
                    new_rev = directory.write_field(
                        emp, m.field, desired, cur_rev)
                except WriteConflict:
                    raise _Conflict(conn)
                self._db.execute(
                    "INSERT INTO fields(connector, employee, field, value, rev,"
                    " engine_rev) VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(connector, employee, field) DO UPDATE SET"
                    " value=excluded.value, rev=excluded.rev,"
                    " engine_rev=excluded.engine_rev",
                    (conn, emp, m.field, desired, new_rev, new_rev))
                writes += 1
        return writes

    # ---------------- 冲突追赶 ----------------
    def _catch_up(self, connector: str) -> set:
        """条件写冲突后追赶该连接器的新变更（须在 _process 事务内调用）。"""
        cursor = self._cursor(connector)
        if cursor is None:
            return self._full_load(connector)
        try:
            new_max, changes = self._connectors[connector].list_changes(cursor)
        except CursorExpired:
            return self._full_load(connector)
        affected = self._apply_changes(connector, changes)
        self._set_cursor(connector, new_max)
        return affected

    # ---------------- 主处理循环 ----------------
    def _process(self, affected) -> Tuple[int, int]:
        central_writes = 0
        connector_writes = 0
        queue = list(affected)
        queued = set(affected)
        catch_ups = 0
        self._begin()
        try:
            while queue:
                emp = queue.pop()
                queued.discard(emp)
                records, fields = self._employee_state(emp)
                values, winners = self._compute_central(emp, records, fields)
                central_writes += self._persist_central(emp, values)
                try:
                    connector_writes += self._write_outbound(
                        emp, values, winners, records, fields)
                except _Conflict as conflict:
                    catch_ups += 1
                    if catch_ups > _MAX_CATCH_UPS:
                        raise RuntimeError(
                            "too many write conflicts; external writers not"
                            " converging")
                    for other in self._catch_up(conflict.connector) | {emp}:
                        if other not in queued:
                            queued.add(other)
                            queue.append(other)
            self._commit()
        except BaseException:
            self._rollback()
            raise
        return central_writes, connector_writes
