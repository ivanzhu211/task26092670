"""进程内外部目录（连接器侧存储）。

记录以 employeeId 为键，字段为字符串，缺失即空串。
任何写字段或删记录——无论外部还是引擎所为——都从目录内单调递增、
永不复用的计数器领一个新修订号。目录不记录写入者身份。
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Tuple


class CursorExpired(Exception):
    """增量游标早于保留窗口，已过期。"""


class WriteConflict(Exception):
    """条件写的期望字段修订号与当前不符（或记录不存在）。"""


@dataclass(frozen=True)
class Change:
    rev: int
    employee_id: str
    field: Optional[str]    # None 表示删除记录
    value: Optional[str]    # 删除记录时为 None


@dataclass(frozen=True)
class Snapshot:
    max_rev: int
    # employee_id -> {field: (value, field_rev)}
    records: Dict[str, Dict[str, Tuple[str, int]]]


class Directory:
    """单个外部目录。引擎只能用 list_all / list_changes / write_field。

    线程安全：外部管理员可在任意时刻（包括 Sync 运行中）从其它线程调用
    external_write / external_delete，不会被引擎阻塞。
    """

    def __init__(self, name: str, change_retention: int = 1000) -> None:
        if change_retention < 1:
            raise ValueError("change_retention must be >= 1")
        self.name = name
        self._lock = threading.Lock()
        self._counter = 0
        # employee_id -> {field: (value, field_rev)}
        self._records: Dict[str, Dict[str, Tuple[str, int]]] = {}
        self._changes: Deque[Change] = deque()
        self._retention = change_retention
        # 测试钩子：在读取快照完成之后、返回之前调用（锁外执行）。
        # 用于确定性地把外部写入插在引擎读与写之间。
        self.after_read_hook: Optional[Callable[[str], None]] = None

    # ---- 内部 ----
    def _next_rev(self) -> int:
        self._counter += 1
        return self._counter

    def _append_change(self, change: Change) -> None:
        self._changes.append(change)
        while len(self._changes) > self._retention:
            self._changes.popleft()

    def _write_field_locked(self, employee_id: str, field: str, value: str) -> int:
        record = self._records.setdefault(employee_id, {})
        rev = self._next_rev()
        record[field] = (value, rev)
        self._append_change(Change(rev, employee_id, field, value))
        return rev

    def _fire_hook(self, kind: str) -> None:
        hook = self.after_read_hook
        if hook is not None:
            hook(kind)

    # ---- 引擎 API ----
    def list_all(self) -> Snapshot:
        with self._lock:
            records = {emp: dict(fields) for emp, fields in self._records.items()}
            snap = Snapshot(self._counter, records)
        self._fire_hook("list_all")
        return snap

    def list_changes(self, since_rev: int) -> Tuple[int, List[Change]]:
        with self._lock:
            current = self._counter
            if since_rev > current:
                raise CursorExpired(
                    f"cursor {since_rev} beyond current {current}")
            if since_rev < current:
                if not self._changes or self._changes[0].rev > since_rev + 1:
                    raise CursorExpired(
                        f"cursor {since_rev} older than retained window")
                changes = [c for c in self._changes if c.rev > since_rev]
            else:
                changes = []
            result = (current, changes)
        self._fire_hook("list_changes")
        return result

    def write_field(self, employee_id: str, field: str, value: str,
                    expected_rev: int) -> int:
        """以字段修订号为条件写。expected_rev=0 表示字段必须不存在。"""
        with self._lock:
            record = self._records.get(employee_id)
            if record is None:
                raise WriteConflict(f"record {employee_id!r} does not exist")
            current_rev = record[field][1] if field in record else 0
            if current_rev != expected_rev:
                raise WriteConflict(
                    f"field {field!r} rev {current_rev} != expected {expected_rev}")
            return self._write_field_locked(employee_id, field, value)

    # ---- 外部管理员 API（测试/外部行为，引擎不得调用） ----
    def external_write(self, employee_id: str, field: str, value: str) -> int:
        with self._lock:
            return self._write_field_locked(employee_id, field, value)

    def external_delete(self, employee_id: str) -> int:
        with self._lock:
            rev = self._next_rev()
            self._records.pop(employee_id, None)
            self._append_change(Change(rev, employee_id, None, None))
            return rev

    # ---- 测试观察口（引擎不使用） ----
    def debug_record(self, employee_id: str) -> Optional[Dict[str, str]]:
        with self._lock:
            record = self._records.get(employee_id)
            if record is None:
                return None
            return {field: value for field, (value, _rev) in record.items()}

    def debug_employees(self) -> List[str]:
        with self._lock:
            return sorted(self._records)
