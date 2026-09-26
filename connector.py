"""In-process external directory (connector) with revision tracking.

The engine may only use: list_all(), list_changes(), write_field().
admin_* methods model the external administrator and are used by tests.
"""
from __future__ import annotations

import threading
from collections import deque
from typing import Optional


class CursorExpired(Exception):
    """Raised when the requested delta cursor is older than the retained log."""


class Directory:
    """A single external directory. Records keyed by employeeId; fields are
    strings, missing means "". Every write/delete consumes a fresh revision
    from a monotonic, never-reused counter. The directory never reveals who
    performed a write."""

    def __init__(self, name: str, fields, changelog_size: int = 1024) -> None:
        self.name = name
        self.fields = tuple(fields)
        self._lock = threading.Lock()
        self._records = {}  # emp -> {field: (value, rev)}
        self._rev = 0
        self._log = deque(maxlen=changelog_size)

    def _check_field(self, field: str) -> None:
        if field not in self.fields:
            raise KeyError(f"{self.name}: unknown field {field!r}")

    def _check_value(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("field values must be strings")

    # ---- engine-facing API ----
    def list_all(self):
        """-> (snapshot, max_rev); snapshot: emp -> {field: (value, rev)}"""
        with self._lock:
            snap = {emp: dict(rec) for emp, rec in self._records.items()}
            return snap, self._rev

    def list_changes(self, since_rev: int):
        """-> (changes, new_cursor); change: (rev, emp, field|None, value|None).
        field is None for record deletion. Raises CursorExpired if entries
        needed to catch up were dropped from the retained log."""
        with self._lock:
            if since_rev > self._rev:
                raise ValueError(f"cursor {since_rev} beyond max rev {self._rev}")
            if self._log and self._log[0][0] > since_rev + 1:
                raise CursorExpired(
                    f"{self.name}: cursor {since_rev} expired, "
                    f"oldest retained rev {self._log[0][0]}")
            changes = [e for e in self._log if e[0] > since_rev]
            return changes, self._rev

    def write_field(self, employee_id: str, field: str, value: str,
                    expected_rev: int) -> Optional[int]:
        """Conditional write: succeeds only if the record exists and the
        field's current revision equals expected_rev. -> new rev or None."""
        self._check_field(field)
        self._check_value(value)
        with self._lock:
            rec = self._records.get(employee_id)
            if rec is None:
                return None
            current_rev = rec.get(field, ("", 0))[1]
            if current_rev != expected_rev:
                return None
            rev = self._next_rev()
            rec[field] = (value, rev)
            self._log.append((rev, employee_id, field, value))
            return rev

    # ---- external administrator API (tests only) ----
    def admin_write(self, employee_id: str, field: str, value: str) -> int:
        """External write; creates the record if it does not exist."""
        self._check_field(field)
        self._check_value(value)
        with self._lock:
            rec = self._records.setdefault(employee_id, {})
            rev = self._next_rev()
            rec[field] = (value, rev)
            self._log.append((rev, employee_id, field, value))
            return rev

    def admin_delete(self, employee_id: str) -> Optional[int]:
        """External delete of a whole record. -> delete rev or None."""
        with self._lock:
            if employee_id not in self._records:
                return None
            del self._records[employee_id]
            rev = self._next_rev()
            self._log.append((rev, employee_id, None, None))
            return rev

    def _next_rev(self) -> int:
        self._rev += 1
        return self._rev
