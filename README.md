# Identity Sync Engine

Bidirectional attribute sync between external directories (connectors) and a
central identity store. Python 3 standard library only; engine state persists
in SQLite, so an instance can be dropped and rebuilt on the same DB file
between syncs with no behaviour change.

## Layout

- `connector.py` — in-process `Directory`: records keyed by employeeId,
  string fields (missing = ""), a monotonic never-reused revision counter,
  a bounded change log. Engine-facing API: `list_all()`,
  `list_changes(since_rev)` (raises `CursorExpired`), and conditional
  `write_field(emp, field, value, expected_rev)`. `admin_write/admin_delete`
  model the external administrator (tests only; the directory never reveals
  who wrote).
- `mapping.py` — `FieldMapping(attr, inbound, outbound)` with transform
  chains (`upper`, `trim`, `("default", v)`); central attributes are
  `displayName`, `email`, `department`, `jobTitle`.
- `engine.py` — `SyncEngine(db_path, connectors, mappings, priorities)` with
  `sync_full()`, `sync_delta()` (both return `(central_writes,
  connector_writes)`) and `get(employee_id)`.
- `tests/test_sync.py` — randomized full-vs-delta equivalence against an
  independent oracle, concurrent-writer, deterministic read/write
  interleaving, and a 10k-identity x 4-connector performance test.

## How it works

The engine caches, per connector field, the last read `(value, rev)` plus
`engine_rev` — the revision of its own last write. A connector asserts an
attribute iff the record exists, the connector is in the attribute's priority
list, and the field's current rev differs from `engine_rev` (revisions are
never reused, so this identifies engine writes without comparing values).
Central value = first assertion in priority order, else "". Non-winning
connectors with outbound mappings are brought up to date with conditional
writes; a failed compare-and-set means a concurrent external write, which is
never overwritten and is picked up by the next sync via the revision cursor.
Delta syncs cost O(changes); an expired cursor degrades that connector to a
full refresh.

## Run tests

    python3 -m unittest discover -s tests -t . -v
