# Storage and Recovery

## Storage Model

### Redis (hot state)

Managed key families:

- `device:{device_id}:last_heartbeat` (string timestamp)
- `device:{device_id}:heartbeats` (zset of heartbeat timestamps)
- `room:{room_id}:presence` (hash with `in_room`, `ts`)
- `room:{room_id}:occupancy` (zset of occupancy transitions)
- `dedup:{sha256(...)}` (fall dedup key with TTL)

Primary purpose:
- Fast read-path state for health and occupancy APIs.
- Short-term dedup and live alarm fan-out support.

### SQLite (durable state)

Defined in `core/db.py`:

- `events`
  - Append-only durable log for all processed events
    (`device_id`, `room_id`, `type`, `ts`, `payload` JSON, `received_at`, `late`).
- `fall_warnings`
  - Durable deduplicated fall alarms
    (`device_id`, `room_id`, `ts`, `confidence`, `dedup_key`, `received_at`).
  - Uniqueness enforced by `UNIQUE(dedup_key)`.
- `state_snapshots`
  - Periodic serialized capture of managed Redis state (`snapshot_ts`, `state_json`).

The connection runs `journal_mode=WAL`, `synchronous=NORMAL`, and `busy_timeout=5000` (`core/db.py`)
so the per-event insert on the hot path avoids an fsync per commit while staying crash-safe for
committed transactions except on OS/power loss (which snapshot + replay recovery tolerates).

## Persistence Semantics

- Every flushed worker event is inserted into `events` before handler-specific logic.
- `fall_warn` handler inserts into `fall_warnings` after Redis dedup acceptance.
- Commits are explicit (`db_connection.commit()`) per insertion path.

## Snapshot Semantics

`RecoveryManager.write_snapshot()`:

1. Captures selected managed Redis keys.
2. Stores JSON snapshot in `state_snapshots` with `snapshot_ts`.
3. Runs periodically via background loop (default 60s).

Capture uses `SCAN` (never `KEYS`, which is O(N) and blocks the whole Redis server) and a single
pipelined batch of value reads. The periodic loop stamps `snapshot_ts` first, then runs the
capture off the event loop (thread executor) so a large keyspace never freezes ingestion or the
alarm hot path; the SQLite write stays on the loop thread.

Captured Redis structures:

- strings
- hashes
- zsets (member + score)

## Restore Semantics

`RecoveryManager.restore_state()`:

1. Load latest snapshot by `snapshot_ts DESC`.
2. Clear managed Redis keys.
3. Reapply snapshot values.
4. Replay durable events from `events` in `ts ASC` order.

Replay notes:

- Cutoff is on `received_at` (ingestion order), not `ts` (device clock): a snapshot reflects the
  events ingested before its wall-clock timestamp, so a late event (old `ts`, ingested after the
  snapshot) is replayed instead of being silently dropped. Rows are still ordered by `ts ASC`.
- Boundary is inclusive (`received_at >= snapshot_ts`) to avoid dropping exact-boundary events.
- Replay invokes same handlers used in normal flow.
- Timestamp-aware handlers avoid stale overwrite of newer state.
- Malformed rows are skipped instead of failing recovery.

## Failure Modes and Safeguards

- Redis cold start:
  - Rebuilt from snapshot plus replay.
- Snapshot unavailable/corrupt:
  - Replay still rebuilds from full event log.
- Duplicate fall warnings after Redis restart:
  - SQLite unique `dedup_key` prevents durable duplication.
  - Counted separately as DB conflict metric.
- Out-of-order event insertion:
  - Replay reads `ORDER BY ts ASC` to recover chronological state.

## Evidence in Tests

Behavior validated by test suite:

- Non-blocking backpressure and no HIGH/NORMAL priority inversion under a saturated normal lane
  (`tests/test_queue_backpressure.py`)
- High lane priority over normal lane (`tests/test_phase2_ingestion.py`)
- Handler isolation and persistence flow (`tests/test_phase3_processing.py`)
- Snapshot restore and replay boundary correctness (`tests/test_recovery.py`)
- Burst alarm latency and replay equivalence checks (`tests/test_integration.py`)
