---
applyTo: 'core/**/*.py'
---

# Recovery Invariants  zip01

**Scope**: `core/recovery.py`, `core/db.py` (snapshots/durability), restart sequences.  
**Audience**: Before touching recovery logic, snapshot/replay, or crash safety.  
**Update Cadence**: When recovery or durability semantics change; these are architectural safety invariants.

---

## Event Durability Guarantee

**Invariant**: Events are never lost. Persisted events survive crashes and are recoverable on restart.

### The Contract: Persist-Before-Ack

- Event arrives at `POST /events`.
- **Before** returning 202, event is written to SQLite `events` table.
- If persistence fails (database unavailable), return 503 (transient error); client can retry.
- If persistence succeeds, return 202; event is durable, and client does not retry.

```python
@app.post("/events")
async def ingest_event(request: Request):
    """Event durability: persist first, then ack."""
    validated = validate_raw_event(body)
    
    # MUST succeed before 202
    try:
        await db.insert_event(validated)
    except Exception as e:
        logger.error(f"Persist failed: {e}")
        return {"status": "error", "error": "persist_failed"}, 503
    
    # Now it's safe to queue and return 202
    await event_queue.put(validated)
    return {"status": "ok"}, 202
```

### Handler Execution  Durability

- Handlers run **after** the 202 response.
- If handler fails, event remains durable (persisted in SQLite).
- On restart, event is replayed and handler is retried (idempotently).

This separation ensures **no event loss**, even if:
- Process crashes during handler execution.
- Redis is unavailable when handler tries to update state.
- Handler throws an exception.

---

## Snapshot & Replay Boundary

**Invariant**: Recovery must restore hot state correctly without losing or duplicating events.

### The Snapshot

- Periodically (every ~1 minute by default), the recovery manager writes a snapshot to SQLite `state_snapshots` table.
- Snapshot captures: timestamp, and a serialized copy of managed Redis state.

### The Replay Boundary

- On startup:
  1. Load the latest snapshot from SQLite.
  2. Clear managed Redis keys.
  3. Reapply snapshot data to Redis.
  4. Replay all events with `received_at >= snapshot_timestamp` (inclusive).
- **Inclusive boundary**: events received at or after the snapshot time are replayed.
- This ensures **late events are not dropped**: if an offline device ingests an event after the snapshot was taken, that event is replayed on restart.

 Good (inclusive boundary):
```python
async def restore_state(self) -> None:
    """Restore from snapshot + replay."""
    # Load snapshot
    snapshot = await db.fetch_latest_snapshot()
    if not snapshot:
        logger.info("No snapshot; starting fresh")
        return
    
    snapshot_ts = snapshot['ts']
    
    # Clear managed keys (alarm_bus, occupancy, etc.)
    self.redis.delete_keys_for_managed_prefixes()
    
    # Reapply snapshot data
    for key, value in snapshot['redis_state'].items():
        self.redis.set(key, value)
    
    # Replay events: received_at >= snapshot_ts (inclusive)
    async for event in db.fetch_events_since(snapshot_ts):
        await self.process_event(event)
```

### Why Inclusive?

Consider this scenario:
1. Snapshot taken at T=10:00 (captures Redis state).
2. Offline device ingests event at T=10:01 (event stored in SQLite).
3. Process crashes at T=10:02 before processing the event.
4. On restart:
   - If replay starts at T=10:00 (exclusive), the event at T=10:01 is skipped (lost!).
   - If replay starts at T=10:00 (inclusive), the event at T=10:01 is replayed (correct!).

The inclusive boundary is the safe default.

---

## Redis Cold-Start Guarantee

**Invariant**: If Redis is wiped or unavailable at startup, managed state is rebuilt from SQLite. Unmanaged keys are not touched.

### Managed Keys (Owned by zip01)

Redis keys that zip01 writes and recovers:
- `device:{id}:last_heartbeat`, `device:{id}:heartbeats`
- `room:{id}:occupancy`, `room:{id}:presence`
- Dedup cache (optional): `dedup:{key}`

### Unmanaged Keys (External)

Keys written by external systems (e.g., logging, monitoring):
- These are **not** cleared or overwritten during recovery.
- Only managed keys are reset and reapplied from snapshot.

### Implementation

```python
async def restore_state(self) -> None:
    """Clear managed keys only; rebuild from snapshot."""
    managed_prefixes = [
        "device:",
        "room:",
        "dedup:",
    ]
    
    # Clear only managed keys
    for prefix in managed_prefixes:
        keys = self.redis.keys(f"{prefix}*")
        if keys:
            self.redis.delete(keys)
    
    # Reapply from snapshot
    snapshot = await db.fetch_latest_snapshot()
    for key, value in snapshot['redis_state'].items():
        if any(key.startswith(p) for p in managed_prefixes):
            self.redis.set(key, value)
```

### Recovery from SQLite Alone

If Redis is completely unavailable, zip01 can still recover:
1. Skip the redis.set() for managed keys (catch connection errors).
2. Replay events from SQLite.
3. Handlers populate Redis as events are processed.

This is graceful degradation: recovery is slower (hot state is not preloaded), but data integrity is maintained.

---

## No Event Loss Through Crash

**Invariant**: Combining persist-before-ack + snapshot + replay ensures no event loss, even under crashes or failures.

### The Path

1. **Live event ingestion**:
   - Event persisted to SQLite.
   - Event enqueued.
   - Response 202.
   - Handler runs (may fail, but event is safe).

2. **Graceful shutdown**:
   - Worker pool drains queued events.
   - Final snapshot written.
   - SQLite connection closed.

3. **Crash (no graceful shutdown)**:
   - Any in-flight handlers are interrupted.
   - Durable events remain in SQLite.
   - Hot state (Redis) may be incomplete.

4. **Restart**:
   - Load snapshot (may be old).
   - Replay all events from snapshot boundary.
   - Handlers run again (idempotently).
   - Hot state rebuilt.

### The Guarantee

No event is lost because:
- Every accepted event is persisted (step 1).
- On restart, all events are replayed (step 4).
- Handlers are idempotent (running twice produces same result as once).

---

## Metrics Persistence

**Invariant**: Counters and histograms are **not** persisted. They reset on restart. This is acceptable and intentional.

### Why?

- Metrics are for operator visibility, not correctness.
- Resetting on restart is acceptable (metrics are sampled and dashboards track trends).
- Persisting metrics would require extra database writes (latency impact).

### Affected Metrics

- `events_accepted`, `events_rejected_*`: reset on restart.
- `alarms_total`, `alarms_dedup`: reset on restart.
- `alarm_p95_ms`: reset on restart.
- Queue depths (`queue_pressure`, `high_queue_depth`): observable state, not persisted (current snapshot only).

 Good (metrics reset):
```python
# Metrics are instance variables; lost on restart
class Metrics:
    def __init__(self):
        self.events_accepted = 0
        self.alarms_total = 0
    
    def record_event_accepted(self):
        self.events_accepted += 1  # Lost on restart
```

### Durability vs Metrics

- **Durable**: Event rows in SQLite (used for replay, analysis, SLA verification).
- **Ephemeral**: Counters in Python (used for real-time dashboards, alerting).

For historical metrics, query SQLite directly (e.g., "How many alarms in the last hour?").

---

## Snapshot Frequency and Overhead

**Implementation** (may change):
- Snapshot every ~1 minute (configurable).
- Snapshot includes entire managed Redis state (O(n) in live state size).
- Async snapshot writes (don't block event processing).
- Old snapshots are cleaned up (keep only latest).

**Tradeoff**:
- Frequent snapshots: faster recovery (less replay), but more database writes.
- Infrequent snapshots: slower recovery (more replay), but fewer writes.

Default (1 minute) balances both for typical load.

---

## Recovery Testing

**Test Coverage** (see `tests/test_core.py`):
- `test_recover_state_from_snapshot_and_replay()`: full recovery cycle.
- `test_recover_state_with_offline_events()`: late events ingested after snapshot.
- `test_recover_state_redis_cold_start()`: Redis cleared; rebuilt from snapshot + replay.
- `test_recovery_idempotent()`: replaying events twice produces same state.
- `test_recovery_no_event_loss()`: all persisted events present after recovery.

---

## Graceful Shutdown

**Invariant**: Shutdown must allow in-flight events to complete before closing resources.

### Sequence

1. Receive shutdown signal (SIGTERM).
2. Stop accepting new HTTP requests (close listener).
3. Drain worker pool (allow in-flight handlers to complete, bounded by timeout).
4. Write final snapshot.
5. Close SQLite connection.
6. Exit.

 Good (graceful):
```python
async def shutdown_event():
    """Graceful shutdown on SIGTERM."""
    logger.info("Shutdown signal received; draining workers...")
    await worker_pool.drain(timeout=5)  # wait up to 5s for handlers to finish
    
    logger.info("Writing final snapshot...")
    await recovery_manager.snapshot()
    
    logger.info("Closing database...")
    await db.close()
    
    logger.info("Shutdown complete")
```

### Result

- Events already persisted: durable and safe.
- In-flight handlers: allowed to complete (or killed after timeout, but event is still durable).
- Fresh restart: replays any incomplete events.

---

## Summary: Non-Negotiables

1. **Events persist before the 202 response.** No event loss.
2. **Replay boundary is inclusive (>= snapshot_ts).** Late events not dropped.
3. **Managed Redis keys are cleared and rebuilt.** Unmanaged keys untouched.
4. **Metrics reset on restart.** Acceptable; durability is separate.
5. **Handlers are idempotent.** Safe to replay.

These invariants ensure crash safety and data correctness. Test them, keep them, and update this file if they change.

---

## Observability

**Guidance** (recommended):
- Log snapshot timestamps and event replay boundaries.
- Alert on recovery taking longer than expected (indicates large replay backlog).
- Track "events replayed since last snapshot" as a health metric.
- Verify that event count matches between SQLite and replayed state.

Example log:
```
Snapshot loaded: ts=2025-08-06T09:00:00Z, state_size=1200 keys
Replaying events from ts=2025-08-06T09:00:00Z (inclusive)
Replayed 450 events in 2.3s
Recovery complete; hot state ready
```

