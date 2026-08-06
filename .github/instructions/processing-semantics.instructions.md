---
applyTo: 'processing/**/*.py'
---

# Processing Semantics  zip01

**Scope**: `processing/worker_pool.py`, handler functions, alarm bus.  
**Audience**: When changing routing, buffering, handler dispatch, or priority logic.  
**Update Cadence**: When ordering or priority behavior changes; these are architectural invariants.

---

## Ordering Guarantee

**Invariant**: Ordering is guaranteed only within a bounded reorder window. Events arriving outside the window are applied in ingestion order, not timestamp order. This is intentional.

### The Guarantee (What's True)

- Events from a single device arriving within `DEVICE_REORDER_BUFFER_MS` (currently 100ms) are **sorted by `ts` before applying handlers**.
- Events arriving after the window has flushed are applied **later**, regardless of their `ts`.
- **This is bounded, not unconditional**: unbounded buffering would break real-time alarm latency (p95 < 1s target).

### The Implication (What Handlers Must Do)

All handlers must be **idempotent** and **timestamp-aware**. They must not assume strict apply order.

 Good (ts-aware, idempotent):
```python
def handle_heartbeat(self, event: Event) -> None:
    """Update device heartbeat only if event is newer."""
    current_ts = self.redis.get(f"device:{event.device_id}:last_heartbeat")
    
    # Only update if strictly newer
    if current_ts is None or event.ts > current_ts:
        self.redis.set(f"device:{event.device_id}:last_heartbeat", event.ts)
        self.redis.append(f"device:{event.device_id}:heartbeats", event.ts)
```

 Bad (assumes strict order, not idempotent):
```python
def handle_heartbeat(self, event: Event) -> None:
    """Update heartbeat (WRONG: not ts-aware)."""
    # If an older event arrives after a newer one, this overwrites with stale data
    self.redis.set(f"device:{event.device_id}:last_heartbeat", event.ts)
```

### Why This Design?

- **Real-time constraint**: Waiting for all possible late arrivals would introduce unbounded latency (could wait forever).
- **Idempotence payoff**: Handlers become recoverable; replaying events (on restart) applies idempotently without side effects.
- **Correctness**: Derived state (occupancy, latest status) is kept correct by ts-aware logic, not by apply order.

### Test Coverage

See `tests/test_ordering.py`:
- `test_events_within_window_sorted_by_ts()`: events arriving within 100ms are reordered.
- `test_events_after_window_applied_in_ingestion_order()`: events arriving after flush are applied later.
- `test_late_offline_device_arrival_applied_after_newer_events()`: simulates offline device catching up with backlog.

---

## Priority Preservation

**Invariant**: `fall_warn` (HIGH priority) events never wait behind NORMAL events on the same worker.

### Guarantee

- HIGH `fall_warn` events are drained **before** NORMAL events at every stage:
  - Ingestion queue: HIGH lane first.
  - Worker input queue: HIGH lane first.
- Full NORMAL lane still backpressures ingress (we do not drop events).
- Full HIGH lane backpressures ingress only under adversarial flood (rare).

### Implementation (May Change)

- `ingestion.queue.PriorityEventQueue` maintains two queues:
  - `high_queue` (bounded at `HIGH_QUEUE_MAX_SIZE`, default 100,000)
  - `normal_queue` (bounded at `NORMAL_QUEUE_MAX_SIZE`, default 500,000)
- `get()` always drains high lane first (then normal).
- Worker pool routes HIGH events to worker's high lane, NORMAL to normal lane.

 Good (priority-aware):
```python
# Router loop: always prefer HIGH
while True:
    try:
        event = await self.event_queue.get()  # returns HIGH first
        worker_id = hash(event.device_id) % self.worker_count
        await self.workers[worker_id].enqueue(event)  # uses internal priority queue
    except StopIteration:
        break
```

### No Priority Inversion

The key: HIGH events do not wait for NORMAL work. This is critical for alarm latency SLA (p95 < 1s).

---

## Handler Failure Isolation

**Invariant**: If one handler fails, remaining handlers still run. Event remains durable.

### Guarantee

- Event is persisted to SQLite **before** any handler runs.
- If handler A fails (throws exception), handlers B, C, D still execute.
- Worker loop continues; failure does not stop event processing.
- Failures are logged with context (event ID, device, handler name, exception).

### Implementation (May Change)

```python
async def handle_event(self, event: Event) -> None:
    """Apply all handlers to event; continue on failure."""
    handlers = self.get_handlers_for_type(event.type)
    
    for handler in handlers:
        try:
            await handler.handle(event)
        except Exception as e:
            logger.error(
                f"Handler {handler.__class__.__name__} failed for event {event.id}",
                exc_info=True,
                extra={"device_id": event.device_id, "event_type": event.type}
            )
            # Continue to next handler
```

 Good (isolated failure):
```python
# Multiple handlers, one fails
for handler in handlers:
    try:
        handler.handle(event)  # may fail
    except Exception:
        logger.error(...)
        # continue

# Event is still durable; can be retried later
```

 Bad (failure stops processing):
```python
# If first handler fails, rest don't run
for handler in handlers:
    handler.handle(event)  # exception bubbles up; rest skipped
```

### Recovery Property

Because events are durable (persisted first), a handler failure during live processing is not a data loss. On restart, events are replayed and handlers run again (idempotently). This is why idempotence is so critical.

---

## Dedup Semantics

**Invariant**: Dedup must use SQLite as the authoritative source, not Redis cache. This ensures correctness during recovery and after crashes.

### The Dedup Key

**Implementation** (may change):
- Key: `sha256(device_id + room_id + truncated_ts_to_second)`.
- Truncate `ts` to the nearest second to allow late arrivals within the same second to dedupe.
- Example: events at `10:00:00.100` and `10:00:00.900` from the same device, same room, dedupe to the same key.

### Authoritative Source

- SQLite `fall_warnings` table has a `UNIQUE(dedup_key)` constraint.
- **First insert wins**: `INSERT OR IGNORE INTO fall_warnings (dedup_key, ...) VALUES (...)`.
- If the insert succeeds, it's a new alarm. If it conflicts, it's a duplicate.
- Redis is a non-gating cache (logged and ignored if unavailable); SQLite is the source of truth.

 Good (authoritative):
```python
# SQLite is source of truth
try:
    inserted_id = await db.insert_alarm(
        dedup_key=dedup_key,
        device_id=event.device_id,
        ...
    )
except sqlite3.IntegrityError:
    logger.info(f"Dedup: alarm {dedup_key} already exists")
    return  # Duplicate; don't publish again

# Redis is cache only (optional)
try:
    redis.setex(f"dedup:{dedup_key}", 60, "1")
except Exception:
    logger.warning("Redis dedup cache unavailable; continuing with SQLite")
    # SQLite result stands; no failure
```

### Live vs Recovery Replay

- **Live path** (event arriving in real-time): If dedup key exists, it's a real duplicate; suppress and count as `dedup`.
- **Recovery path** (replaying persisted event): If dedup key exists, it's an expected re-apply (already persisted during live or previous recovery); count as `db_conflict`, not `dedup`.

Metrics distinguish these:
- `alarms_dedup`: live duplicates suppressed.
- `recovery_db_conflicts`: expected conflicts during replay.

### Test Coverage

See `tests/test_processing.py`:
- `test_duplicate_fall_alarm_within_window_suppressed()`: same device/room/ts window  one alarm.
- `test_duplicate_fall_alarm_across_window_boundary_allowed()`: different seconds  two alarms.
- `test_recovery_replay_dedup_key_counted_as_conflict_not_dedup()`: recovery re-applies with `db_conflict` metric.

---

## Queuing and Backpressure

**Invariant**: Events are never silently dropped. If queue is full, the response is delayed (backpressure).

### Behavior

- Event arrives at ingress (POST /events).
- Validated and persisted to SQLite immediately.
- Enqueued to priority queue.
- If normal lane is full: `put()` awaits capacity (response 202 is delayed).
- If high lane is full (rare, only under adversarial flood): `put()` awaits (response 202 is delayed).
- On response 202: event is guaranteed durable and queued (client does not retry).

 Good (backpressure, no drop):
```python
@app.post("/events")
async def ingest_event(request: Request):
    """Ingest event (may be delayed if queue is full)."""
    # Validate
    validated = validate_raw_event(body)
    
    # Persist (before ack)
    await db.insert_event(validated)
    
    # Enqueue (may await if queue is full)
    await event_queue.put(validated)  # blocks if needed
    
    # Return 202 (event is durable and queued)
    return {"status": "ok"}, 202
```

---

## Worker Routing and Consistency

**Invariant**: All events from a device go to the same worker. This maintains per-device ordering within the reorder window.

### Implementation (May Change)

- Router hashes `device_id`: `worker_id = sha256(device_id) first byte mod WORKER_COUNT`.
- Default `WORKER_COUNT = 8`.
- All events for a device land on the same worker.
- Each worker maintains a per-device buffer; events are sorted by `ts` within the reorder window.

### Scaling

If you increase `WORKER_COUNT` (e.g., from 8 to 16), events may hash to different workers on restart. This breaks per-device ordering for devices already ingested. Changing `WORKER_COUNT` is a breaking change; increase it only on a fresh start or with a migration plan.

---

## Summary: Non-Negotiables

1. **Handlers must be idempotent and ts-aware.** No exceptions; this enables recovery.
2. **SQLite is the authoritative dedup source.** Redis cache is optional.
3. **Events are never dropped; backpressure delays responses.** No silent loss.
4. **HIGH priority never waits behind NORMAL.** Alarm latency SLA depends on this.
5. **Handler failures don't stop the worker.** Log and continue.

These invariants are tested and must be preserved across refactors.

