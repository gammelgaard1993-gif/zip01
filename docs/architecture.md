# Architecture

## Summary

zip01 is a layered backend for high-volume sensor events.

1. HTTP ingestion (`POST /events`) receives device events; the reference generator posts one
   flat JSON event per request.
2. Validation enforces schema and clock-skew constraints.
3. A two-lane in-process queue prioritizes `fall_warn` events.
4. A worker pool routes events to type-specific handlers by device.
5. Redis stores hot operational state.
6. SQLite stores durable event history and alarms.
7. FastAPI serves read APIs and SSE alarm streams (service listens on `:8080`).
8. Recovery restores hot state from snapshot + event replay.

## Operational Invariants

The runtime-critical path should be read as a sequence of invariants rather than as a set of loosely related modules. These invariants are the bridge between the scoring targets in the challenge contract and the main functional and non-functional requirements:

1. Persist-before-ack: every accepted event is durably recorded in SQLite before the HTTP response is finalized. This is the basis for restart/recovery correctness and for the no-silent-loss expectation under burst load.
2. Bounded per-device ordering: events for the same device are processed in `ts` order within the worker reorder window. This preserves correctness for late arrivals without requiring unbounded buffering.
3. Staged alarm delivery: alarm publication spans durable persistence, worker buffering, room-level buffering, and SSE fan-out. The 1-second p95 alarm target applies to the full path, not only handler completion.
4. Recovery on ingestion order: replay uses the ingestion-order cutoff (`received_at`) so late events ingested after a snapshot are replayed correctly even when their device `ts` predates the snapshot.
5. Delay-based backpressure: burst traffic may slow `POST /events`, but the system must not silently drop valid events; high-priority `fall_warn` traffic is isolated from normal traffic and only backpressures when its own lane saturates.

Taken together, these invariants explain why the challenge is scored on the full path from ingestion to alarm delivery and recovery. The core functional requirements—device health, room occupancy, alarms, and recovery—are all exercised through this path, while the non-functional requirements show up as durability, bounded latency, deterministic ordering, and explicit overload behavior.

### Runtime Path at a Glance

```text
POST /events
  └─ validate ──> persist to SQLite ──> enqueue (HIGH/NORMAL)
       └─ worker router (device-hash) ──> per-device reorder buffer (100ms)
            └─ handlers (Redis hot state + alarm publication)
                 └─ per-room alarm buffer (100ms) ──> SSE subscribers / replay
```

## Runtime Composition

App initialization (`api/app.py`) creates shared singletons on `app.state`:

- `db_connection` (SQLite)
- `redis_client` (Redis)
- `alarm_bus` (in-memory pub/sub)
- `event_queue` (high + normal lanes)
- `worker_pool`
- `recovery_manager`

Startup sequence:

1. Initialize DB and Redis clients.
2. Create alarm bus, queue, and worker pool.
3. Run state restoration (`restore_state`).
4. Start periodic snapshots.
5. Start async worker pool.

The primary ingestion path is the `POST /events` route, always active.

Shutdown sequence:

1. Gracefully drain the worker pool: let the router move the global queue into worker lanes and
   let workers finish buffered flushes, bounded by a timeout (enqueued events are already durable,
   so anything left at the deadline is replayed on restart).
2. Stop the snapshot loop and write a final snapshot of the drained hot state.
3. Close SQLite connection.

## Component Responsibilities

### Ingestion

- `api.routes.events.ingest_event` (primary transport, `POST /events`)
  - Accepts one flat JSON event per request; responds `202 Accepted`.
  - Rejects oversized bodies with `413` (`{"error": "payload_too_large"}`, counts
    `events_rejected_too_large`) before parsing: checks the declared `Content-Length`, then the
    actual byte length, against `MAX_EVENT_BYTES` (16 KB).
  - Rejects non-JSON / non-object bodies with `400` (counts `events_rejected_invalid_json`).
  - Delegates acceptance rules to the validator.
  - Persist-before-ack: writes the durable `events` row before the `202`; a storage error returns
    `503` (`{"error": "persist_failed"}`) and the event is neither enqueued nor accepted.
  - Treats persistence and queue insertion as one cancellation-shielded admission operation. If
    the client disconnects after persistence begins, admission finishes before cancellation is
    propagated, so a durable event is never abandoned between SQLite and the queue.
  - Applies backpressure through the HTTP response: a full NORMAL lane makes `event_queue.put`
    await, delaying the `202` instead of dropping the event. HIGH `fall_warn` returns immediately
    under normal/burst load; only a saturated HIGH lane (`HIGH_QUEUE_MAX_SIZE`) backpressures, and
    it never drops `fall_warn`.

- `ingestion.validator.validate_raw_event`
  - Verifies required keys and value types.
  - Converts timestamp to UTC datetime.
  - Rejects events outside +/-1 hour.
  - Marks late events older than 30 seconds.
  - Assigns priority (`fall_warn` high, others normal).

- `ingestion.queue.PriorityEventQueue`
  - Maintains two queues:
    - `high_queue`: bounded (`HIGH_QUEUE_MAX_SIZE`, default 100,000). Sized far above any real
      `fall_warn` burst; `put()` awaits capacity only under an adversarial flood and never drops.
    - `normal_queue`: bounded (configurable, default 500,000)
  - `get()` always drains high lane first.
  - Uses a shared availability signal rather than competing lane getter tasks. Cancelling an idle
    `get()` cannot consume or strand an event, and each wake rechecks HIGH before NORMAL.

### Processing

- `processing.worker_pool.WorkerPool`
  - Routes each event to a worker by consistent hash of `device_id`
    (`sha256(device_id)` first byte mod `WORKER_COUNT`, default 8), so all of a device's
    events land on one worker.
  - Each worker owns a bounded two-lane priority queue (`WORKER_NORMAL_QUEUE_MAX_SIZE`): HIGH
    (`fall_warn`) is drained before NORMAL, so downstream routing preserves priority and cannot
    grow an unbounded FIFO; a full worker NORMAL lane backpressures the router (and thus ingress).
  - Keeps a per-device reorder buffer that sorts by `ts` before applying handlers.
  - Flushes after the reorder delay (`DEVICE_REORDER_BUFFER_MS`, 100ms).
  - Ordering guarantee is bounded to that window: an event arriving after its device's buffer
    already flushed is applied out of `ts` order relative to already-handled events (never
    dropped). Correctness of derived state is preserved by ts-aware, idempotent handlers rather
    than by strict apply order (see `tests/test_ordering.py`).
  - Runs hot-state handlers only; durability is owned by admission (the event is persisted to
    SQLite before the `202` response), so a handler failure never risks the durable record.
  - Tracks each admitted event by `received_at` until handler completion (including isolated
    failure); the oldest in-flight value protects the snapshot replay cutoff.
  - Isolates handler failures (logs exception and continues).

- Handlers (`processing/handlers/*`)
  - `HeartbeatHandler`: updates device last heartbeat and heartbeat history in Redis.
  - `PresenceHandler`: atomically updates room occupancy transitions and latest state with Redis
    `WATCH`/`MULTI`. Equal timestamps converge by a deterministic `device_id:in_room` tie-breaker;
    conflicts retry and increment `presence_watch_conflicts`. Trimming preserves exactly one
    pre-window anchor with an exclusive score boundary.
  - `FallWarnHandler`: deduplicates and persists alarms to SQLite first, publishes alarms, and
    stamps `published_at` so conflict-path replay is idempotent (republish only when a durable
    row exists with `published_at IS NULL`). Both `UPDATE published_at` statements route through
    `BatchedSQLiteWriter.submit()` when `self._writer` is set, keeping the commit off the asyncio
    event loop so `AlarmBus._dispatch_room` tasks are not stalled before SSE delivery.
  - `GenericEventHandler`: no-op beyond persistence (already done in worker flow); handles
    `motion`, `sleep_state`, `net_status`, and is the fallback for any unmapped event type.

- `processing.alarm_bus.AlarmBus`
  - Per-room subscribers with async queues.
  - Per-room reorder buffering before publish (`ALARM_REORDER_BUFFER_MS`, 100ms).
  - `subscribe(room_id)` registers the new queue and, under the same lock, replays any alarms
    already held in `_room_buffers[room_id]` into it, so a subscriber arriving after `publish()`
    but before the `_dispatch_room` snapshot does not miss in-flight alarms.
  - Supports stream consumption used by SSE endpoint.

### API

Routes in `api/routes/*`:

- Event ingestion into the priority queue (`POST /events`)
- Device health from Redis (`/devices/{device_id}/health`)
- Room occupancy from Redis transitions (`/rooms/{room_id}/occupancy`)
- Complete alarm list from bounded SQLite keyset batches (`/alarms`)
- Gap-free alarm SSE stream (`/alarms/stream`): `alarm_bus.subscribe(room_id)` is called in
  the route handler body before `return StreamingResponse(...)`, so the subscriber queue is
  registered before HTTP 200 headers are sent and before the event loop begins executing the
  generator; SQLite replay then follows, with alarm bus overlap suppression for the boundary
- Metrics counters, queue depth, and alarm p95 latency (`/metrics`)

### Recovery

- `core.recovery.RecoveryManager`
  - Loads latest snapshot from SQLite `state_snapshots`.
  - Rejects legacy presence snapshots without tie-break metadata and discards their cutoff, then
    performs a full durable-log replay so equal-timestamp state remains deterministic.
  - Clears managed Redis keys and reapplies snapshot.
  - Replays events from SQLite `events` ordered by `ts ASC`.
  - Uses inclusive replay boundary on ingestion order (`received_at >= snapshot_ts`), so late
    events ingested after the snapshot are replayed instead of dropped.
  - Runs periodic snapshot loop.
