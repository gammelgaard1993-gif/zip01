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
2. Bounded per-device ordering: events for the same device are processed by `ts`, optional `seq`, then durable event ID within the worker reorder window. This preserves deterministic correctness for late arrivals without requiring unbounded buffering.
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
    (`sha256(device_id)` first byte mod `WORKER_COUNT`, default 24), so all of a device's
    events land on one worker.
  - Each worker owns a bounded two-lane priority queue (`WORKER_NORMAL_QUEUE_MAX_SIZE`): HIGH
    (`fall_warn`) is drained before NORMAL, so downstream routing preserves priority and cannot
    grow an unbounded FIFO; a full worker NORMAL lane backpressures the router (and thus ingress).
  - Keeps a per-device reorder buffer that sorts by `ts`, optional `seq`, then durable event ID before applying handlers.
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
    already claimed as `_inflight_batches[room_id]` into it, so a subscriber arriving mid-dispatch
    doesn't miss them. `_dispatch_room` takes its delivery-time subscriber snapshot atomically (in
    the same lock acquisition as marking the batch in-flight), before any reorder-delay sleep, so
    a queue that subscribes afterward is guaranteed to be excluded from that snapshot and only
    ever receives the batch via this replay path -- never both. Deliberately does NOT replay from
    `_room_buffers`: any alarm still sitting there always has a pending dispatch task backing it
    (guaranteed by `publish()`), and that task's own future atomic snapshot will already include a
    subscriber that joined before it runs, so replaying here too would double-deliver. (An earlier
    version of this logic replayed from both `_inflight_batches` and `_room_buffers`, and took the
    delivery snapshot fresh after the reorder-delay sleep -- both were real, reproducible
    double-delivery bugs found via specialist review; see `tests/test_alarms.py`'s
    `AlarmBusCrossInstanceBridgeTests`/late-subscriber regression tests.)
  - Supports stream consumption used by SSE endpoint.
  - **Cross-instance bridge**: when constructed with a redis client (the default in `api/app.py`,
    since Redis is already a required shared dependency), a dispatched batch is always delivered
    to this instance's local subscribers first, synchronously -- identical timing/atomicity to
    the pre-bridge behavior, so `subscribe()`'s in-flight-batch replay handoff is unaffected by
    redis latency. It is then also PUBLISHed to an `alarms:{room_id}` Redis channel, tagged with
    this instance's id, purely so *other* instances' background listener threads (subscribed via
    `psubscribe("alarms:*")`) can relay it to their own local subscribers; a listener ignores
    messages tagged with its own instance id, since that instance already delivered the batch
    directly. This is what lets an SSE client connected to any instance receive alarms processed
    by any other instance, without ever double-delivering to a subscriber on the originating
    instance. `AlarmBus.start()` blocks until its listener thread's `psubscribe` is acknowledged,
    so an instance never begins publishing before it is guaranteed to receive other instances'
    alarms. See "Multi-Instance Considerations" below. Unit tests construct `AlarmBus()` with no
    redis client, which keeps the pre-bridge direct-delivery behavior unchanged.

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
  - Replays events from SQLite `events` ordered by `ts ASC, id ASC`.
  - Uses inclusive replay boundary on ingestion order (`received_at >= snapshot_ts`), so late
    events ingested after the snapshot are replayed instead of dropped.
  - Runs periodic snapshot loop.
  - **Known trade-off**: `snapshot_ts` is pinned to the oldest currently in-flight event's
    `received_at` (never a plain wall-clock time), guaranteeing no admitted-but-unprocessed event
    is ever excluded from replay. Under sustained backlog this can lag "now" by a large margin,
    which inflates (but never breaks) the recovery replay window -- a deliberate
    correctness-over-recovery-time choice, not an oversight. `RecoveryManager.metrics_snapshot()`
    exposes `snapshot_lag_ms` (surfaced on `GET /metrics`) so a growing lag is observable instead
    of assumed bounded; a `snapshot_lag_high` warning logs when it exceeds several snapshot
    intervals.

## Multi-Instance Considerations

This project's reference design (per `REQUIREMENTS.md`) is a single process: one in-process
priority queue, one dedicated SQLite writer thread, one in-memory alarm bus. The notes below
record what's already safe under multiple instances, what required the pub/sub bridge above, and
what would still need attention before running N replicas behind a load balancer -- kept here as
a known-scope record, not as a statement that multi-instance is required or fully implemented.

- **Already safe under concurrent multi-writer access, no changes needed**:
  - `PresenceHandler`'s Redis `WATCH`/`MULTI` optimistic transactions on room presence/occupancy
    are commutative and convergent regardless of which instance or in what order updates land
    (`presence_watch_conflicts` counts retries).
  - `FallWarnHandler` dedup is anchored on SQLite's `dedup_key` UNIQUE constraint -- authoritative
    regardless of which instance's writer thread executes the insert first.
  - `GET /rooms/{room_id}/occupancy` and `GET /devices/{device_id}/health` read directly from
    Redis and are safe under arbitrary (non-sticky) load balancing today.

- **Fixed by the AlarmBus redis pub/sub bridge (see above)**: SSE alarm delivery no longer
  requires a client's stream connection and the alarm's originating instance to be the same
  process.

- **Still requires sticky (consistent-hash by `device_id`/`room_id`) routing at the load balancer
  to keep the *literal* bounded-reorder-window guarantee true fleet-wide**: `DEVICE_REORDER_BUFFER_MS`
  and `ALARM_REORDER_BUFFER_MS` are each a single process's reorder window. Without sticky routing,
  a device's or room's events split across instances would each run an independent 100ms window
  racing the others -- final state still converges correctly (the sinks above are commutative/
  idempotent), but the specific "ordered within 100ms" claim degrades to "eventually consistent,
  order not strictly bounded across instances." Sticky routing is an LB/ops concern, not an
  application change.

- **Not addressed, and would need design work before running multiple durable-writer instances**:
  - SQLite's single-dedicated-writer-thread model doesn't corrupt under N processes writing the
    same file (WAL locking still serializes them correctly), but it doesn't scale with instance
    count either -- more processes means more small, lock-contending commits instead of amortized
    batching, and a real multi-host deployment would need a shared disk with correct POSIX
    advisory locking (most network-mounted filesystems don't provide this). The realistic paths
    are "single writer instance + N stateless reader instances" or replacing SQLite with a store
    built for concurrent multi-writer durability.
  - Periodic snapshot capture (`RecoveryManager.start_snapshot_loop`) is not coordinated across
    instances: each instance snapshots on its own timer against its own in-flight watermark, and
    `state_snapshots` retention (newest N rows) doesn't distinguish which instance wrote a row.
    This is unverified-safe, not proven-safe, under N concurrent snapshotters and would need
    either a designated single snapshot-writer role or a fleet-wide (minimum) watermark.
