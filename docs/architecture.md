# Architecture

## Summary

zip01 is a layered backend for high-volume sensor events.

1. HTTP ingestion (`POST /events`) receives device events; the reference generator posts one
   flat JSON event per request. An MQTT subscriber is an optional secondary path (off by default).
2. Validation enforces schema and clock-skew constraints.
3. A two-lane in-process queue prioritizes `fall_warn` events.
4. A worker pool routes events to type-specific handlers by device.
5. Redis stores hot operational state.
6. SQLite stores durable event history and alarms.
7. FastAPI serves read APIs and SSE alarm streams (service listens on `:8080`).
8. Recovery restores hot state from snapshot + event replay.

## Runtime Composition

App initialization (`api/app.py`) creates shared singletons on `app.state`:

- `db_connection` (SQLite)
- `redis_client` (Redis)
- `alarm_bus` (in-memory pub/sub)
- `event_queue` (high + normal lanes)
- `worker_pool`
- `mqtt_subscriber` (only when `ENABLE_MQTT=True`; otherwise `None`)
- `recovery_manager`

Startup sequence:

1. Initialize DB and Redis clients.
2. Create alarm bus, queue, and worker pool; create the MQTT subscriber only if `ENABLE_MQTT`.
3. Run state restoration (`restore_state`).
4. Start periodic snapshots.
5. Start the MQTT subscription thread if enabled.
6. Start async worker pool.

The primary ingestion path is the `POST /events` route, always active regardless of MQTT.

Shutdown sequence:

1. Stop the MQTT client loop (if running).
2. Gracefully drain the worker pool: let the router move the global queue into worker lanes and
   let workers finish buffered flushes, bounded by a timeout (enqueued events are already durable,
   so anything left at the deadline is replayed on restart).
3. Stop the snapshot loop and write a final snapshot of the drained hot state.
4. Close SQLite connection.

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
  - Applies backpressure through the HTTP response: a full NORMAL lane makes `event_queue.put`
    await, delaying the `202` instead of dropping the event. HIGH `fall_warn` returns immediately
    under normal/burst load; only a saturated HIGH lane (`HIGH_QUEUE_MAX_SIZE`) backpressures, and
    it never drops `fall_warn`.

- `ingestion.mqtt_subscriber.MQTTSubscriber` (optional secondary transport, off by default)
  - Subscribes to `teton/devices/+/events` at QoS 1.
  - Decodes JSON payload.
  - Validates event shape and timestamp constraints.
  - Enqueues validated events to high or normal lane.
  - Enqueues without blocking the single MQTT delivery thread, so a saturated NORMAL lane never
    stalls HIGH `fall_warn` delivery; backpressure pauses NORMAL and never drops.

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
    SQLite before the `202`/MQTT ack), so a handler failure never risks the durable record.
  - Isolates handler failures (logs exception and continues).

- Handlers (`processing/handlers/*`)
  - `HeartbeatHandler`: updates device last heartbeat and heartbeat history in Redis.
  - `PresenceHandler`: updates room occupancy transitions and latest state in Redis.
  - `FallWarnHandler`: deduplicates, persists alarms to SQLite, and publishes the committed row ID
    with each alarm so API replay can reconcile durable and live delivery.
  - `GenericEventHandler`: no-op beyond persistence (already done in worker flow); handles
    `motion`, `sleep_state`, `net_status`, and is the fallback for any unmapped event type.

- `processing.alarm_bus.AlarmBus`
  - Per-room subscribers with async queues.
  - Per-room reorder buffering before publish.
  - Supports stream consumption used by SSE endpoint.

### API

Routes in `api/routes/*`:

- Event ingestion into the priority queue (`POST /events`)
- Device health from Redis (`/devices/{device_id}/health`)
- Room occupancy from Redis transitions (`/rooms/{room_id}/occupancy`)
- Complete alarm list from bounded SQLite keyset batches (`/alarms`)
- Gap-free alarm SSE stream using subscribe-first SQLite replay + alarm bus overlap suppression
  (`/alarms/stream`)
- Metrics counters, queue depth, and alarm p95 latency (`/metrics`)

### Recovery

- `core.recovery.RecoveryManager`
  - Loads latest snapshot from SQLite `state_snapshots`.
  - Clears managed Redis keys and reapplies snapshot.
  - Replays events from SQLite `events` ordered by `ts ASC`.
  - Uses inclusive replay boundary on ingestion order (`received_at >= snapshot_ts`), so late
    events ingested after the snapshot are replayed instead of dropped.
  - Runs periodic snapshot loop.
