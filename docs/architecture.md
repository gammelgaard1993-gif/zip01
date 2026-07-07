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
2. Stop worker/router/flush tasks.
3. Stop snapshot loop.
4. Close SQLite connection.

## Component Responsibilities

### Ingestion

- `api.routes.events.ingest_event` (primary transport, `POST /events`)
  - Accepts one flat JSON event per request; responds `202 Accepted`.
  - Rejects non-JSON / non-object bodies with `400` (counts `events_rejected_invalid_json`).
  - Delegates acceptance rules to the validator.
  - Applies backpressure through the HTTP response: a full NORMAL lane makes `event_queue.put`
    await, delaying the `202` instead of dropping the event. HIGH `fall_warn` returns immediately.

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
    - `high_queue`: unbounded
    - `normal_queue`: bounded (configurable, default 500,000)
  - `get()` always drains high lane first.

### Processing

- `processing.worker_pool.WorkerPool`
  - Routes each event to a worker by consistent hash of `device_id`
    (`sha256(device_id)` first byte mod `WORKER_COUNT`, default 8), so all of a device's
    events land on one worker.
  - Keeps a per-device reorder buffer that sorts by `ts` before applying handlers.
  - Flushes after the reorder delay (`DEVICE_REORDER_BUFFER_MS`, 100ms).
  - Persists every event to SQLite before handler execution.
  - Isolates handler failures (logs exception and continues).

- Handlers (`processing/handlers/*`)
  - `HeartbeatHandler`: updates device last heartbeat and heartbeat history in Redis.
  - `PresenceHandler`: updates room occupancy transitions and latest state in Redis.
  - `FallWarnHandler`: deduplicates, persists alarms to SQLite, publishes to alarm bus.
  - `GenericEventHandler`: currently no-op; event persistence is already done in worker flow.

- `processing.alarm_bus.AlarmBus`
  - Per-room subscribers with async queues.
  - Per-room reorder buffering before publish.
  - Supports stream consumption used by SSE endpoint.

### API

Routes in `api/routes/*`:

- Event ingestion into the priority queue (`POST /events`)
- Device health from Redis (`/devices/{device_id}/health`)
- Room occupancy from Redis transitions (`/rooms/{room_id}/occupancy`)
- Alarm list from SQLite (`/alarms`)
- Alarm SSE stream from SQLite replay + alarm bus (`/alarms/stream`)
- Metrics counters, queue depth, and alarm p95 latency (`/metrics`)

### Recovery

- `core.recovery.RecoveryManager`
  - Loads latest snapshot from SQLite `state_snapshots`.
  - Clears managed Redis keys and reapplies snapshot.
  - Replays events from SQLite `events` ordered by `ts ASC`.
  - Uses inclusive replay boundary on ingestion order (`received_at >= snapshot_ts`), so late
    events ingested after the snapshot are replayed instead of dropped.
  - Runs periodic snapshot loop.
