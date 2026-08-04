# Critical Functions

This document describes high-impact functions and classes: purpose, input/output, side effects, and failure behavior.

## Validation and Ingestion

### `ingestion.validator.validate_raw_event(raw)`

Purpose:
- Convert raw event dictionaries into typed `ValidatedEvent` objects and enforce acceptance rules.

Inputs:
- `raw`: a flat event dict with envelope keys `device_id`, `room_id`, `type`, `ts` (required)
  and optional `seq`. All other top-level keys (e.g. `in_room`, `magnitude`, `state`,
  `confidence`, `rssi`) are collected into `payload`; there is no nested `payload` on the wire.

Outputs:
- `ValidatedEvent` with UTC `ts`, derived `payload`, `late`, `priority`, `received_at`, `seq`.

Side effects:
- Increments `events_late` metric when the event is late (`ts` older than 30s).

Failure behavior:
- Raises `ValidationError` for schema issues, invalid timestamp parse, or clock skew beyond
  +/-1 hour. `seq` is diagnostics-only and is not used for ordering.

### `api.routes.events.ingest_event(request, response)` (primary transport)

Purpose:
- Accept `POST /events`, validate, and enqueue onto the priority queue.

Inputs:
- HTTP request body: one flat JSON event.

Outputs:
- `202 Accepted` (`{"status": "accepted"}`) on success; `202` with
  `{"status": "rejected", "reason": ...}` on clock-skew rejects; `413`
  (`{"error": "payload_too_large"}`) on oversized bodies; `400` on invalid JSON or schema;
  `503` (`{"error": "persist_failed"}`) when the durable write fails.

Side effects:
- Rejects bodies over `MAX_EVENT_BYTES` (16 KB) with `413` before parsing (checks declared
  `Content-Length` then actual byte length) and increments `events_rejected_too_large`.
- Persist-before-ack: writes the durable `events` row before returning `202`, so an accepted
  event is crash-safe. A storage error returns `503`, increments `events_persist_failed`, and the
  event is neither enqueued nor accepted.
- Increments `events_ingested_total`, reject counters, and `queue_pressure` when the NORMAL
  lane is full.
- `await event_queue.put(...)` applies backpressure: a full NORMAL lane delays the response;
  HIGH `fall_warn` returns immediately under normal/burst load and awaits capacity only when the
  bounded HIGH lane (`HIGH_QUEUE_MAX_SIZE`) is saturated. Events are never dropped.

### `ingestion.mqtt_subscriber.MQTTSubscriber._on_message(...)` (optional transport, off by default)

Purpose:
- Handle the MQTT message callback and bridge the thread-based MQTT client into the async queue
  when `ENABLE_MQTT=True`.

Inputs:
- MQTT message payload bytes and topic metadata.

Outputs:
- None; enqueues validated event.

Side effects:
- Increments ingest/reject/pressure counters and logs structured ingest/rejection events.
- Hands the event to the event loop without blocking the MQTT delivery thread; a saturated
  normal lane pauses (never drops) NORMAL, while HIGH `fall_warn` events are never stalled.

Failure behavior:
- Rejects and logs invalid payloads/validation failures.
- Raises runtime error only if the async loop was not initialized.

## Queue and Worker Orchestration

### `ingestion.queue.PriorityEventQueue.get()`

Purpose:
- Return next event, preferring high-priority lane.

Behavior:
- If high lane has events, pop high first.
- Otherwise pop normal lane.

Failure behavior:
- Standard async queue wait semantics when no events available.

### `processing.worker_pool.WorkerPool._router_loop()`

Purpose:
- Move events from the global priority queue into worker-specific queues.

Inputs:
- `ValidatedEvent` from the priority queue (HIGH drained before NORMAL).

Outputs:
- Event placed onto the worker queue selected by `_worker_index(device_id)`
  (`sha256(device_id)` first byte mod `WORKER_COUNT`).

Side effects:
- Preserves per-device affinity: all events for a device always route to the same worker, which
  owns that device's reorder buffer.

### `processing.worker_pool.WorkerPool._flush_device_buffer(...)`

Purpose:
- Enforce per-device timestamp ordering and execute handler processing (durability is owned by
  admission, not the worker).

Behavior:
1. Wait reorder buffer duration.
2. Sort buffered events by timestamp.
3. Invoke resolved handler for each event in `ts` order.

Failure behavior:
- Handler exceptions are logged with context and processing continues; the durable record is
  unaffected because it was written at admission.

## Handler Functions

### `processing.handlers.heartbeat.HeartbeatHandler.handle(event)`

Purpose:
- Maintain latest heartbeat and heartbeat history for availability calculations.

Side effects:
- Writes/updates Redis string and zset.
- Trims zset to heartbeat window.

Failure behavior:
- Invalid existing timestamp in Redis is ignored; event still processed.

### `processing.handlers.presence.PresenceHandler.handle(event)`

Purpose:
- Track room occupancy transitions and most recent occupancy state.

Side effects:
- Appends transition into occupancy zset.
- Conditionally updates current presence hash if event is newer.
- Trims transition history to the occupancy window, but preserves the most recent transition at
  or before the window cutoff as an initial-state anchor so the 1h occupancy query can recover
  the room's state at the window start.

Failure behavior:
- Invalid stored timestamp in hash is treated as missing and replaced by newer event.

### `processing.handlers.fall_warn.FallWarnHandler.handle(event)`

Purpose:
- Deduplicate fall warnings, persist unique alarms, publish to live subscribers.

Side effects:
- Writes Redis dedup key with TTL.
- Inserts into SQLite `fall_warnings`.
- Publishes `AlarmEvent` with the committed `fall_warning_id` to `AlarmBus` for replay/live
  overlap reconciliation.
- Updates alarm/dedup/conflict counters.

Failure behavior:
- Duplicate in Redis dedup window returns early (counts `fall_warnings_deduped`).
- SQLite unique conflict after a Redis miss returns early. On the live path this is a real
  post-TTL duplicate and also counts `fall_warnings_deduped`; during recovery replay
  (`replay=True`) it is an expected re-apply and counts `fall_warnings_db_conflicts` instead, so
  the dedup count is never inflated by recovery.

## API Computation Functions

### `api.routes.occupancy.room_occupancy(...)`

Purpose:
- Compute occupancy percentage for selected window from transition history.

Inputs:
- `room_id`, `window` accepting `Nm` (minutes), `Nh` (hours), or bare `N` (seconds); default
  `5m`. Invalid or non-positive windows return `400`.

Outputs:
- Current occupancy state and fractional occupancy for window.

Side effects:
- Read-only against Redis.

Failure behavior:
- If no state exists, returns defaults (`current_occupancy=false`, occupancy based on empty history).
- For the 1h window, the initial state is recovered from the preserved pre-window anchor
  transition, so a room occupied continuously since before the window reports correctly.

### `api.routes.alarms.alarms_stream(...)`

Purpose:
- Stream historical (optional `since`) and live alarms via SSE.

Behavior:
- Subscribes to the room queue before replay begins.
- Captures the room's durable high-water row ID and replays matching rows through it in bounded
  `(ts, id)` batches when `since` is provided.
- Suppresses queued alarms already covered by the replay, then yields newer live alarms.
- Feed latency is recorded centrally in `AlarmBus._dispatch_room` (at dispatch time), so it is
  measured even when no SSE client is connected; the stream itself only delivers frames.

Failure behavior:
- On disconnect/cancellation, unsubscribes cleanly in `finally`.

### `api.routes.alarms.get_alarms(...)`

Purpose:
- Return every persisted alarm matching `since` and optional `room_id` without materializing the
  full SQLite result set in server memory.

Behavior:
- Freezes the result at a matching high-water row ID, reads bounded `(ts, id)` keyset batches, and
  incrementally streams the unchanged `alarms` + `since` JSON response.
- Concurrently inserted rows remain available to a later request instead of being silently
  truncated or inconsistently mixed into the current response.

## Recovery and Snapshots

### `core.recovery.RecoveryManager.restore_state()`

Purpose:
- Reconstruct managed Redis hot state after startup/restart.

Behavior:
1. Load latest snapshot from SQLite.
2. Clear managed Redis keys.
3. Apply snapshot (if present).
4. Replay events from SQLite in ascending timestamp order.

Failure behavior:
- Individual malformed replay rows are skipped; replay continues.

### `core.recovery.RecoveryManager._replay_events(since_ts)`

Purpose:
- Re-run handlers for durable events to rebuild hot state.

Details:
- Uses `received_at >= since_ts` inclusive cutoff (ingestion order, not device `ts`) so a late
  event ingested after the snapshot is replayed rather than silently dropped.
- Reconstructs `ValidatedEvent` with priority derived from event type.

Failure behavior:
- Skips rows failing parse/JSON/schema conversion and continues.
