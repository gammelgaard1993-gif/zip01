# Event Flow

## End-to-End Path

### 1) Ingestion

- `POST /events` receives one flat JSON event per request (primary path); the optional MQTT
  subscriber feeds the same validator + queue when `ENABLE_MQTT=True`.
- The body is parsed as a JSON object.
- Non-JSON or non-object bodies are rejected with `400` and counted
  (`events_rejected_invalid_json`).
- The validator checks schema and timestamp range, then computes:
  - `late` flag (`ts` older than 30s)
  - queue priority (`fall_warn` -> HIGH, else NORMAL)
- Schema failures return `400`; clock-skew rejections return `202` with
  `{"status": "rejected", "reason": ...}`.

### 2) Queueing and Backpressure

- `fall_warn` is sent to the unbounded high lane and returns immediately.
- Other types are sent to the bounded normal lane (default 500,000).
- On the HTTP path, a full normal lane makes `event_queue.put` await, so the `202` response is
  delayed until capacity frees up; the event is never dropped. A full lane also increments
  `queue_pressure`. HIGH `fall_warn` is never stalled behind NORMAL (no priority inversion).
- On the optional MQTT path, the same full lane pauses NORMAL delivery without blocking the
  MQTT thread, so HIGH `fall_warn` keeps flowing.

### 3) Worker Routing and Ordering

- Router loop pops from the priority queue, preferring the high lane.
- Event is assigned to a worker by `sha256(device_id)` first byte mod `WORKER_COUNT`.
- Worker stores events in a per-device buffer and sorts by event `ts`.
- Flush task waits the reorder window (`DEVICE_REORDER_BUFFER_MS`, 100ms), then processes oldest first.

### 4) Persistence and Handler Dispatch

For each flushed event:

1. Persist event row in SQLite `events`.
2. Dispatch to type-specific handler.
3. On handler error, log and continue processing remaining events.

### 5) Type-specific State Effects

- `heartbeat`
  - Set `device:{id}:last_heartbeat` if event is newer.
  - Append timestamp into `device:{id}:heartbeats` zset.
  - Trim zset to configured window.

- `presence`
  - Append transition JSON (`ts`, `in_room`) in `room:{id}:occupancy` zset.
  - Update `room:{id}:presence` hash only if event timestamp is newer.
  - Trim transitions zset to the window but keep one pre-window anchor transition, so longer
    windows (e.g. `1h`) can still recover the room's state at the window start.

- `fall_warn`
  - Build dedup key from device, room, and second-truncated timestamp.
  - `SET NX EX` in Redis for dedup window.
  - If new: insert into SQLite `fall_warnings`, publish to alarm bus.
  - If duplicate in Redis window: count dedup and stop.
  - If Redis misses but SQLite unique key conflicts: on the live path this is a real post-TTL
    duplicate and counts as dedup; during recovery replay it is an expected re-apply and counts
    as a DB conflict instead. Either way, stop.

- `motion`, `sleep_state`, `net_status`
  - No additional hot-state aggregation in handler.
  - Event already durable through global event log insertion.

## Alarm Delivery Path

1. `fall_warn` accepted by handler.
2. Alarm persisted to SQLite.
3. Alarm published to in-memory room buffer in alarm bus.
4. Alarm bus dispatches after reorder delay to each subscriber queue.
5. At dispatch, the alarm bus records feed latency from `received_at` (so it is measured even
   with no stream client connected).
6. `/alarms/stream` yields SSE `data:` frames per alarm.

## Recovery Path

1. Startup loads latest snapshot from SQLite.
2. Managed Redis keys are cleared.
3. Snapshot data is reapplied to Redis.
4. Events are replayed from SQLite in timestamp order.
5. Replay cutoff is on `received_at` (ingestion order), inclusive of the snapshot timestamp, so
   late events ingested after the snapshot are not dropped.
6. Timestamp-aware handlers prevent stale state from overwriting newer state.

## Failure and Edge Behavior

- Invalid JSON/schema: reject event, increment reject counters.
- Clock skew outside +/-1 hour: reject as skew.
- Queue saturation: the `POST /events` response is delayed (or the optional MQTT NORMAL delivery
  is paused) instead of dropping; HIGH `fall_warn` events keep flowing.
- Worker handler exception: event loop continues.
- Duplicate fall alarm in dedup window: suppressed.
- Redis cold start with warm SQLite: recovery reconstructs managed hot state.
