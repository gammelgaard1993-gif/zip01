# Event Flow

## End-to-End Path

### Operational Invariants that Govern the Critical Path

The runtime-critical path is the mechanism that satisfies the scoring targets. The key invariants are:

- Persist-before-ack: accepted events are durably written to SQLite before the HTTP response completes, so recovery and no-silent-loss behavior are anchored in admission rather than in later processing.
- Bounded per-device ordering: worker-side buffering reorders events for the same device by `ts` within a short window, preserving correctness for slightly late arrivals without turning the system into an unbounded buffer.
- Staged alarm delivery: alarm latency is measured across the full path from ingestion to SSE fan-out, not only through handler execution.
- Recovery on ingestion order: replay uses the ingestion-order cutoff (`received_at`) so late events are recovered correctly even when their device `ts` predates a snapshot.
- Delay-based backpressure: burst traffic slows `POST /events` rather than dropping valid events, and `fall_warn` traffic is kept isolated from normal traffic.

These invariants connect the scoring targets to the main requirements: correctness of health and occupancy, alarm delivery latency, recovery correctness, and burst-load handling without silent loss.

### 1) Ingestion

- `POST /events` receives one flat JSON event per request (primary path).
- Oversized bodies are rejected with `413` (`{"error": "payload_too_large"}`, counted
  `events_rejected_too_large`) before JSON parsing/validation: the declared `Content-Length` is
  checked as a fast path, then the body is read incrementally (`request.stream()`), aborting as
  soon as the running total exceeds `MAX_EVENT_BYTES` (16 KB) so an oversized body is never fully
  buffered in memory.
- The body is parsed as a JSON object.
- Non-JSON or non-object bodies are rejected with `400` and counted
  (`events_rejected_invalid_json`).
- The validator checks schema and timestamp range, then computes:
  - `late` flag (`ts` older than 30s)
  - queue priority (`fall_warn` -> HIGH, else NORMAL)
- Schema checks include: `type` must be one of the 6 documented event types (unknown types are
  rejected, not accepted at NORMAL priority); the type-specific field (`in_room`, `confidence`,
  `magnitude`, `state`, `rssi`) is required and type-checked (`in_room` must be a real bool, not a
  truthy/falsy string; `confidence` must be a finite number in `[0, 1]`; `magnitude`/`rssi` must be
  finite numbers; `state` must be a non-empty string).
- Schema failures return `400`; clock-skew rejections return `202` with
  `{"status": "rejected", "reason": ...}`.
- Persist-before-ack: a validated event is written to the durable SQLite `events` log **before**
  the `202`, so an acknowledged event survives a crash even though its hot-state handler runs
  later. A storage error returns `503`
  (`{"error": "persist_failed"}`, counted `events_persist_failed`) and the event is neither
  enqueued nor acknowledged, so the client can retry (no false accept, no silent loss).
- Persistence and queue insertion run as one cancellation-shielded admission task. The event is
  registered in the in-flight watermark before persistence and removed after handling (or after a
  pre-persist failure), so client cancellation cannot create a durable-but-unqueued gap.

### 2) Queueing and Backpressure

- `fall_warn` is sent to the bounded high lane (`HIGH_QUEUE_MAX_SIZE`, default 100,000) and
  returns immediately under normal/burst load; the lane awaits capacity only under an adversarial
  `fall_warn` flood and never drops.
- Other types are sent to the bounded normal lane (default 500,000).
- On the HTTP path, a full normal lane makes `event_queue.put` await, so the `202` response is
  delayed until capacity frees up; the event is never dropped. A full lane also increments
  `queue_pressure`. HIGH `fall_warn` is not stalled behind NORMAL (no priority inversion); a
  saturated HIGH lane backpressures rather than dropping.
- An idle `get()` waits on a shared availability signal and rechecks HIGH before NORMAL. Cancelling
  that wait cannot remove or strand a later event from either lane.

### 3) Worker Routing and Ordering

- Router loop pops from the priority queue, preferring the high lane.
- Event is assigned to a worker by `sha256(device_id)` first byte mod `WORKER_COUNT`.
- Each worker owns a bounded two-lane priority queue (HIGH drained before NORMAL), so a HIGH
  `fall_warn` never waits behind a NORMAL backlog already routed to that worker, and the lane
  cannot grow unbounded (a full NORMAL lane backpressures the router).
- Worker stores events in a per-device buffer and sorts by event `ts`.
- Flush task waits the reorder window (`DEVICE_REORDER_BUFFER_MS`, 10ms), then processes oldest first.
- **Ordering contract is bounded, not unconditional**: strict `ts` apply-order only holds for
  events that arrive within the same `DEVICE_REORDER_BUFFER_MS` window. An event arriving after
  its device's buffer already flushed (arbitrarily late, e.g. an offline device catching up) is
  applied later than chronologically-newer events already handled. Nothing is lost or dropped in
  this case; ts-aware, idempotent handlers keep the derived aggregate correct regardless of apply
  order. Heartbeat advances only for a newer `ts`; presence also resolves equal timestamps with a
  deterministic tie-breaker. The durable
  SQLite log still has both rows. Guaranteeing strict order for unbounded lateness would require
  unbounded per-device buffering, which is not compatible with real-time alarm latency.

### 4) Persistence and Handler Dispatch

For each flushed event:

### 4) Handler Dispatch

Durability already happened at admission (step 1), so the worker owns only the derived hot state.
For each flushed event:

1. Dispatch to type-specific handler.
2. On handler error, log and continue processing remaining events (the durable record is safe).

### 5) Type-specific State Effects

- `heartbeat`
  - Set `device:{id}:last_heartbeat` if event is newer.
  - Append timestamp into `device:{id}:heartbeats` zset.
  - Trim zset to configured window.

- `presence`
  - `WATCH` the room presence hash and occupancy zset, recompute from fresh values, then update
    both atomically with `MULTI`/`EXEC`.
  - Keep one canonical transition per timestamp. Newer timestamps win; equal timestamps use the
    maximum `device_id:in_room` tie-breaker in both the hash and transition member.
  - On `WatchError`, retry and increment `presence_watch_conflicts`.
  - Trim strictly below the selected pre-window anchor using an exclusive score boundary, so the
    occupancy API can recover the state at the start of longer windows.

- `fall_warn`
  - Build dedup key from device, room, and second-truncated timestamp.
  - Insert into SQLite `fall_warnings` first (`INSERT OR IGNORE`, `UNIQUE(dedup_key)` is the
    authoritative reservation) and commit; this determines new-vs-duplicate, not Redis.
  - If new: best-effort write a Redis dedup key with TTL as a non-gating cache (a Redis outage
    here is logged and ignored), then publish the alarm with its durable `fall_warning_id` to the
    alarm bus, then stamp `published_at` on the durable row. The `UPDATE published_at` is routed
    through `BatchedSQLiteWriter.submit()` (when the writer is set) so the commit does not block
    the event loop between publish and SSE delivery.
  - If the insert conflicts (`dedup_key` already present), read `id` and `published_at` from the
    durable row:
    - If `published_at IS NULL`: republish once with the durable row's `id` and stamp
      `published_at` (guarded by `published_at IS NULL`; also routed through
      `BatchedSQLiteWriter` when set).
    - If `published_at` is not null: do not republish; count as dedup on the live path, or as DB
      conflict during replay.

- `motion`, `sleep_state`, `net_status`
  - No additional hot-state aggregation in handler.
  - Event already durable through global event log insertion.

## Alarm Delivery Path

1. `fall_warn` accepted by handler.
2. Alarm persisted to SQLite.
3. Alarm published to in-memory room buffer in alarm bus.
4. Alarm bus dispatches after reorder delay (`ALARM_REORDER_BUFFER_MS`, 10ms) to each
   subscriber queue (bounded, `SSE_SUBSCRIBER_QUEUE_MAX_SIZE`). A subscriber whose queue is full
   (stalled/slow client) is evicted (`sse_subscribers_evicted`) instead of blocking dispatch to
   the room's other subscribers; once drained, that client's stream closes and it must reconnect
   with `since` to resume without a gap.
5. At dispatch, the alarm bus records feed latency from `received_at` (so it is measured even
   with no stream client connected).
6. `/alarms/stream` calls `alarm_bus.subscribe(room_id)` in the route handler body, **before**
   `return StreamingResponse(...)`. This ensures the subscriber queue is registered before HTTP
   200 headers are sent — the `event_generator` coroutine only executes after Starlette has
   committed those headers, so any subscription placed inside the generator body would arrive
   after the event loop has already processed a backlog of pending I/O (100–300ms under load),
   past the 100ms `_dispatch_room` window. `subscribe()` also replays any alarms already held
   in `_room_buffers[room_id]` into the new queue under the same lock, so an alarm published
   between `subscribe()` and the `_dispatch_room` snapshot is not silently dropped.
7. After subscribing, the route captures a durable high-water ID and replays matching alarms
   from SQLite in bounded `(ts, id)` batches.
8. Buffered alarms already covered by replay are suppressed by durable ID; newer buffered alarms
   are then yielded as SSE `data:` frames, so the replay/live boundary has no gap.

## Alarm History Read Path

1. `/alarms` validates `since` and captures the maximum matching SQLite row ID.
2. Matching rows through that boundary are read in bounded `(ts, id)` keyset batches.
3. The existing `{"alarms": [...], "since": ...}` JSON shape is streamed incrementally while
  still returning every matching persisted alarm; there is no silent hard-limit truncation.
4. Alarms inserted after the captured boundary remain retrievable in the next request.

## Recovery Path

1. Startup loads latest snapshot from SQLite.
2. Managed Redis keys are cleared.
3. Snapshot data is reapplied to Redis.
  Snapshots containing legacy presence state without tie-break metadata are rejected instead,
  including their cutoff, and recovery starts from the full durable log.
4. Events are replayed from SQLite in timestamp order.
5. Replay cutoff is on `received_at` (ingestion order), inclusive of the snapshot timestamp, so
   late events ingested after the snapshot are not dropped.
6. Timestamp-aware handlers prevent stale overwrite; deterministic presence ties make replay
  converge even when SQLite rows share the same `ts`.

## Failure and Edge Behavior

- Invalid JSON/schema: reject event, increment reject counters.
- Clock skew outside +/-1 hour: reject as skew.
- Queue saturation: the `POST /events` response is delayed instead of dropping; HIGH `fall_warn`
  events keep flowing.
- Worker handler exception: event loop continues.
- Duplicate fall alarm with an already-published durable row: suppressed.
- Recovery encounter of a durable row with `published_at IS NULL`: republished once, then marked
  published to prevent repeat republish loops.
- Redis cold start with warm SQLite: recovery reconstructs managed hot state.
