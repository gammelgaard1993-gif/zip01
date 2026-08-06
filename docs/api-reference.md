# API Reference

All routes are registered in `api/app.py` and implemented in `api/routes/*`. The service listens
on `:8080` by default (`config.py`, overridable via the `PORT` env var).

## POST /events

Source: `api/routes/events.py` (primary ingestion transport)

Body:

- One flat JSON event: `device_id`, `room_id`, `type`, `ts` (ISO 8601), optional `seq`, plus the
  required type-specific field: `in_room` (bool, presence), `magnitude` (finite number, motion),
  `state` (non-empty string, sleep_state), `confidence` (finite number in `[0, 1]`, fall_warn),
  `rssi` (finite number, net_status); `heartbeat` carries none. `type` must be one of these 6
  documented values.

Responses:

- `202 Accepted` — `{"status": "accepted"}` once validated and enqueued.
- `202 Accepted` — `{"status": "rejected", "reason": "clock_skew_future|clock_skew_past"}` when
  the timestamp is outside +/-1 hour (received but not enqueued).
- `413 Payload Too Large` — `{"error": "payload_too_large"}` when the request body exceeds the
  16 KB (`MAX_EVENT_BYTES`) limit (counts `events_rejected_too_large`).
- `400 Bad Request` — `{"error": "invalid_json"}` for non-JSON / non-object bodies.
- `400 Bad Request` — `{"error": "<reason>"}` for schema failures.
- `503 Service Unavailable` — `{"error": "persist_failed"}` when the durable write fails; the
  event is not enqueued or acknowledged (counts `events_persist_failed`).

Behavior:

- Enforces a 16 KB request size limit (`MAX_EVENT_BYTES`) before validation: checks the declared
  `Content-Length` header as a fast path, then reads the body incrementally and aborts with `413`
  as soon as the running total exceeds the cap, so an oversized body (including one with a
  missing/lying `Content-Length` or chunked transfer-encoding) is never fully buffered in memory.
- Validates via `ingestion.validator.validate_raw_event`.
- Persist-before-ack: writes the durable `events` row before returning `202`, so an accepted
  event survives a crash. On a storage error the response is `503` and the event is not enqueued.
- Enqueues to the HIGH lane (`fall_warn`) or the bounded NORMAL lane.
- Backpressure: a full NORMAL lane delays the response (`await event_queue.put`) rather than
  dropping; HIGH returns immediately under normal/burst load and awaits capacity only when the
  bounded HIGH lane is saturated (never drops `fall_warn`). A full NORMAL lane increments
  `queue_pressure`.

## GET /devices/{device_id}/health

Source: `api/routes/health.py`

Returns:

- `device_id`
- `last_heartbeat_ts` (ISO timestamp string)
- `availability_5m` (0.0 to 1.0)

Behavior:

- Reads `device:{id}:last_heartbeat` from Redis.
- Computes availability from the heartbeat zset count over the 5-minute window
  (`count / 300`, clamped to 1.0).
- Returns `404` when no heartbeat key exists.

## GET /rooms/{room_id}/occupancy?window={1m|5m|1h|Nm|Nh|N}

Source: `api/routes/occupancy.py`

Returns:

- `in_room` (bool) — latest presence state from the `room:{id}:presence` hash.
- `occupied_pct` (0.0 to 1.0) — fraction of the window the room was occupied.
- `window_seconds` (int) — the resolved window length in seconds.

Behavior:

- `window` accepts `Nm` (minutes), `Nh` (hours), or bare `N` (seconds); default `5m`. Invalid or
  non-positive windows return `400`.
- Reads the transition zset within the requested duration.
- Seeds the initial state from the most recent transition at/before the window start (the
  preserved pre-window anchor), so a room occupied since before the window reports correctly.
- Replays transitions in `ts` order to accumulate occupied seconds.

## GET /alarms?since=<epoch>&room_id=<id>

Source: `api/routes/alarms.py`

Returns:

- `alarms`: list of persisted fall warnings (`device_id`, `room_id`, `ts`, `confidence`,
  `received_at`), ordered by `ts ASC`.
- `since`: the epoch value echoed back.

Responses:

- `400 Bad Request` — `{"detail": "invalid since"}` when `since` is NaN, infinite, negative, or
  out of range (greater than now + 1 hour, or otherwise unrepresentable as a timestamp).

Behavior:

- `since` is a float Unix epoch (default `0.0` = full history), converted to a UTC ISO string and
  compared as `ts >= since`. It is validated to be finite and within `0.0 <= since <= now + 1h`
  before use.
- Optional `room_id` applies an exact room filter.
- Returns the complete matching history in the existing JSON shape while reading SQLite in
  bounded keyset batches (`ALARM_REPLAY_BATCH_SIZE`, default 500), ordered by `(ts, id)`.
- Captures a high-water row ID when response streaming begins. Rows inserted afterward are not
  mixed into that response and remain retrievable on the next request; no hard limit truncates
  persisted alarms.

## GET /alarms/stream?room_id=<id>&since=<iso>

Source: `api/routes/alarms.py`

Media type:

- `text/event-stream`

Responses:

- `400 Bad Request` — `{"detail": "invalid since"}` when `since` is present but empty or not a
  valid ISO timestamp.

Behavior:

- Subscribes to the room queue before replay so alarms published during historical catch-up are
  buffered rather than missed.
- If `since` is provided, captures a durable high-water row ID and replays matching SQLite rows
  through that boundary in bounded `(ts, id)` batches.
- Suppresses buffered overlap already included in the replay, then streams new alarms. Durable
  `fall_warning_id` values make the replay/live handoff gap-free without duplicate delivery.
- Feed latency is observed centrally in the alarm bus at dispatch time (baseline `received_at`),
  so `alarm_feed_latency_ms_p95` is measured even when no stream client is connected; the stream
  itself only delivers frames.
- Unsubscribes subscriber queue on stream termination.
- Each subscriber's queue is bounded (`SSE_SUBSCRIBER_QUEUE_MAX_SIZE`). A stalled/slow client that
  doesn't drain fast enough is evicted (counts `sse_subscribers_evicted`) rather than blocking
  delivery to other subscribers in the room; once its queue is drained the connection closes, and
  the client must reconnect with `since=<last seen ts>` to resume without a gap.

## GET /metrics

Source: `api/routes/metrics.py`

Returns a `counters` object with runtime metrics, including:

- Ingestion: `events_ingested_total`, `events_late`
- Rejections: `events_rejected_too_large`, `events_rejected_invalid_json`,
  `events_rejected_invalid_schema`, `events_rejected_clock_skew`,
  `events_rejected_clock_skew_future`, `events_rejected_clock_skew_past`
- Fall handling: `fall_warnings_total`, `fall_warnings_deduped`, `fall_warnings_db_conflicts`
- Backpressure: `queue_pressure`, `queue_depth_high`, `queue_depth_normal`
- Latency: `alarm_feed_latency_ms_p95`
