# API Reference

All routes are registered in `api/app.py` and implemented in `api/routes/*`. The service listens
on `:8080` by default (`config.py`, overridable via the `PORT` env var).

## POST /events

Source: `api/routes/events.py` (primary ingestion transport)

Body:
- One flat JSON event: `device_id`, `room_id`, `type`, `ts` (ISO 8601), optional `seq`, plus any
  type-specific fields (`in_room`, `magnitude`, `state`, `confidence`, `rssi`).

Responses:
- `202 Accepted` — `{"status": "accepted"}` once validated and enqueued.
- `202 Accepted` — `{"status": "rejected", "reason": "clock_skew_future|clock_skew_past"}` when
  the timestamp is outside +/-1 hour (received but not enqueued).
- `400 Bad Request` — `{"error": "invalid_json"}` for non-JSON / non-object bodies.
- `400 Bad Request` — `{"error": "<reason>"}` for schema failures.

Behavior:
- Validates via `ingestion.validator.validate_raw_event`.
- Enqueues to the HIGH lane (`fall_warn`) or the bounded NORMAL lane.
- Backpressure: a full NORMAL lane delays the response (`await event_queue.put`) rather than
  dropping; HIGH returns immediately. A full lane increments `queue_pressure`.

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

Behavior:
- `since` is a float Unix epoch (default `0.0` = full history), converted to a UTC ISO string and
  compared as `ts >= since`.
- Optional `room_id` applies an exact room filter.
- Reads from SQLite `fall_warnings`.

## GET /alarms/stream?room_id=<id>&since=<iso>

Source: `api/routes/alarms.py`

Media type:
- `text/event-stream`

Behavior:
- If `since` is provided, replays persisted alarms from SQLite first.
- Subscribes to room queue in alarm bus and streams new alarms.
- Feed latency is observed centrally in the alarm bus at dispatch time (baseline `received_at`),
  so `alarm_feed_latency_ms_p95` is measured even when no stream client is connected; the stream
  itself only delivers frames.
- Unsubscribes subscriber queue on stream termination.

## GET /metrics

Source: `api/routes/metrics.py`

Returns a `counters` object with runtime metrics, including:
- Ingestion: `events_ingested_total`, `events_late`
- Rejections: `events_rejected_invalid_json`, `events_rejected_invalid_schema`,
  `events_rejected_clock_skew`, `events_rejected_clock_skew_future`,
  `events_rejected_clock_skew_past`
- Fall handling: `fall_warnings_total`, `fall_warnings_deduped`, `fall_warnings_db_conflicts`
- Backpressure: `queue_pressure`, `queue_depth_high`, `queue_depth_normal`
- Latency: `alarm_feed_latency_ms_p95`
