# Submission — Teton Real-Time Streaming Backend

## Overview

A five-layer pipeline (ingestion → processing → Redis hot state + SQLite durable
log → API) that ingests ~5k sensor events/sec, prioritises fall warnings, and
serves health, occupancy, and a real-time alarm feed. Redis holds ephemeral
aggregates; SQLite is the append-only source of truth used for restart recovery.

## Key design decisions

- **Dual-lane priority queue.** `fall_warn` uses an unbounded HIGH lane; everything
  else a bounded (500k) NORMAL lane. When NORMAL fills, the MQTT thread blocks on
  enqueue — natural backpressure, never a silent drop.
- **Per-device ordering via consistent hashing.** Events for a device always route
  to the same worker, which buffers briefly (100 ms) and sorts by `ts` before
  applying handlers, so late/out-of-order events correct history deterministically.
- **Timestamp-aware, idempotent handlers.** Heartbeat/presence apply state only when
  newer by `ts`; fall warnings dedup on `SHA256(device+room+second)` (Redis, 10 s TTL)
  with a SQLite `UNIQUE(dedup_key)` backstop. This makes recovery replay safe.
- **Recovery.** Snapshots every 60 s; on cold start the latest snapshot is loaded
  into Redis and only events with `ts >= snapshot_ts` (inclusive — no boundary loss)
  are replayed from SQLite.
- **Alarm latency.** Reorder budget kept to 100 ms (worker) + 100 ms (alarm bus) so
  p95 ingestion-to-SSE delivery stays well under 1 s. Latency is measured from
  server ingestion (`received_at`), documented in `api/routes/alarms.py`.

## Observability

Structured JSON logs on every key path; `GET /metrics` exposes counters
(`events_ingested_total`, `events_rejected_clock_skew`, `events_late`,
`fall_warnings_total`, `fall_warnings_deduped`, `fall_warnings_db_conflicts`,
queue depths, and `alarm_feed_latency_ms_p95`).

## Running

- `make run` — starts Mosquitto + Redis (Docker) and the service.
- `make test` — 37 unit/integration tests (recovery equivalence, exact-ts cutoff,
  offline occupancy backfill, in-process burst p95).
- `DEVICES=500 make burst`, `make offline`, `make smoke` — load/scenario drivers.
- Windows (no make/docker): see README "Running on Windows".

## Known limitations

- The burst test in `tests/` is an in-process surrogate (no broker); true 50k/sec
  validation requires `make burst` against running infra.
- Alarm latency is server-ingestion based by design; for offline replays the device
  `ts` may be much older than delivery time.
