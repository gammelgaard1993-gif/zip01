# Teton Real-Time Streaming Backend — Project Requirements & Architecture Guide

> **Purpose:** This document is the single source of truth for building, reviewing, and evaluating the Teton backend challenge solution. Use it as a guide for code review and a reference when prompting AI assistance.
> **Goal:** Maximum score (100/100). Pass bar is 75.

---

## Table of Contents

1. [Scoring Targets](#1-scoring-targets)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Responsibilities](#3-component-responsibilities)
4. [Data Flow Diagrams](#4-data-flow-diagrams)
5. [Storage Design](#5-storage-design)
6. [API Contract](#6-api-contract)
7. [Functional Requirements](#7-functional-requirements)
8. [Non-Functional Requirements](#8-non-functional-requirements)
9. [Edge Cases & Failure Modes](#9-edge-cases--failure-modes)
10. [Observability Requirements](#10-observability-requirements)
11. [Restart & Recovery Checklist](#11-restart--recovery-checklist)

---

## 1. Scoring Targets

| Category | Points | What "full marks" looks like |
|---|---|---|
| Correctness of aggregations under all conditions | 30 | Occupancy windows, device health, fall dedup all correct after burst, offline replay, and late events |
| Behavior under burst load and backpressure | 20 | No silent drops at 10x rate; `fall_warn` prioritized; delay is acceptable |
| Restart / recovery correctness | 15 | State fully restored after hard kill; no alarms missed during gap |
| Alarm feed latency p95 | 15 | New fall warnings reach subscribers within **1 second** at p95 under all load levels |
| Code quality and design clarity | 15 | Clear module boundaries, typed, documented, no magic globals |
| Observability (logs, metrics) | 5 | Structured logs on every key path; counters for ingested, rejected, late, deduped |

---

## 2. Architecture Overview

The only fixed contract is the event schema and the reference generator, which
**POSTs JSON events to `/events` over HTTP**.

The system is composed of five distinct layers, each with a single job and a clear
boundary. The stack below — HTTP ingestion, an in-process priority queue, a hot-state
store, and a durable log.
Use Redis for hot/ephemeral aggregates and SQLite for the durable append-only log;
both sit behind narrow interfaces and can be swapped for any equivalent store. HTTP
`POST /events` is the **primary** transport (what the reference generator uses); an
optional MQTT subscriber can feed the *same* validator and queue as a secondary path.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DEVICE LAYER                                │
│   ~5,000 simulated sensors  ──►  reference event generator          │
│   POSTs one JSON event per request over HTTP                        │
└────────────────────────────┬────────────────────────────────────────┘
                             │  HTTP   POST /events   (JSON)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       INGESTION LAYER                               │
│   /events endpoint  ──►  Validator  ──►  Priority Queue (in-process)│
│                                                                     │
│   • Validates schema and clock skew                                 │
│   • Rejects events > 1h in the future                               │
│   • Accepts events up to 1h in the past                             │
│   • Classifies priority: fall_warn=HIGH, all others=NORMAL          │
│   • Applies backpressure: never drops, may delay NORMAL             │
└────────────────────────────┬────────────────────────────────────────┘
                             │  validated events
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       PROCESSING LAYER                              │
│   Worker Pool  ──►  Event Router  ──►  Handler per event type       │
│                                                                     │
│   Handlers:                                                         │
│   • HeartbeatHandler   → device health + availability window        │
│   • PresenceHandler    → room occupancy state + time-series         │
│   • FallWarnHandler    → dedup → persist → publish to alarm bus     │
│   • MotionHandler      → stored (future use)                        │
│   • SleepStateHandler  → stored (future use)                        │
│   • NetStatusHandler   → stored (future use)                        │
└──────────┬─────────────────────────────┬───────────────────────────┘
           │  hot state writes           │  persistent writes
           ▼                             ▼
┌──────────────────────┐     ┌───────────────────────────────────────┐
│   HOT-STATE STORE    │     │            DURABLE LOG                │
│  (Redis — pluggable) │     │        (SQLite — pluggable)           │
│                      │     │                                       │
│  • Device heartbeat  │     │  • events table (append-only log)     │
│    sorted sets       │     │  • fall_warnings table (deduped)      │
│  • Room presence     │     │  • occupancy_snapshots table          │
│    sorted sets       │     │  • state_snapshots table              │
│  • Fall dedup sets   │     │                                       │
│  • Alarm queues      │     │  Written synchronously on every       │
│    per room          │     │  validated event — survives restart   │
└──────────────────────┘     └───────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                       API LAYER  (FastAPI)                          │
│                                                                     │
│   GET /devices/{device_id}/health          ← reads Redis            │
│   GET /rooms/{room_id}/occupancy?window=   ← reads Redis            │
│   GET /alarms?since=<ts>                   ← reads SQLite           │
│   GET /alarms/stream  (SSE)                ← subscribes alarm bus   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Responsibilities

### 3.1 HTTP Events Endpoint (`api/routes/events.py`) — primary transport

| Responsibility | Detail |
|---|---|
| Accept `POST /events` | One flat JSON event per request; respond `202 Accepted` |
| Parse raw payload | JSON decode, hand to Validator |
| Forward to Validator | Pass raw event dict |
| Apply backpressure | If the NORMAL lane is full, block/slow the response — never drop |

**Must NOT:** perform any aggregation, storage, or business logic.

#### Optional secondary transport — MQTT Subscriber (`ingestion/mqtt_subscriber.py`)

The challenge lets you pick the transport; HTTP is what the reference generator uses.
An optional MQTT subscriber may feed the *same* Validator and Priority Queue for
deployments (config.py - `ENABLE_MQTT`, off by default):

| Responsibility | Detail |
|---|---|
| Connect to Mosquitto broker | Persistent session, QoS 1 (at-least-once delivery) |
| Subscribe to wildcard topic | `teton/devices/+/events` |
| Parse raw payload | JSON decode, schema validation |
| Forward to Validator | Pass raw event dict |

**Must NOT:** perform any aggregation, storage, or business logic.

---

### 3.2 Validator (`ingestion/validator.py`)

| Responsibility | Detail |
|---|---|
| Required envelope | `device_id`, `room_id`, `type`, `ts` (ISO 8601 with ms); `seq` (monotonic per device) is optional — reject if any required key is missing or malformed |
| Per-type fields | Flat, at top level: `in_room` (presence), `magnitude` (motion), `state` (sleep_state), `confidence` (fall_warn), `rssi` (net_status); `heartbeat` has none — **no nested `payload`**. The validator derives an internal `payload` dict from these leftover fields |
| Clock skew — future | Reject if `ts > now + 1 hour` (clearly broken clock); log + count `events_rejected_clock_skew` |
| Clock skew — past | Accept if `ts >= now - 1 hour` (offline buffer replay); flag `late=True` if `ts < now - 30s` (beyond the normal ±30s device drift) |
| Ordering key | `ts` is authoritative for ordering/aggregation; `seq` is captured for diagnostics but may have gaps, so it is **not** relied on for ordering |
| Assign priority | `fall_warn` → `Priority.HIGH`; everything else → `Priority.NORMAL` |
| Output | `ValidatedEvent` dataclass with `late: bool`, `priority: Priority`, parsed `ts: datetime`, `seq`, derived `payload`, and `received_at` |

**Must NOT:** touch Redis, SQLite, or any queue directly.

---

### 3.3 Priority Queue (`ingestion/queue.py`)

| Responsibility | Detail |
|---|---|
| Accept validated events | Non-blocking enqueue |
| Separate lanes | `HIGH` lane (fall_warn) and `NORMAL` lane |
| Backpressure | If NORMAL lane exceeds capacity, block/slow the `POST /events` response (or pause MQTT ACK on the optional broker path); never drop |
| Worker handoff | Workers always drain HIGH lane before NORMAL lane |

**Capacity target:** HIGH lane unbounded; NORMAL lane max 500,000 items (covers 10x burst for 30s).

---

### 3.4 Worker Pool (`processing/worker_pool.py`)

| Responsibility | Detail |
|---|---|
| Drain the priority queue | A single router pulls from the queue (HIGH before NORMAL) and dispatches to N worker tasks (configurable, default 8) |
| Consistent-hash routing | `sha256(device_id) % N` — every event for a device always lands on the same worker |
| Per-device reorder buffer | Each worker buffers a device's events for `DEVICE_REORDER_BUFFER_MS` (100 ms) and sorts by `ts` before applying, so slightly-late arrivals correct into order |
| Persist-before-handle | Every validated event is written to the SQLite `events` log **before** its handler runs, so the durable record survives even if the handler fails |
| Route each event | Call the correct Handler based on `event.type` |
| Error isolation | A single handler failure is logged and skipped; it must not stall the device buffer or kill the worker |

**Why consistent hashing:** all events for `dev_0001` are owned by one worker, which holds that device's reorder buffer and applies events in `ts` order (not arrival order). This enforces per-device ordering without a global lock. Cross-device ordering within a room is not guaranteed by the transport and is reconstructed downstream from `ts`.

---

### 3.5 Handlers (`processing/handlers/`)

Every validated event is already durably recorded in the SQLite `events` log by the
worker's persist-before-handle step (§3.4). Handlers therefore own only the **derived
hot state** in Redis, plus dedup and the alarm bus — never the durable log.

#### HeartbeatHandler
- Write `device:{device_id}:last_heartbeat` = `ts` (Redis String)
- Add `ts` score to `device:{device_id}:heartbeats` (Redis Sorted Set)
- Trim sorted set to last 5 minutes: `ZREMRANGEBYSCORE ... 0 (now - 300)`
- Availability = `count of entries in set / 300` (expected 1 per second)

#### PresenceHandler
- Write `room:{room_id}:presence` = `{in_room, ts}` (Redis Hash) — only if `ts` is newer than current
- Add interval to `room:{room_id}:occupancy` (Redis Sorted Set of `{ts, in_room}` transitions)
- Trim to last 1 hour: `ZREMRANGEBYSCORE ... 0 (now - 3600)`
- Occupancy % computed at query time by replaying transitions within window

#### FallWarnHandler
- Dedup key = `SHA256(device_id + room_id + ts.truncate_to_second)` → Redis Set with 10s TTL
- If key already exists: discard (duplicate), increment dedup counter
- If key is new: SET key → persist to SQLite `fall_warnings` → publish to alarm bus
- Alarm bus = `asyncio.Queue` per `room_id` (in-process, fed to SSE streams)

#### Generic Handler (Motion, SleepState, NetStatus)
- No aggregation is required for scoring, and these types keep no Redis state
- Their durability is already covered by the persist-before-handle write to the SQLite
  `events` log (§3.4), so the handler is effectively a no-op beyond that durable record

---

### 3.6 Alarm Bus (`processing/alarm_bus.py`)

| Responsibility | Detail |
|---|---|
| Per-room queues | `Dict[room_id, asyncio.Queue]` |
| Fan-out | Multiple SSE subscribers on the same room each get their own queue |
| Ordering | Events published in `ts` order within each room |
| Persistence bridge | Alarm bus is in-memory; SQLite is the durable store. On restart, consumers replay from `?since=<ts>` |

---

### 3.7 API Layer (`api/`)

| Responsibility | Detail |
|---|---|
| Serve read endpoints | FastAPI async routes, reads from Redis and SQLite |
| SSE stream endpoint | `GET /alarms/stream?room_id=` — streams from alarm bus |
| No business logic | API routes only read and format; they never write state |
| Input validation | Pydantic models on all query params |

---

### 3.8 Recovery Manager (`core/recovery.py`)

| Responsibility | Detail |
|---|---|
| On startup | Detect if Redis is cold (empty or missing keys) |
| Replay from SQLite | Read `events` ordered by `ts ASC` and re-run through handlers (timestamp-aware + idempotent, so re-applying is safe) |
| Snapshot | Every 60 seconds write current aggregation state to `state_snapshots`, stamped with wall-clock `snapshot_ts` |
| Startup optimization | Load the latest snapshot into Redis, then replay only events with `received_at >= snapshot_ts` |
| Cut off on `received_at`, not `ts` | A late event (old device `ts`, e.g. now−20m) ingested *after* the snapshot has `ts < snapshot_ts` yet must still replay. Filtering on `received_at` (ingestion time) keeps it; the boundary is inclusive so an event received exactly at `snapshot_ts` is never dropped |
| Fall-warn replay safety | `fall_warn` replays in `replay=True` mode: `UNIQUE(dedup_key)` collisions count as `db_conflicts` (not dedups) and never re-publish alarms |

---

## 4. Data Flow Diagrams

### 4.1 Normal Event Flow

```
Device / Event Generator
  │
  │  HTTP  POST /events   (one JSON event)
  │
  │  on request (optional: MQTT subscriber on_message)
  ▼
HTTP /events endpoint
  │
  │  raw dict
  ▼
Validator ──── REJECT ──► log + counter (clock skew, bad schema)
  │
  │  ValidatedEvent(priority, late, ts)
  ▼
Priority Queue
  │  HIGH lane ◄── fall_warn
  │  NORMAL lane ◄── everything else
  ▼
Worker Pool (worker assigned by device_id hash)
  │
  │  buffer 100ms + sort this device's pending events by ts
  ▼
Persist to SQLite events log  (every event — source of truth for recovery)
  │
  ▼
Event Router (by event.type)
  ├──► HeartbeatHandler ──► Redis: last_heartbeat + heartbeats sorted set
  ├──► PresenceHandler  ──► Redis: presence hash + occupancy sorted set
  ├──► FallWarnHandler  ──► dedup check ──► SQLite fall_warnings + Alarm Bus
  └──► GenericHandler   ──► no-op (already in events log)
```

---

### 4.2 Fall Warning Flow (detailed)

```
FallWarnHandler receives event
  │
  ├── Compute dedup_key = SHA256(device_id + room_id + floor(ts, 1s))
  │
  ├── Redis: SETNX dedup:{dedup_key} EX 10
  │         │
  │         ├── 0 (key existed) ──► DUPLICATE — discard, log, increment counter
  │         │
  │         └── 1 (key is new)  ──► ORIGINAL
  │                                   │
  │                                   ├── SQLite: INSERT INTO fall_warnings
  │                                   │   (device_id, room_id, ts, confidence, dedup_key)
  │                                   │
  │                                   └── Alarm Bus: publish to room queue
  │                                         │
  │                                         └── SSE subscribers receive within 1s
```

---

### 4.3 Offline Replay Flow (late events)

```
Device comes back online after 20 min offline
  │
  │  POSTs 1,200 buffered events to /events (all ts 20min ago)
  ▼
HTTP /events endpoint receives burst
  │
  ▼
Validator: ts < now - 30s → flags late=True, still accepts (within 1h window)
  │
  ▼
Priority Queue: fall_warn → HIGH lane, rest → NORMAL lane
  │  (backpressure applied if NORMAL lane fills — slow the HTTP response)
  ▼
Worker for this device_id:
  │  Collects all pending events for device
  │  Sorts by ts ASC before applying
  ▼
Handlers re-apply state in correct chronological order
  │
  ├── HeartbeatHandler: backfills sorted set — availability recalculated correctly
  │
  └── PresenceHandler: inserts transitions at correct ts positions
        Redis sorted set is ordered by score (ts) so insertion is always correct
        Occupancy % query at any point now reflects the replayed history
```

---

### 4.4 Restart & Recovery Flow

```
Hard kill (SIGKILL)
  │
  ▼
Process stops — Redis state lost on restart
  │
  ▼
Service starts up
  │
  ├── Recovery Manager checks SQLite state_snapshots
  │     │
  │     ├── Snapshot found ──► load into Redis (warm up hot state)
  │     │                  ──► replay events WHERE received_at >= snapshot_ts (ORDER BY ts)
  │     │                       (received_at cutoff keeps late events ingested post-snapshot)
  │     │
  │     └── No snapshot    ──► full replay of all events from SQLite events table
  │
  ▼
Redis hot state restored
  │
  ▼
HTTP /events endpoint resumes accepting POSTs
  (optional MQTT path: broker redelivers QoS 1 messages from the downtime)
  │
  ▼
New events processed normally — no gap in state
  │
  ▼
SSE clients reconnect using GET /alarms?since=<last_ts>
  SQLite is the source of truth for historical alarms
```

---

## 5. Storage Design

### 5.1 Redis Key Schema

| Key Pattern | Type | Content | TTL |
|---|---|---|---|
| `device:{id}:last_heartbeat` | String | ISO timestamp | none |
| `device:{id}:heartbeats` | Sorted Set | score=unix_ts, value=ts | trimmed to 5min window |
| `room:{id}:presence` | Hash | `{in_room: bool, ts: str}` | none |
| `room:{id}:occupancy` | Sorted Set | score=unix_ts, value=`{ts,in_room}` | trimmed to 1h window |
| `dedup:{sha256_key}` | String | `"1"` | **10 seconds** |

### 5.2 SQLite Schema

```sql
-- Append-only event log (source of truth for recovery)
CREATE TABLE events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT    NOT NULL,
    room_id     TEXT    NOT NULL,
    type        TEXT    NOT NULL,
    ts          TEXT    NOT NULL,   -- ISO8601, original device timestamp
    payload     TEXT    NOT NULL,   -- full JSON blob
    received_at TEXT    NOT NULL,   -- server ingestion time
    late        INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_events_device_ts ON events(device_id, ts);
CREATE INDEX idx_events_ts        ON events(ts);

-- Deduplicated fall warnings (alarm history)
CREATE TABLE fall_warnings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT    NOT NULL,
    room_id     TEXT    NOT NULL,
    ts          TEXT    NOT NULL,   -- original device timestamp
    confidence  REAL    NOT NULL,
    dedup_key   TEXT    NOT NULL UNIQUE,
    received_at TEXT    NOT NULL
);
CREATE INDEX idx_fall_ts     ON fall_warnings(ts);
CREATE INDEX idx_fall_room   ON fall_warnings(room_id, ts);

-- Periodic aggregation snapshots (speeds up restart recovery)
CREATE TABLE state_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_ts  TEXT    NOT NULL,  -- when snapshot was taken
    state_json   TEXT    NOT NULL   -- full serialized Redis state
);
```

---

## 6. API Contract

> The read-endpoint shapes below match the reference `example_solution` stub, which
> `eval/check.py` reads. The stub is deliberately naive about *values*; we match its
> response **shape** while computing correct aggregations. The service listens on
> **:8080** by default (the generator's default `--target`).

### `POST /events` — ingestion (primary transport)

Accepts one flat JSON event per request from the generator.

**Request body:**
```json
{
  "device_id": "dev_0001",
  "room_id": "room_000",
  "type": "fall_warn",
  "ts": "2026-05-23T18:53:49.123Z",
  "seq": 42,
  "confidence": 0.92
}
```
- Type-specific fields are **top-level**: `in_room` (presence), `magnitude` (motion), `state` (sleep_state), `confidence` (fall_warn), `rssi` (net_status); `heartbeat` carries none.
- **Response:** `202 Accepted` on enqueue. Malformed JSON → `400`. Under load the response is delayed (backpressure) rather than dropping the event.

**Status-code contract**

| Situation | Status | Body |
|---|---|---|
| Valid event enqueued | `202` | `{"status":"accepted"}` |
| Clock-skew reject (>1h future / past) | `202` | `{"status":"rejected","reason":…}` |
| Body not a JSON object / bad JSON | `400` | `{"error":"invalid_json"}` |
| Missing/typed-wrong envelope field | `400` | `{"error":"invalid_schema"}` |

---

### `GET /devices/{device_id}/health`

**Response:**
```json
{
  "last_heartbeat_ts": "2026-05-23T18:53:49.123Z",
  "availability_5m": 0.97
}
```
- `availability_5m`: float 0–1, heartbeats received / 300 (expected 1/sec over 5 min)
- Returns `404` if device has never sent a heartbeat
- `device_id` may be present as an extra field; the graded fields are `last_heartbeat_ts` and `availability_5m`

---

### `GET /rooms/{room_id}/occupancy?window=1m|5m|1h`

**Response:**
```json
{
  "in_room": true,
  "occupied_pct": 0.73,
  "window_seconds": 300
}
```
- `window`: the grader queries `1m`, `5m`, and `1h`; the parser also accepts any `Nm` / `Nh` / bare `N` (seconds). Echoed back as `window_seconds`
- `in_room`: bool, latest presence event wins by `ts`
- `occupied_pct`: float 0–1, time `in_room=true` / window duration
- Computed from the Redis sorted set at query time — reflects late-event corrections

---

### `GET /alarms?since=<epoch_seconds>`

**Response:**
```json
{
  "alarms": [
    {
      "device_id": "dev_0001",
      "room_id": "room_14",
      "ts": "2026-05-23T18:53:49.123Z",
      "confidence": 0.92,
      "received_at": "2026-05-23T18:53:49.201Z"
    }
  ],
  "since": 1748026429.0
}
```
- Reads from SQLite `fall_warnings` — durable, survives restart
- `since` is a float epoch (matches the stub); inclusive, ordered by `ts ASC`, and echoed back in the response

---

### `GET /alarms/stream` (SSE)

**Headers:** `Content-Type: text/event-stream`

**Stream format:**
```
data: {"device_id":"dev_0001","room_id":"room_14","ts":"...","confidence":0.92}

data: {"device_id":"dev_0002","room_id":"room_03","ts":"...","confidence":0.87}
```
- One `data:` line per alarm, blank line separator (standard SSE format)
- Client reconnects with `?since=<last_ts>` to replay missed alarms from SQLite
- p95 latency target: **≤ 1 second** from ingestion to delivery

---

### `GET /metrics`

Returns the observability counters (see [Section 10](#10-observability-requirements)) as JSON, regardless of which transport ingested the events.

---

## 7. Functional Requirements

### F-01 — Per-Device Health
- [ ] Latest heartbeat timestamp stored and queryable per device
- [ ] Rolling 5-minute availability calculated from sorted set (not a simple counter)
- [ ] Availability recalculates correctly after late heartbeats are inserted

### F-02 — Per-Room Occupancy
- [ ] Current `in_room` state reflects the latest presence event by `ts` (not arrival order)
- [ ] Occupancy % computed correctly for 1m, 5m, and 1h windows
- [ ] Occupancy history updates correctly when late events are replayed
- [ ] Transitions stored as time-series so any window can be computed at query time

### F-03 — Fall Warning Deduplication
- [ ] Dedup key is deterministic: same device + room + second → same key every time
- [ ] Duplicate within 10s window is silently discarded (counted but not emitted)
- [ ] Each unique fall event persisted to SQLite exactly once with original `ts`
- [ ] Dedup count queryable for grader verification

### F-04 — Active Alarm Feed
- [ ] SSE endpoint streams new fall warnings in real time
- [ ] Ordered by `ts` within each room
- [ ] New subscriber receives alarms from the moment they connect
- [ ] Reconnecting subscriber can request `?since=<ts>` to fill the gap from SQLite
- [ ] p95 delivery latency ≤ 1 second under all load conditions

### F-05 — Event Ordering
- [ ] Events from same device always applied in `ts` order regardless of arrival order
- [ ] Consistent hashing routes same `device_id` to same worker
- [ ] Worker sorts pending events for a device by `ts` before applying handlers

### F-06 — Clock Skew Handling
- [ ] Events with `ts > server_now + 1h` are **rejected** (logged + counted)
- [ ] Events with `ts < server_now - 1h` are **rejected** (logged + counted)
- [ ] Events with `ts` between `now - 1h` and `now - 30s` are **accepted** and flagged `late=True`

### F-07 — Backpressure
- [ ] No events silently dropped under any load level
- [ ] `fall_warn` events processed before all other types (HIGH priority lane)
- [ ] NORMAL lane has a defined max capacity with back-pressure to the HTTP `/events` handler (and optional MQTT subscriber)
- [ ] Delay under 10x burst is logged and measurable

---

## 8. Non-Functional Requirements

### N-01 — Performance
- Baseline: 5,000 devices × ~1 event/sec = ~5,000 events/sec sustained
- Burst: 50,000 events/sec for 30 seconds, twice
- Alarm feed latency: p95 ≤ 1 second under burst
- Adding device 5,001 requires zero redeployment

### N-02 — Reliability
- Hard restart: full state recovery within 30 seconds of startup
- No alarm missed during restart gap (SQLite is durable; consumers replay via `?since`)
- No event lost during 10x burst (backpressure, never drop)

### N-03 — Correctness
- All aggregations reflect `ts` ordering, not arrival ordering
- Late events from offline devices correctly update historical windows
- Dedup is idempotent: re-processing same event log produces same state

### N-04 — Code Quality
- All modules typed with `typing` / dataclasses
- No global mutable state outside of designated state managers
- Each module has a single clear responsibility (see Section 3)
- `config.py` for all tunable constants (window sizes, queue capacity, worker count)

---

## 9. Edge Cases & Failure Modes

| Scenario | Expected Behavior |
|---|---|
| Device sends `ts` 59 minutes in the future | Rejected, logged as `clock_skew_future`, not processed |
| Device sends `ts` 61 minutes in the past | Rejected, logged as `clock_skew_past`, not processed |
| Device sends `ts` 20 minutes in the past | Accepted, flagged `late=True`, aggregations updated correctly |
| Same fall warning sent 3 times in 5 seconds | First accepted, two duplicates discarded, dedup counter += 2 |
| Same fall warning sent 15 seconds apart | Both accepted (outside 10s dedup window) — two distinct events |
| Device goes offline 20 min, replays 1,200 events | All accepted, sorted by ts, state retroactively corrected |
| NORMAL queue reaches max capacity | Back-pressure applied by slowing the `POST /events` response (or MQTT ACK); HIGH lane unaffected |
| Hard kill mid-batch | SQLite WAL ensures no partial writes; recovery replays from last committed event |
| New device connects (no prior state) | Handled gracefully; health returns 404 until first heartbeat |
| Room with no presence events | Occupancy returns 0% for all windows; `in_room` = false |
| Redis cold on restart | Recovery manager replays from SQLite snapshot + event log |
| Two workers receive events for same device | Prevented by consistent hash routing — same device always same worker |

---

## 10. Observability Requirements

### Structured Logs (every key path)

```json
{"ts": "...", "level": "INFO",  "event": "ingested",       "device_id": "...", "type": "heartbeat", "late": false}
{"ts": "...", "level": "WARN",  "event": "clock_skew",     "device_id": "...", "offset_seconds": 4200}
{"ts": "...", "level": "INFO",  "event": "fall_warn",      "device_id": "...", "room_id": "...", "dedup": false}
{"ts": "...", "level": "INFO",  "event": "fall_dedup",     "device_id": "...", "room_id": "...", "dedup": true}
{"ts": "...", "level": "WARN",  "event": "queue_pressure", "lane": "NORMAL",  "depth": 450000}
{"ts": "...", "level": "INFO",  "event": "recovery_start", "snapshot_ts": "...", "events_to_replay": 12400}
{"ts": "...", "level": "INFO",  "event": "recovery_done",  "duration_ms": 2100}
```

### Counters (queryable via `GET /metrics`)

| Metric | Description |
|---|---|
| `events_ingested_total` | All events received by the service |
| `events_rejected_clock_skew` | Rejected for clock skew (future or too far past) |
| `events_late` | Accepted but flagged as late |
| `fall_warnings_total` | Unique fall warnings persisted to SQLite |
| `fall_warnings_deduped` | Duplicates discarded |
| `queue_depth_high` | Current HIGH lane depth |
| `queue_depth_normal` | Current NORMAL lane depth |
| `alarm_feed_latency_ms_p95` | Rolling p95 of alarm delivery latency |

---

## 11. Restart & Recovery Checklist

Use this to verify restart correctness before submission:

- [ ] Start service, ingest 2 minutes of events
- [ ] Note current device health values and occupancy % for 3 rooms
- [ ] Hard-kill the service (`kill -9`)
- [ ] Restart the service
- [ ] Wait for recovery log line: `"event": "recovery_done"`
- [ ] Query `/devices/{id}/health` — values match pre-kill state ✓
- [ ] Query `/rooms/{id}/occupancy?window=5m` — values match pre-kill state ✓
- [ ] Query `/alarms?since=<start_ts>` — all fall warnings present ✓
- [ ] Connect SSE subscriber, verify stream resumes
- [ ] Ingest 30 more seconds — new alarms arrive on SSE stream ✓

---

## Project File Structure

```
teton-backend/
├── main.py                          # Entry point — wires everything together
├── config.py                        # All tunable constants
├── models.py                        # RawEvent, ValidatedEvent, Priority
│
├── core/
│   ├── db.py                        # SQLite connection + schema init
│   ├── redis_client.py              # Redis connection + ping check
│   └── recovery.py                  # Startup replay + periodic snapshot
│
├── ingestion/
│   ├── mqtt_subscriber.py           # Optional MQTT connection + message loop
│   ├── validator.py                 # Schema + clock skew validation
│   └── queue.py                     # Dual-lane priority queue
│
├── processing/
│   ├── worker_pool.py               # Async workers + consistent hash routing
│   ├── alarm_bus.py                 # In-memory per-room alarm queues
│   └── handlers/
│       ├── heartbeat.py
│       ├── presence.py
│       ├── fall_warn.py
│       └── generic.py
│
├── api/
│   ├── app.py                       # FastAPI app init
│   └── routes/
│       ├── events.py                # POST /events HTTP ingestion (primary)
│       ├── health.py
│       ├── occupancy.py
│       ├── alarms.py
│       └── metrics.py
│
├── tests/
│   ├── test_validator.py
│   ├── test_dedup.py
│   ├── test_occupancy.py
│   └── test_recovery.py
│
├── requirements.txt
├── SUBMISSION.md
└── Makefile
```

---

*Stack: Python + FastAPI + Redis + SQLite + optional Mosquitto MQTT + SSE*
*Last updated: 2026-07-06*
