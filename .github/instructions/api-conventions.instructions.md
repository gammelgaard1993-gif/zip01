---
applyTo: 'api/routes/**/*.py'
---

# API Conventions  zip01

**Scope**: `api/routes/*`, request/response shape, status codes.  
**Audience**: When designing or changing endpoints.  
**Update Cadence**: When endpoint behavior changes or a new API pattern emerges; treat as a breaking-change contract.

---

## Response Format

**Invariant**: All responses must be JSON objects at the top level; structure must be consistent and predictable.

### Success: 2xx

**Implementation**:
```json
{
  "status": "ok",
  "data": { ... },
  "meta": { "count": 10, "since": "2025-08-06T10:00:00Z" }
}
```

- `status`: always present, always `"ok"` for 2xx responses.
- `data`: the payload (may be object, array, or scalar).
- `meta`: optional; used for pagination, latency, counts, window info.

 Good:
```python
@app.get("/devices/{device_id}/health")
async def get_device_health(device_id: str = Path(...)) -> dict:
    """Retrieve device health from Redis."""
    return {
        "status": "ok",
        "data": {
            "device_id": device_id,
            "last_heartbeat": "2025-08-06T10:15:00Z",
            "online": True
        }
    }

@app.get("/alarms")
async def list_alarms(since: str | None = None) -> dict:
    """List alarms as paginated keyset."""
    alarms = await db.fetch_alarms_since(since)
    return {
        "status": "ok",
        "data": alarms,
        "meta": {
            "count": len(alarms),
            "since": min(a["id"] for a in alarms) if alarms else None
        }
    }
```

---

### Error: 4xx/5xx

**Implementation**:
```json
{
  "status": "error",
  "error": "invalid_event",
  "message": "Required field 'type' missing",
  "code": 400
}
```

- `status`: always `"error"`.
- `error`: machine-readable error slug (e.g., `invalid_event`, `persist_failed`, `clock_skew`).
- `message`: human-readable explanation of the error.
- `code`: HTTP status code (numeric).

 Good:
```python
@app.post("/events")
async def ingest_event(request: Request) -> dict:
    """Ingest a device event."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return {
            "status": "error",
            "error": "invalid_json",
            "message": "Request body is not valid JSON",
            "code": 400
        }, 400
    
    # Validation
    validated = validate_raw_event(body)
    if not validated.valid:
        return {
            "status": "error",
            "error": validated.reason,  # e.g., "clock_skew"
            "message": validated.message,
            "code": 400
        }, 400
    
    # Persist before ack
    try:
        await db.insert_event(validated)
    except Exception as e:
        logger.error(f"Persist failed: {e}")
        return {
            "status": "error",
            "error": "persist_failed",
            "message": "Failed to store event; retry later",
            "code": 503
        }, 503
    
    return {"status": "ok", "data": {"event_id": ...}}, 202
```

---

## Status Codes

**Invariant**: Status code choice must accurately reflect the outcome and recovery strategy for the client.

**Implementation**:

- **202 Accepted**: Event ingestion (`POST /events`). Event is validated and persisted, but not yet processed by workers. Client should not retry if they receive 202; the event is durable.
  - Use when: event accepted, persisted, queued for processing.

- **200 OK**: Query/read operations and streaming start. Applies to:
  - GET device health, room occupancy, alarms.
  - SSE stream (initial connection).

- **400 Bad Request**: Schema/validation failure, or invalid query parameters. Client should not retry without fixing the request.
  - Use when: missing required field, wrong type, invalid timestamp, unknown event type.
  - Do not use 400 for transient failures (use 503).

- **413 Payload Too Large**: Request body exceeds `MAX_EVENT_BYTES`. Fast-path rejection, before parsing.
  - Use for: oversized POST /events bodies.

- **503 Service Unavailable**: Transient storage failure (database/Redis unavailable). Client should retry.
  - Use when: SQLite connection error, Redis timeout, persistence failed after validation.

---

## Streaming Responses (SSE)

**Invariant**: SSE streams must be gap-free and recoverable.

**Implementation**:

- **Content-Type**: `text/event-stream`.
- **Frame format**: `data: <JSON>\n\n` (per SSE spec).
- **Reconnection**: Support `since` query parameter to resume from last known alarm ID, preventing gaps.
- **Closed streams**: If a stream client is too slow (subscriber queue fills), evict the client (do not block other subscribers). Evicted client must reconnect with `since` to resume.

 Good:
```python
@app.get("/alarms/stream")
async def stream_alarms(
    since: str | None = None
) -> StreamingResponse:
    """Stream alarms as SSE. Supply `since` to resume from a previous ID."""
    
    async def event_generator():
        # Subscribe first, capture high water
        subscriber_id = alarm_bus.subscribe("all_rooms")
        high_water = await alarm_bus.get_high_water()
        
        # Replay historical alarms up to high_water
        historical = await db.fetch_alarms_since(since, limit=high_water)
        for alarm in historical:
            yield f'data: {json.dumps(alarm)}\n\n'
        
        # Stream live alarms (already published after high_water)
        while True:
            try:
                alarm = await alarm_bus.get_next(subscriber_id, timeout=30)
                if alarm:
                    yield f'data: {json.dumps(alarm)}\n\n'
            except asyncio.TimeoutError:
                # Heartbeat (optional)
                yield ': heartbeat\n\n'
            except EvictedException:
                # Queue full; client must reconnect
                break
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )
```

---

## Path Parameters

**Guidance** (not firm):
- Use lowercase, kebab-case for multi-word IDs: `/devices/dev_0001`, `/rooms/room_000`.
- Use snake_case for endpoint segments: `/alarms/stream` (not `/alarmsStream`).
- Path parameter names should be singular: `/devices/{device_id}` (not `/devices/{id}`).

 Good:
```python
@app.get("/devices/{device_id}/health")
async def device_health(device_id: str = Path(...)):
    ...

@app.get("/rooms/{room_id}/occupancy")
async def room_occupancy(room_id: str = Path(...)):
    ...
```

---

## Query Parameters

**Guidance** (not firm):
- Use lowercase, snake_case: `?since=...`, `?window=...`, `?limit=...`.
- Support common filters: `since`, `window`, `limit`.
- Provide defaults: `window=1h`, `limit=100`.
- Validate bounds server-side; return `400` if invalid.

 Good:
```python
@app.get("/rooms/{room_id}/occupancy")
async def occupancy(
    room_id: str = Path(...),
    window: str = Query("1h", description="Time window (1h, 1d, etc.)"),
    limit: int = Query(100, ge=1, le=1000)
) -> dict:
    """Retrieve room occupancy within the time window."""
    if not is_valid_window(window):
        return {
            "status": "error",
            "error": "invalid_window",
            "message": f"Window '{window}' not supported; use '1h', '1d', etc.",
            "code": 400
        }, 400
    ...
```

---

## Timestamp Format

**Invariant**: All timestamps must be ISO 8601 format in UTC.

**Implementation**:
- Format: `2025-08-06T10:15:30Z` (no timezone offset; always Z for UTC).
- Parse with `datetime.fromisoformat()` (Python 3.7+) or similar.
- Always store and return in UTC.

 Good:
```python
from datetime import datetime, timezone

ts = datetime.now(timezone.utc).isoformat()  # "2025-08-06T10:15:30.123456+00:00"
# Standardize to "2025-08-06T10:15:30Z" by replacing "+00:00" with "Z"
ts_normalized = ts.replace("+00:00", "Z")
```

---

## Backpressure and Queue Saturation

**Invariant**: When ingestion queue is full, do not drop events; delay the response instead.

**Implementation**:
- Normal event queue full: `event_queue.put()` awaits capacity; the `202` response is delayed.
- High event queue (fall_warn) full: `put()` awaits; HIGH never drops.
- Document this behavior so clients understand delayed 202 means backpressure, not error.

 Good (comment):
```python
@app.post("/events")
async def ingest_event(request: Request):
    """Ingest a device event.
    
    Returns 202 Accepted when event is persisted and queued.
    If the queue is saturated, the response may be delayed (backpressure).
    Do not retry a 202; the event is already durable.
    """
    ...
```

---

## Contract Stability

**Invariant**: API contracts must not change in breaking ways without coordinating with clients.

**Implementation**:
- Treat response shape as a contract: don't add/remove/rename fields without bumping a version or clear deprecation.
- If adding a new optional field, document its presence and default (if absent).
- If removing a field, deprecate it first (return it with a "deprecated" marker) before removal.
- Changes to status code meaning are breaking: document clearly.

 Good:
```python
# Adding an optional field: document its presence
return {
    "status": "ok",
    "data": {...},
    "meta": {...},
    # New in v1.2: "warning": optional warning message if applicable
}

# Deprecated field: return it, but mark it
return {
    "status": "ok",
    "data": {...},
    "deprecated": {  # new in v1.1
        "old_field": value  # will be removed in v2.0
    }
}
```

---

## Error Recovery for Clients

**Guidance** (recommended patterns):

- **Idempotency**: POST /events should be idempotent (client can retry with same event ID; server stores once).
  - Use a dedup key based on event content and timestamp.
  
- **Resume from error**: For GET requests with pagination, support `since` parameter to resume from last known ID.
  
- **Distinguish transient from permanent failures**:
  - 4xx: client error; retrying won't help (fix the request).
  - 503: server error; client may retry with backoff.
  - 202: success; do not retry.

---

## Summary: When Designing a New Endpoint

- [ ] Response is JSON object with `status`, `data`, and optional `meta`.
- [ ] Status code matches the outcome: 202 for accepted, 200 for success, 4xx for client error, 503 for server error.
- [ ] Path parameters are lowercase kebab-case; query parameters are lowercase snake_case.
- [ ] Timestamps are ISO 8601 in UTC (ending with Z).
- [ ] Invalid input returns 400 with an error slug and message.
- [ ] Transient failures return 503 (client may retry).
- [ ] Streaming endpoints (SSE) support `since` for resumption without gaps.
- [ ] Document the contract in the docstring so clients can rely on it.

