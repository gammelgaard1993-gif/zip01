---
applyTo: 'tests/**/*.py'
---

# Testing  zip01

**Scope**: `tests/` directory and test-related fixtures/utilities.  
**Audience**: Before adding or modifying test cases.  
**Update Cadence**: When new edge cases surface or layer behavior changes; keep tests in sync with code.

---

## Test Organization

**Implementation**:
- File structure mirrors code: `tests/test_api.py`, `tests/test_core.py`, etc.
- Within a file: one `class TestXxx` per module/class under test.
- Shared fixtures and fakes in `tests/conftest.py`.

 Good:
```
tests/
  conftest.py                    # shared fixtures
  test_api.py                    # API routes
  test_ingestion.py              # validation, queueing
  test_processing.py             # worker pool, handlers
  test_core.py                   # db, recovery, metrics
  test_ordering.py               # concurrency-specific scenarios
```

---

## Test Naming

**Invariant**: Test names must clearly state what is being tested, under what conditions, and what the expected outcome is.

**Implementation**:
- Pattern: `test_<subject>_<condition>_<expected_outcome>`
- Condition describes the scenario or constraint.
- Outcome is the assertion or observable result.

 Good:
```python
def test_validate_event_with_future_timestamp_rejects_skew():
    ...

def test_worker_pool_routes_device_events_to_same_worker():
    ...

def test_alarm_bus_evicts_stalled_subscribers():
    ...

def test_recover_state_replays_events_from_snapshot_boundary():
    ...
```

 Bad:
```python
def test_validation():  # what is being tested?
    ...

def test_worker():  # too generic
    ...

def test_alarm():  # condition? outcome?
    ...
```

---

## Behavior vs Implementation Testing

**Invariant**: Tests must assert *externally observable behavior*, not internal state (unless absolutely necessary).

**Implementation**:
- Test what the function/class *returns* or *does*, not how it does it.
- Avoid poking at private attributes (e.g., `bus._internal_queue`); test via public API.
- Exception: testing that a cache is invalidated when expected (test the public behavior, then verify cache state if needed).

 Good:
```python
# Test observable behavior: alarm is returned by the bus
def test_alarm_bus_publish_makes_alarm_visible():
    bus = AlarmBus()
    alarm = create_alarm(room_id="room_1")
    bus.publish(alarm)
    
    recent = bus.get_recent("room_1")
    assert alarm in recent  # observable output

# Test that handler runs; verify counter incremented
def test_heartbeat_handler_increments_event_counter():
    handler = HeartbeatHandler(counter=MetricsCounter())
    event = create_event(type="heartbeat")
    
    handler.handle(event)
    
    assert handler.counter.count == 1  # public counter
```

 Bad:
```python
# Leaks implementation: poking at private queue
def test_alarm_bus_publish():
    bus = AlarmBus()
    alarm = create_alarm(room_id="room_1")
    bus.publish(alarm)
    
    assert len(bus._internal_queue) == 1  # WRONG: private state

# Doesn't test observable behavior; depends on internals
def test_handler():
    handler = Handler()
    handler._state = {...}  # WRONG: setting private state
    handler.run()
    assert handler._result == "ok"
```

---

## Edge Cases by Layer

### Ingestion (`ingestion/validator.py`, `ingestion/queue.py`)

**Invariant**: All schema violations and timestamp bounds must be caught and handled consistently.

**Test coverage**:
- Valid event (all fields correct, timestamp in range).
- Invalid schema:
  - Missing required fields: `type`, `device_id`, `ts`, type-specific field.
  - Wrong value types: `confidence` not a float, `in_room` not a bool, `magnitude` not a number.
  - Unknown `type`.
- Timestamp bounds:
  - Just inside +/- 1 hour.
  - Just outside +/- 1 hour (rejected as skew).
  - Ancient event (older than 30 seconds) marked as `late=True`.
- Priority assignment:
  - `fall_warn`  HIGH queue.
  - Other types  NORMAL queue.
- Queueing:
  - Empty queue, normal queue, full queue (backpressure test; `put()` awaits).
  - HIGH queue never drops under normal/burst load.

Reference: `tests/test_ingestion.py`, `tests/test_ordering.py`.

---

### Processing (`processing/worker_pool.py`, `processing/handlers/*`)

**Invariant**: Ordering, priority preservation, and handler isolation must be maintained under load and failure.

**Test coverage**:
- **Ordering**:
  - In-window arrivals (within 100ms): sorted by `ts`.
  - Out-of-window arrival (after 100ms): applied later, even if `ts` is earlier.
  - Severely late arrival (e.g., offline device catching up): applied in ingestion order, not `ts` order.
  - See `tests/test_ordering.py`.

- **Priority preservation**:
  - HIGH `fall_warn` drains before NORMAL.
  - HIGH never starves or waits behind NORMAL on same worker.
  - Full worker NORMAL lane backpressures router.

- **Handler failure isolation**:
  - One handler throws exception.
  - Other handlers for the same event still run.
  - Event remains durable (persisted before handler).
  - Worker loop continues (not terminated).

- **Dedup semantics**:
  - Duplicate within window: suppressed, counted as `dedup`.
  - Duplicate across window boundary: allowed (may be replay during recovery).
  - SQLite integrity key is authoritative (not Redis cache).
  - Recovery replay of dedup key: counted as `db_conflict`, not `dedup`.

Reference: `tests/test_processing.py`, `tests/test_ordering.py`.

---

### API (`api/routes/*`)

**Invariant**: Endpoint contracts (request/response shape, status codes) must be stable and testable.

**Test coverage**:
- **Contract stability**:
  - Exact response shape: `{"status": "ok", "data": {...}, "meta": {...}}`.
  - Status codes: 202 for ingestion, 200 for queries, 400 for validation, 503 for storage errors.
  - No silent truncation or implicit defaults that break downstream.

- **Pagination/keyset**:
  - First page (no `since`): returns latest `limit` items.
  - Subsequent pages (`since=<id>`): resumes from boundary, no gaps or duplication.
  - Boundary at max row ID: correctly stops.

- **Streaming (SSE)**:
  - Client connects: receives live alarms.
  - Client disconnects mid-stream: doesn't block other subscribers.
  - Client slow to consume: evicted if queue fills; must reconnect with `since`.
  - Reconnect with `since`: no gap, no duplication.

- **Dependency injection**:
  - Singletons wired on `app.state`: `db_connection`, `redis_client`, `alarm_bus`.
  - All endpoints use injected instances, not local creation.

Reference: `tests/test_api.py`.

---

### Core (`core/recovery.py`, `core/metrics.py`)

**Invariant**: Recovery must restore hot state correctly without event loss; metrics must be accurate counters.

**Test coverage**:
- **Recovery**:
  - Snapshot + replay: all events replayed.
  - No gaps: events ingested after snapshot timestamp are replayed.
  - Late events not dropped: offline device ingests events with old `ts` after restart; still replayed.
  - Metrics survive replay: counters match durable log.
  - See `tests/test_core.py::test_recover_state_*`.

- **Metrics**:
  - Counters increment for correct conditions: `events_accepted`, `events_rejected_*`, `alarm_p95_ms`.
  - Counters reset on startup (not persisted).
  - Histogram for alarm latency calculated correctly: `received_at` to alarm `ts` (not ingestion to publish).

Reference: `tests/test_core.py`.

---

## Fixtures and Fakes

**Invariant**: Tests must be fast and isolated; avoid external dependencies.

**Implementation**:
- Prefer minimal fixtures over heavy setup.
- Use fakes for Redis/SQLite when not testing persistence semantics.
  -  Create a `FakeRedisClient()` in `conftest.py` that stores state in-memory.
  -  Spin up a real Redis for every test (slow, flaky, requires Docker).
- Reuse existing fixtures; avoid duplicating mocks.
- Factory functions for test data:
  -  `def create_event(type="heartbeat", device_id="dev_0001", ...):`
  -  `def make_event(...)` or inline `Event(...)`

 Good (conftest.py):
```python
@pytest.fixture
def fake_redis():
    """In-memory Redis-like store for testing."""
    return FakeRedisClient()

@pytest.fixture
def db_in_memory(tmp_path):
    """SQLite in-memory database for testing."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)
    db.init_db()
    yield db
    db.close()

def create_event(
    type: str = "heartbeat",
    device_id: str = "dev_0001",
    ts: str | None = None,
    **kwargs
) -> dict:
    """Factory for test events."""
    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    
    base = {
        "type": type,
        "device_id": device_id,
        "ts": ts,
    }
    base.update(kwargs)
    return base
```

---

## Assertions

**Invariant**: Assertions must be clear, with useful failure messages.

**Implementation**:
- Always include a message explaining what failed and why.
- Use `assert actual == expected` (not `==` backwards).
- Group related assertions; break into separate tests only if they test independent behaviors.

 Good:
```python
result = await validate_event(invalid_event)
assert result is None, f"Expected validation to reject {invalid_event}"

alarm_data = create_alarm(...)
inserted_id = await db.insert_alarm(alarm_data)
assert inserted_id is not None, "Failed to insert alarm"
assert await db.get_alarm(inserted_id) == alarm_data, "Inserted alarm doesn't match"
```

 Bad:
```python
assert validate_event(invalid_event) == None  # cryptic on failure

assert inserted_id  # what was expected?

assert x == y  # message would help
```

---

## Flakiness

**Invariant**: Tests must be deterministic; no timing races or hidden dependencies.

**Implementation**:
- Never use `sleep()` or time-based waits to mask race conditions.
- Use deterministic event sequencing or synchronization primitives (locks, events, queues).
- If a test is inherently timing-dependent (e.g., alarm latency measurement), document why and mark with `@slow`.

 Good:
```python
# Deterministic: order guaranteed by queue semantics
def test_priority_high_drains_before_normal():
    queue = PriorityEventQueue()
    queue.put_high(event1)
    queue.put_normal(event2)
    
    result1 = queue.get()
    result2 = queue.get()
    
    assert result1 == event1
    assert result2 == event2
```

 Bad:
```python
# Flaky: sleep doesn't guarantee timing
def test_alarm_latency():
    import time
    start = time.time()
    bus.publish(alarm)
    time.sleep(0.1)  # WRONG: arbitrary delay
    elapsed = time.time() - start
    assert elapsed < 0.2, "Alarm too slow"
```

---

## Test Execution

**Guidance** (not firm):
- Run targeted tests first: `python -m unittest tests.test_api -v`
- Then broader suite: `python -m unittest discover -s tests -v`
- Use `@slow` or `@integration` markers for heavyweight tests; skip in quick runs.

---

## Summary: Test Quality Checklist

Before committing a test:

- [ ] Name clearly states subject, condition, outcome.
- [ ] Asserts observable behavior, not private state.
- [ ] Covers the documented edge case for its layer.
- [ ] Uses fixtures, not real Redis/SQLite (unless testing persistence).
- [ ] No `sleep()` or timing races.
- [ ] Has a failure message: `assert x == y, f"expected {y}, got {x}"`.
- [ ] Can run in any order (not dependent on test order).
- [ ] Runs in < 1 second (or marked `@slow` with justification).

