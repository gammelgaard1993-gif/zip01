---
applyTo: '**/*.py'
---

# Code Style  zip01

**Scope**: All Python files (`api/`, `ingestion/`, `processing/`, `core/`, `tests/`, etc.)  
**Audience**: Every code change.  
**Update Cadence**: When patterns shift or new conventions emerge; keep in sync with actual codebase.

---

## Python Conventions

### Imports

**Invariant**: Import organization must be clear and predictable.

**Implementation**:
- Group by: stdlib, third-party, local (one blank line between groups).
- Use explicit imports; avoid `from module import *`.
- Sort each group alphabetically.

 Good:
```python
import json
import os
from datetime import datetime

import uvicorn
from fastapi import FastAPI

from core.db import init_db
from ingestion.queue import PriorityEventQueue
```

 Bad:
```python
import *
from core.db import *
import uvicorn, json  # multiple on one line
```

---

### Naming Conventions

**Invariant**: Names should be unambiguous and intention-revealing.

**Implementation**:

- **Functions/methods**: `snake_case`, active verbs (what does it *do*?).
  -  `validate_event()`, `restore_state()`, `publish_alarm()`, `get_device_health()`
  -  `validation()`, `state_restore()`, `event()`, `get()` (too generic)

- **Classes**: `PascalCase`, noun phrases (what is it?).
  -  `WorkerPool`, `AlarmBus`, `RecoveryManager`, `EventValidator`
  -  `Worker`, `Bus` (too generic), `Manage` (verb, not noun)

- **Constants**: `UPPER_SNAKE_CASE` only if module-wide and immutable.
  -  `MAX_EVENT_BYTES = 16_384` (top of module)
  -  `max_event_bytes = 16_384` (looks mutable)
  - Local constants: lowercase `max_retries = 3` (scoped to function/class)

- **Private/internal**: Leading underscore for single-scope encapsulation.
  -  `_internal_queue` (not meant to be accessed from outside the class)
  -  `internal_queue` (implies public contract)

---

### Type Hints

**Invariant**: Type information must be present for code clarity and tooling support.

**Implementation**:
- All new functions must have type hints for parameters and return type.
- Use `from __future__ import annotations` at the top to enable forward references.
- No bare `*args` or `**kwargs` without type annotations.

 Good:
```python
from __future__ import annotations
from typing import Literal

def validate_event(
    event: dict,
    max_clock_skew_seconds: int = 3600
) -> ValidatedEvent | None:
    """Validate event schema and timestamp bounds."""
    ...

async def stream_alarms(
    room_id: str,
    since: str | None = None,
) -> AsyncGenerator[str, None]:
    """Yield SSE alarm frames for a room."""
    ...
```

 Bad:
```python
def validate_event(event, max_clock_skew_seconds=3600):  # no hints
    ...

def stream_alarms(*args, **kwargs):  # bare, no hints
    ...
```

---

### Comments

**Invariant**: Code should be self-documenting; comments explain *why*, not *what*.

**Implementation**:
- Code readability > comments. If you need a comment to explain what code does, rewrite the code.
- Comments explain: intent, non-obvious design decisions, edge cases.

 Good:
```python
# Defer flush to allow out-of-order arrivals within the reorder window.
# Events that arrive late (after the window closes) will be applied in ingestion order,
# not timestamp orderthis is intentional to keep latency bounded.
await asyncio.sleep(DEVICE_REORDER_BUFFER_MS / 1000.0)
```

 Bad:
```python
# sleep for 100ms
await asyncio.sleep(0.1)

# increment counter
counter += 1

# check if value is None
if value is None:
```

---

### Line Length & Formatting

**Guidance** (not firm):
- Max 100 characters per line; wrap logically.
- Use parentheses for implicit line continuation (readability over strictness).

 Good:
```python
result = await validate_and_persist(
    event=parsed_event,
    storage=db,
    dedup_key=dedup_key
)

decorated_response = {
    "status": "ok",
    "data": alarms,
    "meta": {"count": len(alarms), "ts": current_ts}
}
```

---

## FastAPI Endpoints (api/routes/)

**Invariant**: Endpoints must have clear, documented contracts.

**Implementation**:
- Use decorators consistently: `@app.get(...)`, `@app.post(...)`.
- Include a one-line docstring explaining what the endpoint does.
- Use typed `Query`, `Path`, `Body` parameters for clarity and validation.

 Good:
```python
@app.get("/devices/{device_id}/health")
async def get_device_health(
    device_id: str = Path(..., description="Device ID"),
) -> dict:
    """Retrieve device health from Redis."""
    return {"device_id": device_id, "last_heartbeat": ...}

@app.get("/rooms/{room_id}/occupancy")
async def get_occupancy(
    room_id: str = Path(...),
    window: str = Query("1h", description="Time window (e.g., 1h, 1d)"),
) -> dict:
    """Retrieve room occupancy transitions within the time window."""
    ...
```

 Bad:
```python
async def get_device_health(device_id, window=None):  # no types, no docstring
    ...

def occupancy():  # no decorators, generic name
    ...
```

---

## Type Aliases

**Guidance**: Define type aliases at module top for complex or repeated types.

 Good:
```python
from __future__ import annotations
from typing import Literal

EventType = Literal["heartbeat", "fall_warn", "presence", "motion", "sleep_state", "net_status"]
DeviceID = str  # semantic clarity
RoomID = str
Timestamp = str  # ISO 8601

def process_event(event_type: EventType, device_id: DeviceID) -> None:
    ...
```

---

## Async/Await

**Invariant**: Do not block the event loop; use async for I/O.

**Implementation**:
- Use `async def` and `await` for I/O-bound operations (Redis, SQLite, HTTP, asyncio.sleep).
- Never use blocking calls inside async functions (e.g., no `time.sleep()`, no `requests.get()`).

 Good:
```python
async def publish_alarm(alarm: dict) -> None:
    """Publish alarm to bus (non-blocking)."""
    await self.alarm_bus.publish(alarm)
```

 Bad:
```python
async def publish_alarm(alarm: dict) -> None:
    """Publish alarm (blocks event loop!)."""
    time.sleep(0.1)  # WRONG: blocks event loop
    self.redis.set(...)  # WRONG: blocking I/O
```

---

## Error Handling

**Invariant**: Failures must not silently propagate; log and decide (fail fast or retry).

**Implementation**:
- Catch exceptions at the layer that owns the failure.
- Log with context (what operation, with what inputs, why it failed).
- Never swallow exceptions with bare `except:`.

 Good:
```python
try:
    alarm_row = await db.insert_alarm(alarm_data)
except sqlite3.IntegrityError:
    logger.info(f"Duplicate alarm (dedup key: {dedup_key}), skipping.")
    return  # Expected during recovery replay
except Exception as e:
    logger.error(f"Failed to persist alarm: {e}", exc_info=True)
    raise  # Let caller decide
```

 Bad:
```python
try:
    await db.insert_alarm(alarm_data)
except:  # catches everything; hides bugs
    pass  # silent failure
```

---

## Class Structure

**Guidance**: Keep classes focused and cohesive.

- One responsibility per class.
- Use `__init__` for setup; avoid heavy logic.
- Use `async def __aenter__` / `__aexit__` for resource management.

 Good:
```python
class RecoveryManager:
    """Restore hot state from snapshot and replay events."""
    
    def __init__(self, db: Database, redis: Redis):
        self.db = db
        self.redis = redis
    
    async def restore_state(self) -> None:
        """Load snapshot, clear Redis, reapply state, replay events."""
        ...
```

---

## Testing-Related Code

(See `rules/testing.md` for test-specific conventions.)

- Use the same style for test fixtures and helper functions.
- Factory functions for test data: `def create_event(...)` not `make_event(...)`.

---

## Summary: When to Deviate

- **Invariants** (e.g., type hints, idempotent handlers): Always follow.
- **Implementation** (e.g., naming, import groups): Follow unless there's a strong reason; document the deviation.
- **Guidance** (e.g., line length, comment style): Guidelines; use judgment.

If you deviate from code-style.md, update the rule so the codebase stays coherent.

