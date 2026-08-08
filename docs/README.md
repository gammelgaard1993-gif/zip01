# zip01 Documentation

This folder contains project documentation for the zip01 real-time streaming backend.

## Contents

- [Architecture](architecture.md): system boundaries, responsibilities, and runtime composition.
- [Event Flow](event-flow.md): ingestion to API event path and state updates.
- [Critical Functions](critical-functions.md): high-impact functions, inputs/outputs, side effects, and failure behavior.
- [API Reference](api-reference.md): endpoint behavior and response contracts.
- [Storage and Recovery](storage-recovery.md): Redis/SQLite roles, schema, snapshots, and replay behavior.
- [Testing Structure](testing-structure.md): project-level test layering, shared helpers, and CI split guidance.

## Implementation Snapshot

The service starts in FastAPI lifespan and wires:

- SQLite via `core.db.init_db()`
- Redis via `core.redis_client.get_redis_client()`
- In-memory priority queue via `ingestion.queue.PriorityEventQueue`
- HTTP ingestion (primary) via `api.routes.events` (`POST /events`)
- Workers via `processing.worker_pool.WorkerPool`
- Alarm fan-out via `processing.alarm_bus.AlarmBus`
- Recovery and snapshots via `core.recovery.RecoveryManager`

Primary startup wiring is in `api/app.py`. The service listens on `:8080` (see `config.py`).

Current correctness baseline: **113 tests run: 112 passed, 1 optional real-Redis concurrency test skipped**
when `TEST_REDIS_URL` is absent. Challenge-scale throughput remains a separate benchmark task.

## Scope Notes

- Documentation is based on current code and tests in this repository.
- Where requirements and implementation diverge, behavior described here reflects implemented code paths.
