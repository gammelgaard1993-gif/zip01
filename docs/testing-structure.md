# Testing Structure

This repository uses a layered test model so the behavior of the streaming backend stays clear at a project level.

## Test Layers

- Foundation tests validate the basic harness, wiring, and invariants that everything else depends on.
- Ingestion tests validate validation, queueing, priority assignment, and backpressure.
- Processing tests validate worker routing, ordering, deduplication, and handler isolation.
- Core tests validate durability, persistence, snapshot, and replay behavior.
- API tests validate route contracts, response shapes, streaming behavior, and startup wiring.
- Integration tests validate cross-layer scenarios that need more than one subsystem at once.

## Where Tests Live

- `tests/test_phase1_foundation.py`: base harness and shared assumptions.
- `tests/test_phase2_ingestion.py`, `tests/test_events_route.py`, `tests/test_queue_backpressure.py`: ingestion and pressure behavior.
- `tests/test_phase3_processing.py`, `tests/test_dedup.py`, `tests/test_ordering.py`: worker and handler behavior.
- `tests/test_db_writer.py`, `tests/test_recovery.py`: durable storage and recovery.
- `tests/test_api_contract_http.py`, `tests/test_app_lifespan_smoke.py`, `tests/test_alarms.py`, `tests/test_occupancy.py`: API contract and lifecycle behavior.
- `tests/test_integration.py`: cross-layer flows and scenario checks.

## Shared Helpers

- Reusable fake clients, response protocols, and request builders live in `tests/fakes.py`.
- Add new helpers there when the same fake or protocol would otherwise be duplicated across files.

## Adding New Tests

- Choose the lowest layer that can prove the behavior.
- Keep scenario tests focused on one invariant or failure mode at a time.
- Use shared fakes rather than patching deep internals.
- Prefer explicit assertions on externally visible state over implementation details.

## Suggested CI Split

- Fast gate: foundation, ingestion, processing, core, and API contract tests.
- Extended gate: integration, recovery, and backpressure-heavy scenarios.
- Manual or scheduled: long-running load and resilience checks.
