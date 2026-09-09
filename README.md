# zip01

Real-time streaming backend for sensor events with prioritized processing, Redis hot state, SQLite durability, and FastAPI APIs.

## Quickstart

Requires Redis (via Docker or a native install) and Python 3.12+.

```bash
make deps     # install pinned dependencies
make run      # start Redis (Docker) and the service on :8080
```

In a second shell, drive load and inspect the API:

```bash
make test                 # unit + integration tests
make smoke                # quick end-to-end check (service must be running)
DEVICES=500 make burst    # load exercise; inspect drops, queue pressure, and alarm p95
make offline              # offline device replays a 20-min backlog of late events
```

See [SUBMISSION.md](SUBMISSION.md) for the design summary and [Makefile](Makefile) for all targets.

### Running on Windows (no `make`/`docker`)

Start Redis however you prefer (native installs or Docker Desktop), then run the
service and simulator directly:

```powershell
python -m pip install -r requirements.txt -r requirements-dev.txt
python main.py                                              # service on :8080
python event_generator/generate.py --mode baseline --duration 30 --devices 500 --target http://localhost:8080
python event_generator/generate.py --mode burst     --duration 30 --devices 500 --target http://localhost:8080
python event_generator/generate.py --mode offline   --duration 120 --devices 500 --target http://localhost:8080
python -m unittest discover -s tests -v                    # tests
```

The service connects to `localhost:6379` (Redis, required) by default and listens on `:8080`.
HTTP `POST /events` is the primary transport (what the reference generator
[event_generator/generate.py](event_generator/generate.py) posts to).

### Concurrent Load Test

The pooled local helper uses reusable HTTP connections and samples `/metrics` independently of
the request pool. Install development dependencies first, then run it against a running service:

```powershell
python -m pip install -r requirements.txt -r requirements-dev.txt
python helpers/_loadtest.py --devices 500 --duration 120 --connections 32 --metrics-interval 1
python helpers/_loadtest.py --devices 500 --duration 90 --connections 32 --metrics-interval 1 --burst
```

`--connections` caps reusable concurrent POST connections; `--concurrency` remains a legacy
alias. Defaults are 500 devices, a 90-second duration, and 64 connections when using the legacy
`--concurrency` setting. Use `--chart-rate-limit <rate>` to apply one fixed rate/s ceiling to
all dashboard charts; without it, each chart uses a rounded ceiling with 20% headroom and a
minimum of 100 rate/s. The burst is fixed at $10\times$ from elapsed seconds 30 through 60, so use a duration
greater than 60 seconds for a complete burst. The report includes request and sampled SSE latency,
and writes a self-contained HTML dashboard to `loadtest-dashboard.html` by default; use
`--report <path>` to choose another location. The dashboard shows stock-style, elapsed-time rate
charts for accepted ingress requests, committed SQLite batches, and worker-handled events. Each
chart has a left rate/s axis, right queue-depth axis, queue-pressure area, burst shading, and a
hover tooltip with elapsed time, rate, and queue depth; it also includes generated per-room traffic
distribution: an all-room profile sorted from busiest to quietest and horizontal bars for the ten
rooms with the most dispatched events. Metric summaries report counters as first-to-last deltas and gauges
(queue/activity/age/p95/last-batch measurements) as both sampled maxima and final values.

## Testing

The test suite is organized by behavior layer so the project-level structure stays readable:

- Foundation and harness setup
- Ingestion, queueing, and backpressure
- Processing, ordering, and deduplication
- Core durability and recovery
- API contracts and startup wiring
- Integration and scenario coverage

Shared test doubles and helper conventions are documented in [docs/testing-structure.md](docs/testing-structure.md).

Current local result: **113 tests run: 112 passed, 1 optional real-Redis concurrency test skipped** when
`TEST_REDIS_URL` is not configured. The suite verifies cancellation-safe admission and queueing,
atomic presence updates, deterministic recovery, and API contracts. Challenge-scale throughput
and sustained p95 latency are not established by the test suite. Use
[helpers/_loadtest.py](helpers/_loadtest.py) for a local concurrent measurement; the supplied
synchronous generator is useful for compatibility checks, not as proof of the 5k/s and 50k/s targets.

## AI Collaboration Template (v1)

This repository includes a lightweight v1 collaboration template under .github/ for GitHub/Copilot customization:

- .github/copilot-instructions.md: shared baseline context and collaboration precedence.
- .github/.mcp.json: shared MCP integration scaffolding.
- .github/settings.json: default tool/model safety settings.
- .github/instructions/*.instructions.md: scoped rules with path targeting.
- .github/prompts/*.prompt.md: explicit reusable command-style workflows.
- .github/agents/: native custom agents for reusable workflows and specialist roles.
- .github/hooks/: pre/post tool guardrail scripts (invoked by your selected runner).

## Documentation

Project documentation is available in the docs folder:

- [Documentation Index](docs/README.md)
- [Architecture](docs/architecture.md)
- [Event Flow](docs/event-flow.md)
- [Critical Functions](docs/critical-functions.md)
- [API Reference](docs/api-reference.md)
- [Storage and Recovery](docs/storage-recovery.md)
- [Testing Structure](docs/testing-structure.md)
