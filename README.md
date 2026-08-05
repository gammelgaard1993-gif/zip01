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
DEVICES=500 make burst    # 10x burst — verify no drops + alarm p95 <= 1s
make offline              # offline device replays a 20-min backlog of late events
```

See [SUBMISSION.md](SUBMISSION.md) for the design summary and [Makefile](Makefile) for all targets.

### Running on Windows (no `make`/`docker`)

Start Redis however you prefer (native installs or Docker Desktop), then run the
service and simulator directly:

```powershell
python -m pip install -r requirements.txt
python main.py                                              # service on :8080
python event_generator/generate.py --mode baseline --duration 30 --devices 500 --target http://localhost:8080
python event_generator/generate.py --mode burst     --duration 30 --devices 500 --target http://localhost:8080
python event_generator/generate.py --mode offline   --duration 120 --devices 500 --target http://localhost:8080
python -m unittest discover -s tests -v                    # tests
```

The service connects to `localhost:6379` (Redis, required) by default and listens on `:8080`.
HTTP `POST /events` is the primary transport (what the reference generator
[event_generator/generate.py](event_generator/generate.py) posts to).

## Documentation

Project documentation is available in the docs folder:

- [Documentation Index](docs/README.md)
- [Architecture](docs/architecture.md)
- [Event Flow](docs/event-flow.md)
- [Critical Functions](docs/critical-functions.md)
- [API Reference](docs/api-reference.md)
- [Storage and Recovery](docs/storage-recovery.md)
