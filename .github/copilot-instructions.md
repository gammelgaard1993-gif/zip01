# zip01 AI Collaboration Baseline

This file is the shared baseline for AI-assisted development in this repository.
Keep it concise and stable. Put machine-specific overrides in `CLAUDE.local.md` (gitignored).

## Scope

- Repository: zip01
- Runtime: Python 3.12+, FastAPI
- Storage: Redis (hot state) + SQLite (durable history/snapshots)
- Primary transport: `POST /events`

## Architecture Snapshot

- `api/`: FastAPI app lifecycle and read/stream endpoints.
- `ingestion/`: event validation, priority assignment, ingress queueing/backpressure.
- `processing/`: worker routing, reorder buffering, handlers, alarm bus.
- `core/`: durability, redis client, recovery/snapshot/replay, metrics plumbing.
- `tests/`: behavior and integration coverage.

## Working Conventions

- Keep changes surgical and layer-owned; do not cross boundaries unless required.
- Preserve API contracts unless explicitly requested to change them.
- Prefer targeted validation first:
  - `python -m unittest discover -s tests -v`
- Reuse existing patterns in `api/`, `processing/`, `core/`, and `tests/`.

## Challenge Contract Precedence

This repository is built for the Teton streaming-backend challenge. When guidance conflicts,
the challenge-facing runtime contract takes precedence over generic style guidance.

- Primary source of endpoint and payload truth:
  - `REQUIREMENTS.md` (API Contract section)
  - `eval/check.py` (what the local evaluator reads)
- Keep grader-facing response shapes stable for:
  - `POST /events`
  - `GET /devices/{device_id}/health`
  - `GET /rooms/{room_id}/occupancy`
  - `GET /alarms`

Do not normalize these endpoints to a generic envelope format unless the challenge contract is
updated and tests/eval compatibility are updated in the same change.

## Collaboration Precedence

When multiple guidance sources apply, use this order:

1. Local overrides (`*.local.*`, not committed)
2. Repository settings (`settings.json`, `.mcp.json`)
3. Modular rules (`rules/`), once enabled
4. This baseline (`CLAUDE.md`)

## Agent/Specialist Context

Specialist agent definitions live in `.github/agents/` and define ownership boundaries.
Use these to keep work scoped and reduce context contamination between review, security, testing, and layer-specific implementation tasks.

## Notes

- `rules/` files are intentionally not added yet and will be reviewed before creation.
