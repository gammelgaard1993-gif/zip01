---
name: /project:regression-check
description: Validate a change with targeted tests and a quick runtime smoke path.
---

## Purpose

Standardize post-change checks for zip01 without defaulting to oversized validation.

## Inputs

- Changed files/modules
- Optional scenario (`baseline`, `burst`, `offline`)

## Workflow

1. Map changed modules to the narrowest relevant test targets.
2. Run targeted `unittest` command(s) first.
3. If behavior touches event flow or API contract, run a quick smoke scenario against a running service.
4. Report:
   - what was validated
   - pass/fail evidence
   - remaining risk not covered by executed tests

## Expected Output

- Validation summary + exact commands used + any follow-up recommendation.
