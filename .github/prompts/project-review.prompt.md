---
name: /project:review
description: Run a focused repository review across requirements, security, and submission readiness.
---

## Purpose

Run a consistent review workflow before major merges or releases.

## Inputs

- Scope (`full repo` or path subset)
- Optional target branch/commit

## Workflow

1. Run **Requirements Reviewer** for implementation-vs-requirements gaps.
2. Run **Security Reviewer** for OWASP-oriented read-only risk findings.
3. Run **Submission Readiness Reviewer** for delivery/checklist quality risks.
4. Consolidate findings into:
   - blockers
   - non-blocking risks
   - prioritized fix order

## Expected Output

- Single summary with:
  - top blockers
  - evidence references
  - recommended owners (API/Core/Ingestion/Processing/Broker/Testing)
