# Hooks (v1)

These hooks are templates for enforceable guardrails.

## Included

- `pre-tool/block-destructive-git.ps1`
  - Blocks destructive git command patterns.
- `post-tool/quick-validation.ps1`
  - Runs quick validation after code edits.

## Expected Invocation Contract

Your hook runner should pass:

- `-ToolCommand "<raw tool command>"` to pre-tool hooks.
- `-ChangedPath "<relative or absolute path>"` to post-tool hooks (optional).

Adapt argument names if your hook runtime uses environment variables instead.
