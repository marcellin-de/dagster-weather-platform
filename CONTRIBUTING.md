# Contributing

## Branching

- Create a dedicated branch per change.
- Keep pull requests focused (one feature/fix per PR).
- Rebase on `main` before opening the PR when possible.

## Local quality gate

Run before commit:

```bash
make verify
```

This runs:

- `dg check defs`
- unit tests in `tests/`

## Code conventions

- Keep business logic pure/testable when possible.
- Keep Dagster composition in `definitions.py`; avoid scattering resource wiring.
- Reuse shared helpers in `checks/` and `resources/` before duplicating logic.
- Add or update tests for any behavior change.

## Commit style

- Use short, explicit commit titles.
- Prefer commits that are easy to review and revert.
- Mention user-facing behavior changes in PR description.
