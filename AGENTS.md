# Repository Guidelines

## Project Structure & Module Organization
- `backend/`: Flask API, workflow orchestration, scheduler, report generation, and local runtime state.
- `frontend/`: lightweight multi-page UI (`index.html`, `tracker.html`, `reports.html`, `operations.html`) plus shared JS/CSS.
- `core/`: project-owned screening engine used by the workflow; treat it as internal product code, not an external reference.
- `config/project.yaml`: runtime config for database paths, API endpoints, scheduler, and web binding.
- `scripts/`: operator entry points such as `run_bond_workflow.py`, `run_scheduler.py`, and `supplement_bond_holding_period.py`.
- `reports/`: generated Excel/CSV/Markdown/JSON outputs, publication state, and archived runs.

## Build, Test, and Development Commands
- `python3 -m flask --app backend.app run --host 0.0.0.0 --port 8090`: start the local web app.
- `python3 scripts/run_bond_workflow.py`: run the full bond workflow.
- `python3 scripts/run_bond_workflow.py --reuse-existing-outputs`: rebuild reports from existing screening outputs.
- `python3 scripts/run_scheduler.py --force`: start the background scheduler.
- `python3 -m py_compile backend/*.py scripts/*.py tests/*.py`: quick syntax verification.
- `python3 scripts/run_isolated_tests.py -- tests/test_regression_guards.py`: run regression checks in a fresh, network-disabled Docker container. Direct pytest collection is refused before application imports.

## Coding Style & Naming Conventions
- Use Python 3.11+ with 4-space indentation, type hints, and `snake_case` names.
- Keep orchestration in `backend/`; do not move workflow logic into Flask route handlers.
- Prefer config-driven paths and ports; update `config/project.yaml` instead of hardcoding values.
- Frontend is framework-free; keep modules small and reuse helpers from `frontend/shared.js`.

## Working Method
- Think before coding: state assumptions explicitly, surface ambiguity, and prefer asking over silently guessing.
- Prefer the simplest change that fully solves the requested problem; avoid speculative abstraction or configurability.
- Make surgical edits only; do not refactor or clean unrelated code while working on the current task.
- Every meaningful change should have a clear verification path such as tests, API checks, generated artifacts, or logs.

## Data Truthfulness Rules
- Do not fabricate fund business data, returns, NAV values, risk metrics, publication counts, or final lists.
- Raw business data must come from real APIs, the database, or real runtime artifacts.
- Missing business values must preserve missing semantics; do not render them as fake `0`, fake success, or fake pass states.
- Do not loosen production safety gates just to increase output counts.
- Do not treat frontend displays, archived exports, or temporary watch pools as authoritative publication truth.

## Testing Guidelines
- Backend changes should pass `py_compile` and the relevant `pytest` module.
- Run pytest through `scripts/run_isolated_tests.py`; `--image fund-ana:<version>` reuses an image only after source SHA verification. Never mount production data/config into test containers. Scheduled smoke isolates unit-test groups inside its container; real-data audits keep their runtime data source.
- For workflow/report changes, verify `/api/health`, `/api/dashboard/latest`, and one real or dry-run workflow execution.
- If screening rules change, check generated files in `reports/runs/` and the tracker workbook in `reports/`.
- If scheduler or automation behavior changes, also verify `/api/scheduler/status/detail` and relevant task logs.

## Commit & Pull Request Guidelines
- Use clear imperative commit messages, preferably Conventional Commits, such as `feat: tighten bond publish gates`.
- PRs should state strategy changes, config changes, validation commands, and affected output files.
- Include screenshots for frontend updates and mention any Excel/report schema changes.

## Strategy Notes
- The published bond list must exclude funds with structured `holding_days > 90`.
- Frontend should stay focused on the final quality tracker, not raw intermediate audit noise.
- Do not assume "increase final list count" is the default optimization target; data quality, automation correctness, and publication semantics take priority.
- Do not change screening thresholds before confirming the issue is not caused by data freshness, long-term history coverage, holding-period gating, AutoStock hard blocks, or publication semantics.

## Deployment Notes
- For changes that affect the live containerized runtime, complete the full cycle: Docker build, redeploy, and remove old project images.
- Keep `docs/BOND_FUND_AUDIT_AND_IMPLEMENTATION_PLAN_20260409.md` updated with the current live baseline and open priorities.
