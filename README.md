# Job benchmarking for Databricks (Azure)

Find the best compute configuration for a Lakeflow Job with measured data instead of guesswork. The framework copies a job once per configuration **variant**, runs the copies against the same inputs with isolated outputs, and ranks them by runtime and estimated cost (DBU + VM). Results land in Delta tables, an AI/BI dashboard and a Genie agent, plus an AI-written recommendation of what to change in the job definition.

It is also a Genie Code skill: with this folder in `.assistant/skills/job-benchmarking/`, Genie Code knows how to run and interpret it.

## What gets tested

One factor at a time vs the baseline (an exact copy of the job):

* Worker shape (half the workers at 2x size, double the workers at half size; same total cores)
* Autoscaling off (fixed at max)
* Azure VM generation (v4 / v5 / v6)
* Delta cache (local SSD `d` VMs) on/off
* Spot with fallback (for on-demand clusters)
* Photon on/off
* DBR version (default candidates 15.4 and 16.4)
* `spark.sql.shuffle.partitions` = auto / 200
* Serverless jobs compute, standard and performance-optimized (optional, with automatic blocker detection)

Details: [references/variants.md](references/variants.md).

## Requirements

* Azure Databricks workspace with Unity Catalog; DBR 14.3+ or serverless for the notebooks.
* A catalog where you can create schemas, tables and views.
* SELECT on `system.billing`, `system.compute` and `system.lakeflow`.
* Outbound HTTPS to `prices.azure.com` (VM prices), once.
* The job to benchmark must take its output location as a job parameter (default `output_schema`), so variants can write to their own schemas.

## Install

1. Push this folder to a git repository.
2. In the workspace, create a **Git folder** from the repo (Workspace > Create > Git folder). Files with a `# Databricks notebook source` header become notebooks; `notebooks/bench_*.py` stay Python modules.
3. Optional, for Genie Code: put (or clone) the folder at `/Workspace/Users/<you>/.assistant/skills/job-benchmarking/` (or the workspace-level skills folder) so Genie Code picks up `SKILL.md`.

## Quick start

Set the same `bench_catalog` (and optionally `bench_schema`, `output_catalog`, `bench_prefix`) in every notebook.

| Step | Notebook | Notes |
| --- | --- | --- |
| 0 | `notebooks/00_setup_cost_model` | Once per workspace. Region is auto-detected on classic compute; on serverless set `compute_region` (e.g. `northeurope`) |
| opt | `test_job/00_create_test_job` | Creates a small 3-task test job to try everything first |
| 1 | `notebooks/01_discovery` | Set `job_id`. Prints the `benchmark_id` |
| 2 | `notebooks/02_variant_generator` | Set `benchmark_id`. Review the variant list |
| 3 | `notebooks/03_job_copier` | Dry run first, then `dry_run = false` to create the jobs |
| 4 | `notebooks/04_orchestrator` | Dry run first, then `dry_run = false` to start the runs (costs money) |
| 5 | `notebooks/05_analysis` | Re-run after a few hours for complete cost. `ai_model` blank = auto-detect a chat endpoint, `none` = skip |
| 6 | `notebooks/06_deploy_reporting` | Once: creates and publishes the dashboard and Genie agent |

Suggested plan: screen all variants with 1 run each, then confirm the 2-3 best (plus baseline) with `max_runs = 2` or `3` using `variants_filter`.

Everything is idempotent and keyed by `benchmark_id`; several benchmarks (or jobs) can share the schema. To compare over time (e.g. after code changes), benchmark the job again with a new `benchmark_id` and a `code_label`.

## Safety

* The source job is only read. Copies are named `[<prefix>-<benchmark_id>] ...`, have no schedule, triggers or notifications, and write to `<output_catalog>.<prefix>_<benchmark_id>_<variant>`.
* Inputs are read directly. List them in `input_tables` (04) to check their Delta versions did not change during the benchmark.
* Cleanup is manual: delete the benchmark jobs listed in `bench_jobs` and drop their output schemas.

## Tests

The decision logic lives in plain Python modules next to the notebooks; the notebooks only read widgets, call the APIs and write tables. The modules have no Spark or workspace dependency, so their tests run in about two seconds:

* In the workspace: open any file in `tests/` and use the Tests pane (run all, run failed, or run a single test), or run `pytest` from this folder in a web terminal.
* Locally or in CI: `pip install -r requirements-dev.txt && pytest`. `.github/workflows/tests.yml` runs them on every push to `main` and on every pull request (no secrets needed).

When you change a rule, change the module and its test together. `tests/fixtures/bf1e865e.json` is a recorded real benchmark (job settings, clusters, node types and the variants it produced). If a change is meant to alter the variants for that job, update `expected_variants` in the same commit, so the diff shows the behaviour change.

The notebooks load `%autoreload 2`, so edits to the modules are picked up without restarting Python.

## Layout

```
SKILL.md                     Genie Code skill entry point
README.md                    this file
notebooks/
  bench_common.py            shared config widgets + Azure VM helpers (module)
  bench_variants.py          variant generation rules, incl. serverless blockers (used by 02)
  bench_copier.py            job copy: overrides, isolation, tags, serverless conversion (used by 03)
  bench_orchestration.py     run timing, run queue, batches, code version (used by 04)
  bench_analysis.py          cost flags, Pareto, picks, confidence, verdicts (used by 05)
  00_setup_cost_model.py     VM prices + job run cost view
  01_discovery.py ... 06_deploy_reporting.py
tests/                       pytest unit tests for the bench_*.py modules (+ fixtures/)
pytest.ini, requirements-dev.txt, .github/workflows/tests.yml
templates/
  dashboard_template.json    AI/BI dashboard (tokens __BENCH_FQN__, __GENIE_SPACE_ID__)
  genie_template.json        Genie agent (token __BENCH_FQN__)
test_job/                    optional test workload
references/                  variants, cost model, interpreting results, data model, troubleshooting
```

## Limitations

* Azure only (VM naming and price API). See [references/troubleshooting.md](references/troubleshooting.md#porting-to-aws-or-gcp) for porting.
* Costs are list-price estimates (no discounts); Spot is priced with a fixed multiplier. Good for ranking variants, not for invoicing. See [references/cost-model.md](references/cost-model.md).
* One factor at a time: combined effects are not measured.
* Streaming / continuous jobs, pipelines and all-purpose-cluster tasks are not benchmarked.
