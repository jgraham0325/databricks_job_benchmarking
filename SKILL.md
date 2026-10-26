---
name: job-benchmarking
description: Benchmark a Databricks (Azure) Lakeflow Job across cluster configurations - worker shape, autoscaling, VM generation, delta cache, Spot, Photon, DBR, shuffle partitions and serverless - by creating isolated copies of the job, running them, and ranking the variants by task-only runtime and estimated DBU + VM cost, with an AI recommendation, an AI/BI dashboard and a Genie agent. Read this skill when the user asks to benchmark, right-size, tune or compare compute for a job, find the cheapest or fastest cluster config, evaluate serverless vs classic for a job, or work with bench_* / rpt_* benchmark tables.
---

# Job benchmarking

A notebook-based framework that answers "what compute config should this job use?" with measured data. It copies a source job once per **variant** (one factor changed at a time), runs the copies against the same inputs with isolated outputs, and compares them to the **baseline** (an exact copy of the source config).

Everything lives in this folder. Nothing is tied to a workspace: catalog, schema, prefix, region, warehouse and model endpoint are widgets.

## When to use / not use

* Use for: classic job-cluster jobs on **Azure** (notebook, JAR, Python, wheel, dbt tasks), single or multi-task, with jobs clusters or inline task clusters.
* Not for: AWS/GCP (VM name parsing and the price API are Azure-specific; see `references/troubleshooting.md` for what to change), streaming/continuous jobs, Lakeflow Spark Declarative Pipelines (use the pipeline pages), all-purpose clusters (`existing_cluster_id` tasks are reported but not benchmarked).

## Prerequisites to check first

1. The package is in a **Git folder** (or imported as notebooks). The `*.py` files with a `# Databricks notebook source` header must be notebooks; `notebooks/bench_common.py` must stay a plain file (it is imported as a module).
2. A catalog where the user can create schemas, tables and views (`bench_catalog`), and an output catalog for the per-variant schemas (`output_catalog`, defaults to the same).
3. SELECT on the system tables used by the cost view: `system.billing` (`usage`, `list_prices`), `system.compute` (`node_timeline`, `clusters`, `node_types`) and `system.lakeflow` (`job_run_timeline`, `jobs`). Without them the benchmark runs but cost is empty.
4. Outbound HTTPS to `prices.azure.com` from the notebook compute (VM prices), or a pre-loaded price table.
5. The source job must take its **output location as a job parameter** (default name `output_schema`). If outputs are hard-coded, the copies would overwrite production outputs: stop and tell the user. Inputs are read directly and must not change during the benchmark.
6. Cluster quota for the variant sizes in the region, and permission to create jobs.

## Shared widgets (every notebook)

| Widget | Default | Meaning |
| --- | --- | --- |
| `bench_catalog` | (required) | Catalog for state tables, price table, cost view, reporting views |
| `bench_schema` | `job_benchmarking` | Schema in that catalog (`cfg.fqn = bench_catalog.bench_schema`) |
| `output_catalog` | = bench_catalog | Where `<prefix>_<benchmark_id>_<variant>` output schemas are created |
| `bench_prefix` | `BENCH` | Job names: `[<prefix>-<benchmark_id>] <source> \| <variant>` |
| `benchmark_id` | printed by 01 | 8-char ID that groups one benchmark of one source job |

Use the same four config values in every notebook of a benchmark.

## Workflow

Run the notebooks in `notebooks/` in order. Notebooks that cost money default to `dry_run = true`: always show the dry run to the user and get explicit confirmation before setting `dry_run = false`.

| Step | Notebook | What it does | Side effects |
| --- | --- | --- | --- |
| 0 | `00_setup_cost_model` | Loads Azure VM prices for `compute_region` (auto-detected on classic compute; ask the user on serverless), creates `job_run_estimated_cost` view | Tables/views only. Once per workspace |
| (opt) | `../test_job/00_create_test_job` | Creates test data + a 3-task, 2-cluster test job | Creates a job |
| 1 | `01_discovery` (`job_id`) | Reads the source job, saves clusters + task mapping, prints `benchmark_id` | Tables only; source job untouched |
| 2 | `02_variant_generator` | One-factor-at-a-time variants, validated against the workspace node catalog | Tables only |
| 3 | `03_job_copier` | One job per valid variant (tags, isolated output schema, no schedule/notifications, `max_concurrent_runs=1`) | **Creates jobs + schemas** when `dry_run=false` |
| 4 | `04_orchestrator` | Runs the copies (batched/parallel/sequential), syncs runs from the Jobs API, records code version, optional input guard | **Starts runs (cost)** when `dry_run=false` |
| 5 | `05_analysis` | Medians, Pareto, picks, verdicts, AI recommendation, `rpt_*` views | Tables/views; calls `ai_query` (`ai_model` blank = auto-detect, `none` = skip) |
| 6 | `06_deploy_reporting` | Creates/updates the dashboard and Genie agent from `templates/` and publishes | Creates dashboard + Genie agent |

Recommended plan:

1. **Screening**: `max_runs = 1` for all variants (batched, batch size 3).
2. Wait a few hours for system tables, re-run `05_analysis` until no "cost pending" warning.
3. **Confirmation**: re-run `04` with `variants_filter = baseline,<candidates printed by 05>` and `max_runs = 2` or `3`, then `05` again.
4. Run `06` once; afterwards the dashboard and Genie agent update automatically because they read the views.

Notebooks are idempotent: re-running reuses existing jobs (matched by name), skips variants that already have enough runs, and replaces only this benchmark's rows. Variants that already have a job keep their stored definition.

## Helping the user with each step

* Before 03: read the dry-run output with the user. Check the output-parameter warning and the serverless blockers.
* Before 04: estimate cost as roughly `(number of variants x max_runs) x baseline cost per run` (baseline cost is known after the first run). Ask for confirmation.
* Long runs: 04 polls every 30 s and can run for hours. If the session dies, re-run it: nothing is lost, runs are re-synced from the Jobs API.
* After 05: summarise using `rpt_variant_results` and the verdicts, not raw minutes. Follow `references/interpreting-results.md`.
* Recommendations are applied by the user in their job definition (git/bundle). Never edit the source job.

## Safety rules

* Never modify or run the **source** job. Only `[<prefix>-<benchmark_id>]` copies are created and run.
* Never set `dry_run = false` in 03 or 04 without the user's explicit confirmation in the conversation.
* Never drop `bench_*` tables automatically. 04 stops on a schema mismatch; dropping is the user's decision.
* Cleanup (deleting benchmark jobs and `<prefix>_<benchmark_id>_*` schemas) is destructive: list what would be deleted and ask first.

## Reference files

* [references/variants.md](references/variants.md) - every variant dimension, how it is derived, when it is skipped or invalid, serverless conversion rules.
* [references/cost-model.md](references/cost-model.md) - how DBU and VM cost are estimated, Spot assumption, system-table lag, known gaps.
* [references/interpreting-results.md](references/interpreting-results.md) - task-only vs total time, noise threshold, verdicts, picks, how to word a recommendation.
* [references/data-model.md](references/data-model.md) - every table and view, keys and columns; example queries.
* [references/troubleshooting.md](references/troubleshooting.md) - common errors and fixes, porting to AWS/GCP.
