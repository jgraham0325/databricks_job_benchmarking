# Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError: bench_common` | Notebook not in the same folder as `bench_common.py`, or `bench_common.py` was imported as a notebook | Keep the folder layout; `bench_common.py` must be a workspace **file** (no notebook header). Needs DBR 14.3+ or serverless (cwd = notebook folder) |
| `Set the bench_catalog widget ...` | Required widget empty | Set `bench_catalog` (and the same values in every notebook) |
| 00: `Set compute_region` | Region is only detectable on classic compute (not serverless) | Set the Azure `armRegionName` of the workspace, e.g. `northeurope` |
| 00: connection error to `prices.azure.com` | No outbound internet | Load `azure_vm_retail_prices` from a machine with access (same columns), run 00 with `refresh_prices = false` |
| 00: `permission denied` on `system.*` | No system table access | Ask an admin to grant SELECT on `system.billing`, `system.compute`, `system.lakeflow` |
| 02: most variants `INVALID: No matching node type` | Target VM sizes not offered in the region / workspace | Expected; the variant is skipped. Change `vm_generations` |
| 02: serverless variants invalid | Blocker listed in `invalid_reason` (JAR on Scala 2.12, Maven libs, notebook libraries...) | Report to the user; serverless needs code/packaging changes first. Set `include_serverless = false` to hide them |
| 03: `WARNING ... parameter 'output_schema' not found` | Source job has no output parameter | **Stop.** Find the job's real output parameter (`output_param_name`) or have the job parameterised. Otherwise variants write to production outputs |
| 03: `jobs/create` 400 on a serverless variant | Workspace without serverless jobs, or environment version unsupported | Check serverless is enabled; try `serverless_env_version` 2 or 3 for non-JAR jobs |
| 03: quota / `QuotaExceeded` at run time | Regional vCPU quota | Reduce `batch_size`, run `sequential`, or request quota |
| 04: `schema differs ... DROP it manually` | Tracking table from an older version | Drop `bench_runs` / `bench_run_tasks` after confirming with the user; 04 re-syncs from the Jobs API (60-day run history) |
| 04: a run is `SKIPPED` | Two runs of the same variant overlapped (`max_concurrent_runs = 1`) | Re-run 04; skipped runs are re-queued |
| 04: session timed out during polling | Long benchmark | Re-run 04; nothing is lost. For many variants run 04 as a job |
| 04: `input tables changed` | Something wrote to an input during the benchmark | Results may not be comparable; find the writer (Delta history), re-run affected variants in a new benchmark |
| 05: `cost pending` | System table lag | Re-run 05 in a few hours |
| 05: `VM cost incomplete or suspect` | Missing `node_timeline` rows for a cluster | See cost-model.md. Re-run that variant; open a support ticket if it persists |
| 05: `ai_query ... failed` | Endpoint name not available | Set `ai_model` to a chat endpoint listed under Serving in this workspace, or `none` to skip (verdicts are still saved) |
| 06: dashboard shows `Table not found` | Views missing or warehouse lacks access | Run 05 first; grant the dashboard owner / viewers SELECT on the `rpt_*` views and the cost view's sources |
| 06: Genie link in the dashboard is broken | Genie agent recreated | Re-run 06; it rewrites the link |

## Cleanup

Benchmark jobs and output schemas stay until removed. To clean up a benchmark, list `bench_jobs` for the `benchmark_id`, confirm with the user, then delete the jobs and `DROP SCHEMA <output_schema> CASCADE`. State tables can keep the history (they are small).

## Porting to AWS or GCP

The Azure-specific parts are isolated:

* `bench_common.py`: `_VM_RE`, `parse_vm`, `scale_vm`, `change_gen`, `toggle_cache` (Azure VM naming). For AWS, parse `<family><gen><features>.<size>` (e.g. `r6id.2xlarge`): gen and `d` (local NVMe) map directly, size scales through `large/xlarge/2xlarge/...`.
* `02_variant_generator`: availability values (`SPOT_WITH_FALLBACK_AZURE` -> `SPOT_WITH_FALLBACK` under `aws_attributes`).
* `03_job_copier`: `_apply_to_cluster` writes `azure_attributes`.
* `00_setup_cost_model`: VM price source (AWS Price List API / GCP Billing Catalog) and the `azure_attributes` columns of `system.compute.clusters`.

The orchestrator, analysis, reporting and templates are cloud-neutral.
