# Data model

All objects are in `<bench_catalog>.<bench_schema>` (below: `<fqn>`). Every state table has `benchmark_id`; notebooks only replace or merge rows of the benchmark they run for, so many benchmarks can share the schema.

## State tables

| Table | Written by | Grain / key | Main columns |
| --- | --- | --- | --- |
| `bench_source_jobs` | 01 (replace this benchmark's rows) | benchmark_id, job_id | job_name, job_params (JSON defaults), tasks (JSON: task_key, cluster_key, cluster_source, task_type, task_path, depends_on), num_tasks, num_clusters, has_existing_cluster |
| `bench_source_clusters` | 01 (replace) | benchmark_id, job_id, job_cluster_key | worker/driver_node_type, num_workers or min/max_workers, spark_version, runtime_engine, spark_conf_json, azure_availability, first_on_demand, instance_pool_id, policy_id, custom_tags_json, init_scripts_json, data_security_mode, tasks_on_cluster |
| `bench_variants` | 02 (MERGE) | benchmark_id, variant_name | variant_id (`<benchmark_id>_<variant_name>`), source_job_id, dimension, description, overrides_json, is_valid, invalid_reason |
| `bench_jobs` | 03 (MERGE) | benchmark_id, variant_name | new_job_id, new_job_name, output_schema, status (created / exists) |
| `bench_runs` | 04 (MERGE, synced from Jobs API) | run_id | variant_name, new_job_id, run_number, start/end_time, duration_seconds, task_only_seconds, task_exec_sum_seconds, overhead_seconds, max_setup_seconds, n_tasks, life_cycle_state, result_state, status (SUCCESS / FAILED / SKIPPED / RUNNING) |
| `bench_run_tasks` | 04 (MERGE) | task_run_id | run_id, task_key, attempt_number, job_cluster_key, cluster_id, queue/setup/execution/cleanup_seconds, result_state |
| `bench_run_code_versions` | 04 (MERGE) | run_id | code_version (12-char sha of the code manifest), code_label, manifest_json, version_source |
| `bench_results` | 05 (replace) | benchmark_id, variant_name | medians, deltas vs baseline, spreads, picks_*, is_pareto_*, confidence_task_only, vm_cost_suspect, overrides_json, code_version, source_job_ids/names |
| `bench_recommendations` | 05 (replace) | benchmark_id | recommendation_md, model, code_version, generated_at |
| `bench_reporting_assets` | 06 (MERGE) | asset_type, asset_name | asset_id of the dashboard / Genie agent |
| `azure_vm_retail_prices` | 00 (overwrite) | price row | armSkuName, armRegionName, retailPrice, type, effectiveStartDate |

Test job only: `bench_test_events`, `bench_test_products`.

## Views

| View | Grain | Used by |
| --- | --- | --- |
| `job_run_estimated_cost` | job_id, run_id (any job in the workspace, 90 days) | 05, rpt_runs |
| `rpt_variant_results` | benchmark_id, variant_name | dashboard, Genie agent |
| `rpt_runs` | run_id | dashboard, Genie agent |
| `rpt_recommendations` | benchmark_id | dashboard, Genie agent |

## Objects outside the schema

* Benchmark jobs: named `[<prefix>-<benchmark_id>] <source job name> | <variant_name>`, tagged `benchmark_id`, `variant_id`, `variant_name`, `source_job_id` (tags also on every cluster, so they appear in `system.billing.usage.custom_tags`).
* Output schemas: `<output_catalog>.<prefix>_<benchmark_id>_<variant_name>` (lowercase), one per variant.

## Useful queries

Latest benchmark summary:

```sql
SELECT variant_name, n_runs, task_only_min, total_min, cost_usd, task_only_vs_base_pct, cost_vs_base_pct,
       picks_task_only, confidence_task_only
FROM <fqn>.rpt_variant_results
WHERE benchmark_id = (SELECT benchmark_id FROM <fqn>.rpt_variant_results ORDER BY first_run_at DESC LIMIT 1)
ORDER BY task_only_min;
```

All benchmark jobs and schemas of one benchmark (for cleanup review):

```sql
SELECT variant_name, new_job_id, new_job_name, output_schema FROM <fqn>.bench_jobs WHERE benchmark_id = '<id>';
```

Billed DBUs per benchmark straight from billing (independent of the cost view):

```sql
SELECT custom_tags['variant_name'] AS variant, SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE custom_tags['benchmark_id'] = '<id>' AND usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY 1 ORDER BY 2;
```
