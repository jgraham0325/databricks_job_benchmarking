# Variants

`02_variant_generator` produces one-factor-at-a-time variants from the clusters found by `01_discovery`. Every variant changes exactly one dimension across all clusters of the job (a variant that cannot be applied to every cluster is stored with `is_valid = false`). The overrides are stored per cluster key in `bench_variants.overrides_json`; `03_job_copier` applies them to a deep copy of the source job.

Inline task clusters (`new_cluster` on a task) are handled like job clusters with the key `_task_<task_key>`. Tasks on all-purpose clusters (`existing_cluster_id`) are reported and left as is.

## Dimensions

| variant_name | dimension | Change | Generated when |
| --- | --- | --- | --- |
| `baseline` | baseline | none (exact copy, availability as in the source) | always |
| `half_workers_2x_size` | shape | worker VM with 2x vCPU, worker count scaled so total worker cores match | 2x VM exists in the workspace |
| `2x_workers_half_size` | shape | worker VM with 0.5x vCPU, worker count doubled | 0.5x VM exists |
| `autoscale_off` | autoscale | autoscaling clusters fixed at `max_workers` | at least one cluster autoscales |
| `vm_v<N>` | vm_gen | same family/size/features in generation N (driver too, if available) | per `vm_generations` widget, skipped if all clusters are already on N |
| `cache_on` / `cache_off` | cache | toggle the `d` (local SSD, delta cache) feature, opposite of the first cluster's state | VM name parses |
| `spot_with_fallback` | availability | on-demand clusters -> `SPOT_WITH_FALLBACK_AZURE`, keeps `first_on_demand` (min 1, the driver) | at least one on-demand cluster. Spot clusters are never tested as on-demand |
| `photon_on` / `photon_off` | photon | toggle `runtime_engine` | always |
| `dbr_<x_y>` | dbr | `spark_version` from `dbr_candidates` | skipped if all clusters already use that version |
| `shuffle_auto` / `shuffle_200` | shuffle | adds `spark.sql.shuffle.partitions` = `auto` / `200` | skipped if every cluster already uses the value (unset = 200) |
| `serverless_standard` | serverless | all tasks on serverless jobs compute, `performance_target = STANDARD` | `include_serverless = true` |
| `serverless_perf_optimized` | serverless | same, `PERFORMANCE_OPTIMIZED` | `include_serverless = true` |

### Shape details

* The driver keeps its size; only the worker shape changes.
* Fixed counts and autoscale min/max are scaled separately by `old_cores / new_cores` and rounded (at least 1). When a bound cannot match exactly (for example min 1 x D4 -> 2x size cannot be 0.5 x D8), the variant is still valid and the description says `worker capacity differs from baseline: ...`. Mention this when interpreting a shape result.
* Node types are checked against `clusters.list_node_types()` of the workspace (deprecated types excluded), so the same code works in any Azure region.

### Azure VM name parsing

`Standard_{Family}{vCPU}{features}[_{accelerator}]_v{gen}`, for example `Standard_E8ds_v5` = E family, 8 vCPU, `d` local disk, `s` premium storage, v5. Features are kept in canonical order `a d i l p s`. Names that do not match (old series such as `Standard_DS3_v2` are fine; very old `Standard_A*` may not be) produce no shape/gen/cache variants.

## Serverless conversion

The generator builds a per-task plan; the copier applies it (`_to_serverless`) and checks the result (`check_serverless`). The variant is invalid (with a reason) when any task has a blocker:

| Task type | Handling | Blocker |
| --- | --- | --- |
| notebook | compute removed, no `environment_key` | task has `libraries` (use `%pip` / notebook environment instead) |
| spark_jar | JAR libraries -> `environments[].spec.java_dependencies` | DBR is not Scala 2.13 (serverless env 4 = Scala 2.13 / JDK 17), non-JAR libraries (e.g. Maven), no JAR library |
| spark_python / python_wheel | `whl`, PyPI (default index) and `requirements` -> `dependencies` | other library types, custom PyPI repos |
| dbt | gets `dbt-databricks` in its environment | none |
| spark_submit | - | always a blocker |
| for_each | - | nested tasks not converted |
| sql, pipeline, run_job, condition, dashboard | no compute needed | none |

Cluster `spark_conf` and init scripts are dropped and listed in the description. Session-settable confs (e.g. `spark.sql.shuffle.partitions`) can be set in code instead. Node type, Spot, DBR, Photon and access mode are managed by serverless.

`STANDARD` mode has a 4-6 minute startup and is cheaper; `PERFORMANCE_OPTIMIZED` starts in seconds at a higher DBU price. Serverless cost is all DBUs (VMs included), so `vm_usd = 0` is expected.

## Adding a dimension

Add a block to `generate_variants()` in `02_variant_generator` that returns `_variant(name, dimension, description, {cluster_key: {override: value}})`. Supported override keys (see `_apply_to_cluster` in 03): `worker_node_type`, `driver_node_type`, `num_workers`, `min_workers`, `max_workers`, `spark_version`, `runtime_engine`, `azure_availability`, `first_on_demand`, `spark_conf_add`. New keys must be added to `_apply_to_cluster` too.

## Changing a variant after its job exists

Variants with a job in `bench_jobs` keep their stored definition so results stay tied to what ran; 02 prints `kept stored ...` if the regenerated overrides differ. To test a changed definition, start a new benchmark (blank `benchmark_id` in 01).
