# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Discovery
# MAGIC
# MAGIC Reads the settings of the source job(s), extracts every job cluster (and inline task cluster) with the tasks that run on it, and saves them to `bench_source_jobs` and `bench_source_clusters` under a new (or given) `benchmark_id`. Read-only for the source jobs.

# COMMAND ----------

# DBTITLE 1,Configuration
import os, sys, json
sys.path.insert(0, os.getcwd())
import bench_common as bc

cfg = bc.setup(dbutils, spark)
w = bc.w

# One source job per benchmark_id (variant names are unique per benchmark). Benchmark another job with a new ID.
dbutils.widgets.text("job_id", "", "Source job ID")
_jid = dbutils.widgets.get("job_id").strip()
assert _jid.isdigit(), "Set job_id to the numeric ID of the job to benchmark"
job_ids = [int(_jid)]
benchmark_id = bc.benchmark_id_widget(dbutils, required=False, label="Benchmark ID (blank = new)") or bc.new_benchmark_id()
print(f"Benchmark ID: {benchmark_id}\nSource job  : {job_ids[0]}")

# COMMAND ----------

# DBTITLE 1,Fetch job settings
def _cluster_row(key, spec):
    autoscale = spec.autoscale is not None
    az = spec.azure_attributes
    return dict(
        job_cluster_key=key,
        worker_node_type=spec.node_type_id,
        driver_node_type=spec.driver_node_type_id or spec.node_type_id,
        num_workers=None if autoscale else int(spec.num_workers or 0),
        min_workers=int(spec.autoscale.min_workers) if autoscale else None,
        max_workers=int(spec.autoscale.max_workers) if autoscale else None,
        spark_version=spec.spark_version,
        runtime_engine=bc.ev(spec.runtime_engine) or "STANDARD",
        spark_conf_json=json.dumps(dict(spec.spark_conf or {})),
        azure_availability=(bc.ev(az.availability) if az and az.availability else "ON_DEMAND_AZURE"),
        first_on_demand=int(az.first_on_demand) if az and az.first_on_demand is not None else None,
        instance_pool_id=spec.instance_pool_id,
        policy_id=spec.policy_id,
        custom_tags_json=json.dumps(dict(spec.custom_tags or {})),
        init_scripts_json=json.dumps([str(s) for s in (spec.init_scripts or [])]),
        data_security_mode=bc.ev(spec.data_security_mode) or "",
    )

TASK_KINDS = [("notebook_task", "notebook", lambda t: t.notebook_path), ("spark_jar_task", "spark_jar", lambda t: t.main_class_name),
              ("spark_python_task", "spark_python", lambda t: t.python_file), ("python_wheel_task", "python_wheel", lambda t: t.package_name),
              ("sql_task", "sql", None), ("dbt_task", "dbt", None), ("pipeline_task", "pipeline", None), ("run_job_task", "run_job", None)]

jobs_rows, cluster_rows, warnings = [], [], []
for jid in job_ids:
    try:
        s = w.jobs.get(jid).settings
    except Exception as e:
        warnings.append(f"Job {jid}: {e}")
        continue
    name = s.name or f"job_{jid}"
    clusters = {jc.job_cluster_key: _cluster_row(jc.job_cluster_key, jc.new_cluster)
                for jc in (s.job_clusters or []) if jc.new_cluster}
    tasks = []
    for t in (s.tasks or []):
        if t.job_cluster_key:
            ck, src = t.job_cluster_key, "job_cluster"
        elif t.new_cluster:
            ck, src = f"_task_{t.task_key}", "task_inline"
            clusters[ck] = _cluster_row(ck, t.new_cluster)
        elif t.existing_cluster_id:
            ck, src = t.existing_cluster_id, "existing_cluster"
            warnings.append(f"Job {jid}, task {t.task_key}: runs on all-purpose cluster {ck} (not benchmarkable)")
        else:
            ck, src = None, "serverless_or_none"
            warnings.append(f"Job {jid}, task {t.task_key}: no cluster (serverless or no-compute task)")
        ttype, tpath = "unknown", ""
        for attr, label, path_fn in TASK_KINDS:
            obj = getattr(t, attr, None)
            if obj:
                ttype, tpath = label, (path_fn(obj) if path_fn else "") or ""
                break
        tasks.append(dict(task_key=t.task_key, cluster_key=ck, cluster_source=src, task_type=ttype,
                          task_path=tpath, depends_on=[d.task_key for d in (t.depends_on or [])]))

    jobs_rows.append(dict(benchmark_id=benchmark_id, job_id=str(jid), job_name=name,
                          job_params=json.dumps({p.name: p.default for p in (s.parameters or [])}),
                          tasks=json.dumps(tasks), num_tasks=len(tasks), num_clusters=len(clusters),
                          has_existing_cluster=any(t["cluster_source"] == "existing_cluster" for t in tasks)))
    for key, row in clusters.items():
        on = [t["task_key"] for t in tasks if t["cluster_key"] == key]
        cluster_rows.append(dict(benchmark_id=benchmark_id, job_id=str(jid), job_name=name, **row,
                                 tasks_on_cluster=json.dumps(on), num_tasks_on_cluster=len(on)))

print(f"Discovered {len(jobs_rows)} job(s), {len(cluster_rows)} cluster(s)")
for m in warnings:
    print("  WARNING", m)
assert jobs_rows, "No job could be read"

# COMMAND ----------

# DBTITLE 1,Save (this benchmark's rows only)
JOBS_DDL = ("benchmark_id STRING, job_id STRING, job_name STRING, job_params STRING, tasks STRING, "
            "num_tasks INT, num_clusters INT, has_existing_cluster BOOLEAN")
CLUSTERS_DDL = ("benchmark_id STRING, job_id STRING, job_name STRING, job_cluster_key STRING, worker_node_type STRING, "
                "driver_node_type STRING, num_workers INT, min_workers INT, max_workers INT, spark_version STRING, "
                "runtime_engine STRING, spark_conf_json STRING, azure_availability STRING, first_on_demand INT, "
                "instance_pool_id STRING, policy_id STRING, custom_tags_json STRING, init_scripts_json STRING, "
                "data_security_mode STRING, tasks_on_cluster STRING, num_tasks_on_cluster INT")

def _df(rows, ddl):
    cols = [c.strip().split(" ")[0] for c in ddl.split(",")]
    return spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], ddl)

bc.replace_benchmark_rows(spark, _df(jobs_rows, JOBS_DDL), f"{cfg.fqn}.bench_source_jobs", benchmark_id)
if cluster_rows:
    bc.replace_benchmark_rows(spark, _df(cluster_rows, CLUSTERS_DDL), f"{cfg.fqn}.bench_source_clusters", benchmark_id)
print(f"Saved to {cfg.fqn}.bench_source_jobs / bench_source_clusters")

# COMMAND ----------

# DBTITLE 1,Summary
print(f"benchmark_id = {benchmark_id}   <- use this in 02-05")
display(spark.table(f"{cfg.fqn}.bench_source_clusters").filter(f"benchmark_id = '{benchmark_id}'")
        .select("job_name", "job_cluster_key", "worker_node_type", "driver_node_type", "num_workers",
                "min_workers", "max_workers", "spark_version", "runtime_engine", "azure_availability",
                "tasks_on_cluster"))
dbutils.notebook.exit(benchmark_id)
