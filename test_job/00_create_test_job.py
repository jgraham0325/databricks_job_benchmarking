# Databricks notebook source
# MAGIC %md
# MAGIC # Create the benchmark test job (optional)
# MAGIC
# MAGIC Creates a small, known workload to try the framework end to end before pointing it at a customer job:
# MAGIC
# MAGIC * `bench_test_events` (100K rows, deterministic) and `bench_test_products` (50 rows) in the benchmark schema
# MAGIC * job **`[<prefix>-TEST] Multi-Task Benchmark Test Job`**: `transform_a` and `transform_b` on `shared_cluster` (E8ds_v5 x4, on-demand, no Photon, two Spark confs), `aggregate_c` on `agg_cluster` (D4ds_v5 autoscale 1-3, Spot with fallback, Photon), job parameters `input_schema` and `output_schema`.
# MAGIC
# MAGIC Re-running reuses the job (updates it in place). A full benchmark of this job costs a few USD.

# COMMAND ----------

# DBTITLE 1,Configuration
import os, sys
HERE = os.getcwd()
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "notebooks")))
import bench_common as bc
from pyspark.sql import functions as F

cfg = bc.setup(dbutils, spark)
w = bc.w
dbutils.widgets.text("spark_version", "15.4.x-scala2.12", "DBR for the test job clusters")
spark_version = dbutils.widgets.get("spark_version").strip()
JOB_NAME = f"[{cfg.prefix}-TEST] Multi-Task Benchmark Test Job"

# COMMAND ----------

# DBTITLE 1,Test data (deterministic)
REGIONS = F.array(*[F.lit(r) for r in ["north", "south", "east", "west", "central"]])
CATEGORIES = F.array(*[F.lit(c) for c in ["electronics", "home", "garden", "toys", "sports"]])

products = (spark.range(1, 51).withColumnRenamed("id", "product_id")
            .withColumn("product_name", F.concat(F.lit("product_"), F.col("product_id")))
            .withColumn("category", F.element_at(CATEGORIES, (F.col("product_id") % 5 + 1).cast("int")))
            .withColumn("unit_cost", F.round(F.pmod(F.hash("product_id", F.lit("cost")), 4000) / 100 + 2, 2)))
events = (spark.range(100_000)
          .withColumn("event_date", F.date_sub(F.lit("2025-01-01").cast("date"), F.pmod(F.hash("id", F.lit("d")), 60).cast("int")))
          .withColumn("product_id", (F.pmod(F.hash("id", F.lit("p")), 50) + 1).cast("long"))
          .withColumn("quantity", (F.pmod(F.hash("id", F.lit("q")), 10) + 1).cast("int"))
          .withColumn("amount", F.round(F.pmod(F.hash("id", F.lit("a")), 10000) / 100, 2))
          .withColumn("region", F.element_at(REGIONS, (F.pmod(F.hash("id", F.lit("r")), 5) + 1).cast("int")))
          .drop("id"))
products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{cfg.fqn}.bench_test_products")
events.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{cfg.fqn}.bench_test_events")
print(f"{cfg.fqn}.bench_test_events: {spark.table(f'{cfg.fqn}.bench_test_events').count():,} rows")
print(f"{cfg.fqn}.bench_test_products: {spark.table(f'{cfg.fqn}.bench_test_products').count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create or update the test job
def _notebook_path(name):
    """Workspace path of a task notebook next to this one (works in Git folders and after import)."""
    base = os.path.join(HERE, name).replace("/Workspace", "", 1)
    for p in (base, base + ".py"):
        try:
            st = w.workspace.get_status(p)
        except Exception:
            continue
        if bc.ev(st.object_type) == "NOTEBOOK":
            return p
        raise RuntimeError(f"{p} is a plain file, not a notebook. Clone the package into a Git folder or import "
                           f"test_job/*.py as notebooks (Workspace > Import), then re-run.")
    raise RuntimeError(f"Task notebook {name} not found next to this notebook")

bc.load_node_catalog()
for nt in ("Standard_E8ds_v5", "Standard_D4ds_v5"):
    assert bc.valid_node(nt), f"{nt} is not available in this workspace: edit the cluster specs below"

def _task(key, nb, cluster, deps=()):
    return {"task_key": key, "job_cluster_key": cluster, "depends_on": [{"task_key": d} for d in deps],
            "notebook_task": {"notebook_path": _notebook_path(nb), "source": "WORKSPACE",
                              "base_parameters": {"input_schema": "{{job.parameters.input_schema}}",
                                                  "output_schema": "{{job.parameters.output_schema}}"}}}

settings = {
    "name": JOB_NAME,
    "max_concurrent_runs": 1,
    "tags": {"purpose": "benchmark_test"},
    "parameters": [{"name": "input_schema", "default": cfg.fqn},
                   {"name": "output_schema", "default": f"{cfg.fqn}_test_output"}],
    "tasks": [_task("transform_a", "task_transform_a", "shared_cluster"),
              _task("transform_b", "task_transform_b", "shared_cluster"),
              _task("aggregate_c", "task_aggregate_c", "agg_cluster", ["transform_a"])],
    "job_clusters": [
        {"job_cluster_key": "shared_cluster", "new_cluster": {
            "spark_version": spark_version, "node_type_id": "Standard_E8ds_v5", "driver_node_type_id": "Standard_E8ds_v5",
            "num_workers": 4, "runtime_engine": "STANDARD", "data_security_mode": "SINGLE_USER",
            "spark_conf": {"spark.sql.shuffle.partitions": "200", "spark.databricks.delta.optimizeWrite.enabled": "true"},
            "azure_attributes": {"availability": "ON_DEMAND_AZURE", "first_on_demand": 1}}},
        {"job_cluster_key": "agg_cluster", "new_cluster": {
            "spark_version": spark_version, "node_type_id": "Standard_D4ds_v5", "driver_node_type_id": "Standard_D4ds_v5",
            "autoscale": {"min_workers": 1, "max_workers": 3}, "runtime_engine": "PHOTON", "data_security_mode": "SINGLE_USER",
            "azure_attributes": {"availability": "SPOT_WITH_FALLBACK_AZURE", "first_on_demand": 1}}},
    ],
}

existing = [j for j in w.jobs.list(name=JOB_NAME)]
if existing:
    job_id = existing[0].job_id
    w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})
    print(f"Updated test job {job_id}")
else:
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
    print(f"Created test job {job_id}")
print(f"\nNext: run notebooks/01_discovery with job_id = {job_id} and the same bench_catalog / bench_schema.")
print(f"In 03_job_copier keep output_param_name = output_schema; in 04_orchestrator you can set "
      f"input_tables = {cfg.fqn}.bench_test_events,{cfg.fqn}.bench_test_products")
dbutils.notebook.exit(str(job_id))
