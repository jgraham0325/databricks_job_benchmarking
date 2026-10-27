# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 Variant generator
# MAGIC
# MAGIC Generates one-factor-at-a-time variants from the discovered clusters. Each variant changes a single dimension (worker shape, autoscaling, VM generation, delta cache, Spot, Photon, DBR, shuffle partitions, or serverless) and keeps everything else as in the baseline. Variants that are impossible in this workspace (node type not available, task type not supported on serverless) are stored with `is_valid = false` and a reason. See `references/variants.md`.

# COMMAND ----------

# DBTITLE 1,Reload edited modules automatically
# Picks up edits to the bench_*.py modules without restarting Python
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# DBTITLE 1,Configuration
import os, sys, json
sys.path.insert(0, os.getcwd())
import bench_common as bc
import bench_variants as bv   # variant rules (tested in ../tests/test_variants.py)

cfg = bc.setup(dbutils, spark)
w = bc.w
benchmark_id = bc.benchmark_id_widget(dbutils)

dbutils.widgets.text("dbr_candidates", "15.4.x-scala2.12,16.4.x-scala2.12", "DBR versions to test (comma-separated)")
dbutils.widgets.text("vm_generations", "4,5,6", "Azure VM generations to test")
# Serverless is optional: some workloads can't run on it. When included, two variants are generated:
#   STANDARD (cheaper, 4-6 min startup) and PERFORMANCE_OPTIMIZED (starts in seconds, higher price).
dbutils.widgets.dropdown("include_serverless", "true", ["true", "false"], "Include serverless variants")
# Environment 4 = Scala 2.13 / JDK 17 (minimum for JAR tasks on serverless)
dbutils.widgets.text("serverless_env_version", "4", "Serverless environment version")

DBR_CANDIDATES = [v.strip() for v in dbutils.widgets.get("dbr_candidates").split(",") if v.strip()]
VM_GENERATIONS = [int(g) for g in dbutils.widgets.get("vm_generations").split(",") if g.strip()]
include_serverless = dbutils.widgets.get("include_serverless") == "true"
serverless_env_version = dbutils.widgets.get("serverless_env_version").strip()
SERVERLESS_MODES = bv.SERVERLESS_MODES
print(f"Benchmark {benchmark_id} | DBR {DBR_CANDIDATES} | VM gens {VM_GENERATIONS} | serverless {include_serverless} (env v{serverless_env_version})")

# COMMAND ----------

# DBTITLE 1,Load discovered clusters and node catalog
clusters = [r.asDict() for r in spark.table(f"{cfg.fqn}.bench_source_clusters")
            .filter(f"benchmark_id = '{benchmark_id}'").collect()]
assert clusters, f"No clusters for benchmark_id={benchmark_id}: run 01_discovery first"
job_clusters = {}
for c in clusters:
    job_clusters.setdefault(c["job_id"], []).append(c)
bc.load_node_catalog()
for jid, jc in job_clusters.items():
    print(f"Job {jid}: {[c['job_cluster_key'] for c in jc]}")

# COMMAND ----------

# DBTITLE 1,Generate one-factor-at-a-time variants
# The rules (shape, autoscale, VM generation, cache, Spot, Photon, DBR, shuffle, serverless) live in
# bench_variants.generate_variants and are unit tested in ../tests/test_variants.py. This cell only does the I/O.
def _cluster_dbr(cluster_id):
    """DBR of an all-purpose cluster (tasks with existing_cluster_id, for the serverless check)."""
    return w.clusters.get(cluster_id).spark_version

all_variants = []
for jid, jc in job_clusters.items():
    settings = (w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": int(jid)})["settings"]
                if include_serverless else None)
    vs = bv.generate_variants(jid, jc, benchmark_id=benchmark_id, vm_generations=VM_GENERATIONS,
                              dbr_candidates=DBR_CANDIDATES, include_serverless=include_serverless,
                              serverless_env_version=serverless_env_version, job_settings=settings,
                              cluster_dbr=_cluster_dbr)
    all_variants.extend(vs)
    print(f"Job {jid}: {len(vs)} variants ({sum(x['is_valid'] for x in vs)} valid)")

# Several source jobs in one benchmark would collide on variant_name; benchmark one job per benchmark_id
assert len(job_clusters) == 1, "Use one source job per benchmark_id (run 01_discovery once per job)"

# COMMAND ----------

# DBTITLE 1,Save variants (upsert this benchmark only) and show the matrix
from delta.tables import DeltaTable
VAR_TBL, JOBS_TBL = f"{cfg.fqn}.bench_variants", f"{cfg.fqn}.bench_jobs"
COLS = ["variant_name", "dimension", "description", "is_valid", "invalid_reason", "benchmark_id", "source_job_id",
        "variant_id", "overrides_json"]
DDL = ("variant_name string, dimension string, description string, is_valid boolean, invalid_reason string, "
       "benchmark_id string, source_job_id string, variant_id string, overrides_json string")

# Variants that already have a benchmark job keep their stored definition, so results stay tied to the
# config that ran. A changed definition is reported, not overwritten (use a new benchmark_id instead).
has_job = set()
if spark.catalog.tableExists(JOBS_TBL):
    has_job = {r.variant_name for r in spark.table(JOBS_TBL).filter(f"benchmark_id = '{benchmark_id}' AND new_job_id IS NOT NULL").collect()}
stored = {}
if spark.catalog.tableExists(VAR_TBL):
    stored = {r.variant_name: r.overrides_json for r in spark.table(VAR_TBL).filter(f"benchmark_id = '{benchmark_id}'").collect()}
to_save = []
for x in all_variants:
    if x["variant_name"] in has_job:
        if stored.get(x["variant_name"]) != x["overrides_json"]:
            print(f"  kept stored '{x['variant_name']}' (its job exists; regenerated overrides differ)")
        continue
    to_save.append(x)
if to_save:
    df = spark.createDataFrame([tuple(x.get(c) for c in COLS) for x in to_save], DDL)
    if not spark.catalog.tableExists(VAR_TBL):
        df.write.saveAsTable(VAR_TBL)
    else:
        (DeltaTable.forName(spark, VAR_TBL).alias("t")
         .merge(df.alias("s"), "t.benchmark_id = s.benchmark_id AND t.variant_name = s.variant_name")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"Upserted {len(to_save)} variant(s); kept {len(all_variants) - len(to_save)} that already have jobs")

for i, x in enumerate(all_variants, 1):
    flag = "ok " if x["is_valid"] else "INVALID"
    print(f"{i:>2}. {x['variant_name']:<28} {x['dimension']:<12} {flag:<8} {x['description']}"
          + (f"  ({x['invalid_reason']})" if x["invalid_reason"] else ""))
display(spark.table(VAR_TBL).filter(f"benchmark_id = '{benchmark_id}'")
        .select("variant_name", "dimension", "is_valid", "description", "overrides_json", "invalid_reason")
        .orderBy("dimension", "variant_name"))
