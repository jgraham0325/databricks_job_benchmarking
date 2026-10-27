# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 Job copier
# MAGIC
# MAGIC Creates one benchmark job per valid variant: a copy of the source job with the variant's cluster overrides, an isolated output schema (`<output_catalog>.<prefix>_<benchmark_id>_<variant>`, wired into the output parameter), benchmark tags on the job and every cluster (they flow to `system.billing.usage`), `max_concurrent_runs = 1`, and no schedule, triggers or notifications. Idempotent: existing jobs with the same name are reused.
# MAGIC
# MAGIC **Run with `dry_run = true` first.** With `dry_run = false` it creates jobs and schemas (no runs are started).

# COMMAND ----------

# DBTITLE 1,Reload edited modules automatically
# Picks up edits to the bench_*.py modules without restarting Python
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# DBTITLE 1,Configuration and variants
import os, sys, json
sys.path.insert(0, os.getcwd())
import bench_common as bc
import bench_copier as bcp   # copy logic (tested in ../tests/test_copier.py)

cfg = bc.setup(dbutils, spark)
w = bc.w
benchmark_id = bc.benchmark_id_widget(dbutils)
dbutils.widgets.text("output_param_name", "output_schema", "Job parameter that sets the output schema")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (preview only)")
dbutils.widgets.text("timeout_seconds", "14400", "Timeout per benchmark run (seconds, if the source has none)")
output_param_name = dbutils.widgets.get("output_param_name").strip()
dry_run = dbutils.widgets.get("dry_run") == "true"
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))
NAME_PREFIX = bcp.name_prefix(cfg.prefix, benchmark_id)

variants = [r.asDict() for r in spark.table(f"{cfg.fqn}.bench_variants")
            .filter(f"benchmark_id = '{benchmark_id}' AND is_valid = true").collect()]
assert variants, f"No valid variants for {benchmark_id}: run 02_variant_generator first"
source_job_ids = sorted({v["source_job_id"] for v in variants})
print(f"Benchmark {benchmark_id} | output param '{output_param_name}' | dry run {dry_run} | "
      f"{len(variants)} valid variants for job(s) {source_job_ids}")

# COMMAND ----------

# DBTITLE 1,Override logic and pre-flight checks
# Overrides, isolation (output schema), tags, schedule/notification removal and the serverless conversion
# live in bench_copier and are unit tested in ../tests/test_copier.py. This cell only reads the source jobs.
source_jobs = {jid: w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": int(jid)})["settings"] for jid in source_job_ids}

check_serverless = bcp.check_serverless

def prepare_copy(source, variant):
    """Settings for one benchmark job copy and its output schema (the source dict is not modified)."""
    return bcp.prepare_copy(source, variant, benchmark_id=benchmark_id, prefix=cfg.prefix,
                            output_catalog=cfg.output_catalog, output_param_name=output_param_name,
                            timeout_seconds=timeout_seconds)

for jid, s in source_jobs.items():
    for msg in bcp.preflight_messages(jid, s, output_param_name):
        print(msg)

# COMMAND ----------

# DBTITLE 1,Create benchmark job copies
existing = {j.settings.name: str(j.job_id) for j in w.jobs.list()
            if j.settings and j.settings.name and j.settings.name.startswith(NAME_PREFIX)}
print(f"{len(existing)} job(s) already exist with prefix {NAME_PREFIX}")

created = []
for jid in source_job_ids:
    for v in [x for x in variants if x["source_job_id"] == jid]:
        new_s, out_schema = prepare_copy(source_jobs[jid], v)
        row = dict(benchmark_id=benchmark_id, source_job_id=jid, variant_id=v["variant_id"], variant_name=v["variant_name"],
                   new_job_name=new_s["name"], output_schema=out_schema)
        ov = json.loads(v["overrides_json"] or "{}")
        print(f"  {v['variant_name']:<28} changes: " + ("; ".join(f"{k}: {list(c)}" for k, c in ov.items()) or "(none)"))
        if "_serverless" in ov:
            probs = check_serverless(new_s)
            if probs:
                print(f"      INVALID serverless job: {probs}")
                created.append({**row, "new_job_id": None, "status": "error: " + "; ".join(probs)})
                continue
            if dry_run:
                print(json.dumps({"performance_target": new_s.get("performance_target"), "environments": new_s.get("environments"),
                                  "tasks": [{k: t[k] for k in t if k in ("task_key", "environment_key") or k.endswith("_task")}
                                            for t in new_s["tasks"]]}, indent=2))
        if new_s["name"] in existing:
            row.update(new_job_id=existing[new_s["name"]], status="exists")
            print(f"      = existing job {row['new_job_id']}")
        elif dry_run:
            row.update(new_job_id=None, status="dry_run")
        else:
            try:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {out_schema}")
                res = w.api_client.do("POST", "/api/2.2/jobs/create", body=new_s)
                row.update(new_job_id=str(res["job_id"]), status="created")
                print(f"      -> job {res['job_id']}  output {out_schema}")
            except Exception as e:
                row.update(new_job_id=None, status=f"error: {e}")
                print(f"      ERROR {e}")
        created.append(row)
print(f"\n{len(created)} job record(s) ({'DRY RUN' if dry_run else 'created/reused'})")

# COMMAND ----------

# DBTITLE 1,Save job records (upsert this benchmark only)
from delta.tables import DeltaTable
JOBS_TBL = f"{cfg.fqn}.bench_jobs"
COLS = ["benchmark_id", "source_job_id", "variant_id", "variant_name", "new_job_name", "output_schema", "new_job_id", "status"]
to_save = [r for r in created if r.get("new_job_id")]  # dry-run/error rows are printed, never stored
if to_save:
    df = spark.createDataFrame([tuple(str(r[c]) for c in COLS) for r in to_save], ", ".join(f"{c} string" for c in COLS))
    if not spark.catalog.tableExists(JOBS_TBL):
        df.write.saveAsTable(JOBS_TBL)
    else:
        (DeltaTable.forName(spark, JOBS_TBL).alias("t")
         .merge(df.alias("s"), "t.benchmark_id = s.benchmark_id AND t.variant_name = s.variant_name")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    print(f"Upserted {len(to_save)} job record(s) into {JOBS_TBL}")
    display(spark.table(JOBS_TBL).filter(f"benchmark_id = '{benchmark_id}'")
            .select("variant_name", "new_job_id", "new_job_name", "output_schema", "status").orderBy("variant_name"))
else:
    print("Nothing saved (dry run or no jobs).")
