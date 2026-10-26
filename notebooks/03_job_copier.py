# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Job copier
# MAGIC
# MAGIC Creates one benchmark job per valid variant: a copy of the source job with the variant's cluster overrides, an isolated output schema (`<output_catalog>.<prefix>_<benchmark_id>_<variant>`, wired into the output parameter), benchmark tags on the job and every cluster (they flow to `system.billing.usage`), `max_concurrent_runs = 1`, and no schedule, triggers or notifications. Idempotent: existing jobs with the same name are reused.
# MAGIC
# MAGIC **Run with `dry_run = true` first.** With `dry_run = false` it creates jobs and schemas (no runs are started).

# COMMAND ----------

# DBTITLE 1,Configuration and variants
import os, sys, json, copy
sys.path.insert(0, os.getcwd())
import bench_common as bc

cfg = bc.setup(dbutils, spark)
w = bc.w
benchmark_id = bc.benchmark_id_widget(dbutils)
dbutils.widgets.text("output_param_name", "output_schema", "Job parameter that sets the output schema")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (preview only)")
dbutils.widgets.text("timeout_seconds", "14400", "Timeout per benchmark run (seconds, if the source has none)")
output_param_name = dbutils.widgets.get("output_param_name").strip()
dry_run = dbutils.widgets.get("dry_run") == "true"
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))
NAME_PREFIX = f"[{cfg.prefix}-{benchmark_id}]"

variants = [r.asDict() for r in spark.table(f"{cfg.fqn}.bench_variants")
            .filter(f"benchmark_id = '{benchmark_id}' AND is_valid = true").collect()]
assert variants, f"No valid variants for {benchmark_id}: run 02_variant_generator first"
source_job_ids = sorted({v["source_job_id"] for v in variants})
print(f"Benchmark {benchmark_id} | output param '{output_param_name}' | dry run {dry_run} | "
      f"{len(variants)} valid variants for job(s) {source_job_ids}")

# COMMAND ----------

# DBTITLE 1,Override logic and pre-flight checks
source_jobs = {jid: w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": int(jid)})["settings"] for jid in source_job_ids}

def _apply_to_cluster(spec, changes):
    for key, value in changes.items():
        if key == "worker_node_type": spec["node_type_id"] = value
        elif key == "driver_node_type": spec["driver_node_type_id"] = value
        elif key == "num_workers":
            spec["num_workers"] = int(value); spec.pop("autoscale", None)
        elif key in ("min_workers", "max_workers"):
            spec.setdefault("autoscale", {})[key] = int(value); spec.pop("num_workers", None)
        elif key in ("spark_version", "runtime_engine"): spec[key] = value
        elif key == "azure_availability": spec.setdefault("azure_attributes", {})["availability"] = value
        elif key == "first_on_demand": spec.setdefault("azure_attributes", {})["first_on_demand"] = int(value)
        elif key == "spark_conf_add": spec.setdefault("spark_conf", {}).update(value)
        else: raise ValueError(f"Unknown override key {key}")

ENV_TASKS = ("spark_jar_task", "spark_python_task", "python_wheel_task", "dbt_task")

def _to_serverless(s, spec):
    """Move every task to serverless jobs compute using the per-task plan from 02: removes job_clusters and
    task compute, moves libraries into job environments, sets environment_key where a task needs one
    (not on notebook tasks) and sets the job's performance_target."""
    s.pop("job_clusters", None)
    ver, plan, envs = str(spec.get("environment_version", "4")), spec.get("tasks", {}), {}
    for t in s.get("tasks", []):
        for k in ("job_cluster_key", "new_cluster", "existing_cluster_id", "compute_key", "libraries"):
            t.pop(k, None)
        p = plan.get(t["task_key"], {})
        if p.get("type") in ENV_TASKS:
            env_spec = {"environment_version": ver}
            if p.get("java_dependencies"): env_spec["java_dependencies"] = p["java_dependencies"]
            if p.get("dependencies"): env_spec["dependencies"] = p["dependencies"]
            sig = json.dumps(env_spec, sort_keys=True)
            if sig not in envs:
                envs[sig] = {"environment_key": "default" if len(env_spec) == 1 else f"env_{len(envs) + 1}", "spec": env_spec}
            t["environment_key"] = envs[sig]["environment_key"]
    if envs: s["environments"] = list(envs.values())
    else: s.pop("environments", None)
    s["performance_target"] = spec.get("performance_target", "STANDARD")

def check_serverless(s):
    probs = ["job_clusters still present"] if s.get("job_clusters") else []
    env_keys = {e["environment_key"] for e in s.get("environments", [])}
    for t in s.get("tasks", []):
        probs += [f"{t['task_key']}: {k} still set" for k in ("job_cluster_key", "new_cluster", "existing_cluster_id", "libraries") if k in t]
        if any(x in t for x in ENV_TASKS) and t.get("environment_key") not in env_keys:
            probs.append(f"{t['task_key']}: missing environment_key")
        if "notebook_task" in t and "environment_key" in t:
            probs.append(f"{t['task_key']}: notebook task should not have environment_key")
    return probs

def output_schema_for(variant_name):
    return f"{cfg.output_catalog}.{cfg.prefix.lower()}_{benchmark_id}_{variant_name}".lower()

def prepare_copy(source, variant):
    """Deep copy of the source settings with overrides, isolation, tags and no schedule."""
    s = copy.deepcopy(source)
    overrides = json.loads(variant["overrides_json"] or "{}")
    cluster_map = {jc["job_cluster_key"]: jc["new_cluster"] for jc in s.get("job_clusters", [])}
    cluster_map.update({f"_task_{t['task_key']}": t["new_cluster"] for t in s.get("tasks", []) if "new_cluster" in t})
    if "_serverless" in overrides:
        _to_serverless(s, overrides.pop("_serverless"))
        cluster_map = {}
    for ck, changes in overrides.items():
        for spec in (cluster_map.values() if ck == "_all" else [cluster_map[ck]] if ck in cluster_map else []):
            _apply_to_cluster(spec, changes)

    s["name"] = f"{NAME_PREFIX} {source.get('name', 'job')} | {variant['variant_name']}"
    # No schedule, triggers or notifications; run_as is dropped so the job runs as its creator
    for key in ("schedule", "trigger", "continuous", "queue", "email_notifications", "webhook_notifications",
                "notification_settings", "run_as", "edit_mode", "format", "deployment", "source"):
        s.pop(key, None)
    for t in s.get("tasks", []):
        for key in ("email_notifications", "webhook_notifications", "notification_settings"):
            t.pop(key, None)
    s["max_concurrent_runs"] = 1
    if not s.get("timeout_seconds"):
        s["timeout_seconds"] = timeout_seconds

    tags = dict(benchmark_id=benchmark_id, variant_id=variant["variant_id"], variant_name=variant["variant_name"],
                source_job_id=variant["source_job_id"])
    for jc in s.get("job_clusters", []):
        jc["new_cluster"].setdefault("custom_tags", {}).update(tags)
    for t in s.get("tasks", []):
        if "new_cluster" in t:
            t["new_cluster"].setdefault("custom_tags", {}).update(tags)
    s.setdefault("tags", {}).update(tags)

    out_schema = output_schema_for(variant["variant_name"])
    if output_param_name:
        params = s.get("parameters", [])
        for p in params:
            if p.get("name") == output_param_name:
                p["default"] = out_schema
                break
        else:
            params.append({"name": output_param_name, "default": out_schema})
        s["parameters"] = params
    return s, out_schema

for jid, s in source_jobs.items():
    names = [p["name"] for p in s.get("parameters", [])]
    if output_param_name and output_param_name not in names:
        print(f"WARNING job {jid}: parameter '{output_param_name}' not found (has {names}). It will be added, but "
              f"the tasks must read it, or the variants will write to the production outputs.")
    else:
        print(f"OK job {jid}: parameter '{output_param_name}' exists")
    if s.get("git_source"):
        print(f"NOTE job {jid}: code comes from git_source {s['git_source'].get('git_url')} - all variants run the same ref")

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
