# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Variant generator
# MAGIC
# MAGIC Generates one-factor-at-a-time variants from the discovered clusters. Each variant changes a single dimension (worker shape, autoscaling, VM generation, delta cache, Spot, Photon, DBR, shuffle partitions, or serverless) and keeps everything else as in the baseline. Variants that are impossible in this workspace (node type not available, task type not supported on serverless) are stored with `is_valid = false` and a reason. See `references/variants.md`.

# COMMAND ----------

# DBTITLE 1,Configuration
import os, sys, json
sys.path.insert(0, os.getcwd())
import bench_common as bc

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
SERVERLESS_MODES = [("serverless_standard", "STANDARD"), ("serverless_perf_optimized", "PERFORMANCE_OPTIMIZED")]
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
def _variant(name, dimension, desc, overrides, valid=True, reason=None):
    return dict(variant_name=name, dimension=dimension, description=desc, overrides=overrides,
                is_valid=valid, invalid_reason=reason)

def _per_cluster(clusters, name, dimension, desc, fn):
    """Apply fn to every cluster; the variant is invalid if fn returns None for any of them."""
    overrides = {}
    for c in clusters:
        r = fn(c)
        if r is None:
            return _variant(name, dimension, desc, {}, False, f"No matching node type for {c['job_cluster_key']} ({c['worker_node_type']})")
        overrides[c["job_cluster_key"]] = r
    return _variant(name, dimension, desc, overrides)

# Serverless ----------------------------------------------------------------------------------------
# Spark confs serverless accepts only at session level (set in code), never on the job.
SERVERLESS_SESSION_CONFS = {"spark.sql.shuffle.partitions", "spark.sql.session.timeZone", "spark.sql.ansi.enabled",
                            "spark.sql.legacy.timeParserPolicy", "spark.sql.files.maxPartitionBytes",
                            "spark.databricks.execution.timeout"}
TASK_TYPES = ["notebook_task", "spark_jar_task", "spark_python_task", "python_wheel_task", "spark_submit_task",
              "sql_task", "dbt_task", "pipeline_task", "run_job_task", "condition_task", "for_each_task",
              "dashboard_task", "power_bi_task", "clean_rooms_notebook_task"]
NO_COMPUTE_TASKS = {"sql_task", "pipeline_task", "run_job_task", "condition_task", "dashboard_task", "power_bi_task"}

def _serverless_variant(job_id, clusters, variant_name, perf):
    """Move every task to serverless jobs compute. Builds a per-task plan that 03_job_copier applies.
    Anything that can't run on serverless becomes a blocker (is_valid = false with the reason)."""
    s = w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": int(job_id)})["settings"]
    cl = {c["job_cluster_key"]: c for c in clusters}
    blockers, notes, plan = [], [], {}
    for t in s.get("tasks", []):
        k = t["task_key"]
        ttype = next((x for x in TASK_TYPES if x in t), "unknown")
        libs = t.get("libraries") or []
        p = {"type": ttype}
        if t.get("job_cluster_key"):
            dbr = (cl.get(t["job_cluster_key"]) or {}).get("spark_version") or ""
        elif t.get("new_cluster"):
            dbr = t["new_cluster"].get("spark_version", "")
        elif t.get("existing_cluster_id"):
            try:
                dbr = w.clusters.get(t["existing_cluster_id"]).spark_version or ""
            except Exception:
                dbr = ""
            notes.append(f"{k}: was on an all-purpose cluster")
        else:
            dbr = ""
        if ttype == "spark_jar_task":
            if "scala2.13" not in dbr:
                blockers.append(f"{k}: JAR built for {dbr or 'unknown DBR/Scala'}; serverless env {serverless_env_version} "
                                f"needs a JAR compiled for Scala 2.13 + JDK 17 using public Spark APIs only")
            jars = [l["jar"] for l in libs if "jar" in l]
            other = [list(l)[0] for l in libs if "jar" not in l]
            if other:
                blockers.append(f"{k}: libraries {other} are not moved automatically; package them into the JAR or upload JARs to a UC volume")
            if not jars:
                blockers.append(f"{k}: no JAR library found to put in the serverless environment")
            p["java_dependencies"] = jars
        elif ttype in ("spark_python_task", "python_wheel_task"):
            deps = []
            for l in libs:
                if "whl" in l: deps.append(l["whl"])
                elif "pypi" in l and not l["pypi"].get("repo"): deps.append(l["pypi"]["package"])
                elif "requirements" in l: deps.append(f"-r {l['requirements']}")
                else: blockers.append(f"{k}: library {l} has no serverless environment equivalent")
            p["dependencies"] = deps
        elif ttype == "notebook_task":
            if libs:
                blockers.append(f"{k}: notebook task libraries aren't supported on serverless; use %pip or the notebook environment")
        elif ttype == "spark_submit_task":
            blockers.append(f"{k}: spark_submit_task is not supported on serverless")
        elif ttype == "dbt_task":
            p["dependencies"] = ["dbt-databricks"]
            notes.append(f"{k}: dbt task gets dbt-databricks in its environment (check the version)")
        elif ttype == "for_each_task":
            blockers.append(f"{k}: for_each_task nested tasks are not converted automatically")
        elif ttype not in NO_COMPUTE_TASKS:
            blockers.append(f"{k}: task type {ttype} not handled for serverless")
        plan[k] = p
    for c in clusters:
        conf = json.loads(c.get("spark_conf_json") or "{}")
        if conf:
            sess = sorted(x for x in conf if x in SERVERLESS_SESSION_CONFS)
            other = sorted(x for x in conf if x not in SERVERLESS_SESSION_CONFS)
            msg = f"{c['job_cluster_key']} spark_conf dropped"
            if sess: msg += f" (session-settable in code: {', '.join(sess)})"
            if other: msg += f" (not available on serverless: {', '.join(other)})"
            notes.append(msg)
        if json.loads(c.get("init_scripts_json") or "[]"):
            notes.append(f"{c['job_cluster_key']}: init scripts dropped (not supported on serverless)")
    notes.append("node types, Spot, DBR, Photon (always on) and access mode are managed by serverless")
    desc = (f"All tasks on serverless jobs compute, env v{serverless_env_version}, {perf} mode"
            + (" (4-6 min startup)" if perf == "STANDARD" else " (fast startup, higher price)")
            + " | " + "; ".join(notes))
    return _variant(variant_name, "serverless", desc,
                    {"_serverless": {"performance_target": perf, "environment_version": serverless_env_version, "tasks": plan}},
                    not blockers, "; ".join(blockers) or None)

def generate_variants(job_id, clusters):
    v = [_variant("baseline", "baseline", "Original config (availability as in the source job)", {})]

    # Shape: worker size x2 / x0.5, worker counts rescaled by the core ratio so total worker cores match.
    # Fixed counts and autoscale min/max are scaled separately; inexact bounds are rounded (min 1 worker)
    # and written into the description. The driver keeps its size, so only the worker shape varies.
    def _shape(name, factor, desc):
        mism = []
        def fn(c):
            new = bc.scale_vm(c["worker_node_type"], factor)
            oc, nc = bc.cores_of(c["worker_node_type"]), bc.cores_of(new) if new else None
            if not new or not oc or not nc:
                return None
            o = {"worker_node_type": new}
            def sc(n, label):
                k, exact = bc.scale_workers(n, oc, nc)
                if not exact: mism.append(f"{c['job_cluster_key']} {label} {n}x{oc}c -> {k}x{nc}c")
                return k
            if c.get("min_workers") is not None:
                o["min_workers"], o["max_workers"] = sc(int(c["min_workers"]), "min"), sc(int(c["max_workers"]), "max")
            else:
                o["num_workers"] = sc(int(c.get("num_workers") or 1), "workers")
            return o
        r = _per_cluster(clusters, name, "shape", desc, fn)
        if r["is_valid"] and mism:
            r["description"] += " | worker capacity differs from baseline: " + "; ".join(mism)
        return r
    v.append(_shape("half_workers_2x_size", 2.0, "Half the workers, each 2x the size"))
    v.append(_shape("2x_workers_half_size", 0.5, "Double the workers, each half the size"))

    # Autoscaling off: autoscaling clusters fixed at max_workers. Skipped when nothing autoscales.
    asc = [c for c in clusters if c.get("min_workers") is not None and c.get("max_workers") is not None]
    if asc:
        v.append(_variant("autoscale_off", "autoscale", "Autoscaling off, fixed at max_workers (" + ", ".join(
            f"{c['job_cluster_key']} {int(c['min_workers'])}-{int(c['max_workers'])} -> {int(c['max_workers'])}" for c in asc) + ")",
            {c["job_cluster_key"]: {"num_workers": int(c["max_workers"])} for c in asc}))

    # VM generation (driver follows when available)
    gens = {bc.parse_vm(c["worker_node_type"])["gen"] for c in clusters if bc.parse_vm(c["worker_node_type"])}
    for g in VM_GENERATIONS:
        if len(gens) == 1 and g in gens:
            continue
        def fn(c, g=g):
            nw = bc.change_gen(c["worker_node_type"], g)
            return None if not nw else {"worker_node_type": nw, "driver_node_type": bc.change_gen(c["driver_node_type"], g) or c["driver_node_type"]}
        v.append(_per_cluster(clusters, f"vm_v{g}", "vm_gen", f"VM generation v{g}", fn))

    # Delta cache (local SSD 'd' VMs) toggled from the first cluster's current state
    p0 = bc.parse_vm(clusters[0]["worker_node_type"])
    if p0:
        en = not p0["has_cache"]
        st = "on" if en else "off"
        def fn(c, en=en):
            nw = bc.toggle_cache(c["worker_node_type"], en)
            return None if not nw else {"worker_node_type": nw, "driver_node_type": bc.toggle_cache(c["driver_node_type"], en) or c["driver_node_type"]}
        v.append(_per_cluster(clusters, f"cache_{st}", "cache", f"Delta cache {st} (local SSD {'enabled' if en else 'disabled'})", fn))

    # Availability: on-demand -> Spot with fallback only (Spot clusters are never benchmarked as on-demand)
    od = [c for c in clusters if "SPOT" not in (c.get("azure_availability") or "ON_DEMAND_AZURE")]
    if od:
        v.append(_variant("spot_with_fallback", "availability",
                          f"Spot with fallback for on-demand clusters ({', '.join(c['job_cluster_key'] for c in od)})",
                          {c["job_cluster_key"]: {"azure_availability": "SPOT_WITH_FALLBACK_AZURE",
                                                  "first_on_demand": int(c.get("first_on_demand") or 1)} for c in od}))

    # Photon toggle
    engines = {c.get("runtime_engine") or "STANDARD" for c in clusters}
    new_engine = "STANDARD" if "PHOTON" in engines else "PHOTON"
    on = new_engine == "PHOTON"
    v.append(_variant(f"photon_{'on' if on else 'off'}", "photon", f"Photon {'enabled' if on else 'disabled'}",
                      {c["job_cluster_key"]: {"runtime_engine": new_engine} for c in clusters}))

    # DBR versions
    current = {c.get("spark_version") for c in clusters}
    for dbr in DBR_CANDIDATES:
        if current == {dbr}:
            continue
        short = dbr.split(".x")[0]
        v.append(_variant(f"dbr_{short.replace('.', '_')}", "dbr", f"DBR {short} ({dbr})",
                          {c["job_cluster_key"]: {"spark_version": dbr} for c in clusters}))

    # Shuffle partitions: auto (auto-optimized shuffle) and 200 (Spark default); skipped if already set everywhere
    def _shuffle(c):
        return json.loads(c.get("spark_conf_json") or "{}").get("spark.sql.shuffle.partitions", "200")
    for name, val, desc in [("shuffle_auto", "auto", "shuffle.partitions=auto (auto-optimized shuffle)"),
                            ("shuffle_200", "200", "shuffle.partitions=200 (Spark default)")]:
        if all(_shuffle(c) == val for c in clusters):
            continue
        v.append(_variant(name, "shuffle", desc,
                          {c["job_cluster_key"]: {"spark_conf_add": {"spark.sql.shuffle.partitions": val}} for c in clusters}))

    if include_serverless:
        for name, mode in SERVERLESS_MODES:
            v.append(_serverless_variant(job_id, clusters, name, mode))

    for x in v:
        x.update(benchmark_id=benchmark_id, source_job_id=str(job_id), variant_id=f"{benchmark_id}_{x['variant_name']}",
                 overrides_json=json.dumps(x.pop("overrides")))
    return v

all_variants = []
for jid, jc in job_clusters.items():
    vs = generate_variants(jid, jc)
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
