"""Variant generation used by 02_variant_generator.

Pure logic: no Spark and no API calls. Node types are looked up in bench_common's node catalog
(bc.load_node_catalog() in a notebook, bc.set_node_catalog() in tests). Job settings for the serverless
variants are passed in as the Jobs API 2.2 settings dict.

A variant is a dict: variant_name, dimension, description, overrides, is_valid, invalid_reason.
generate_variants() adds benchmark_id, source_job_id, variant_id and overrides_json (overrides serialised).
"""
import json

import bench_common as bc

# Spark confs serverless accepts only at session level (set in code), never on the job.
SERVERLESS_SESSION_CONFS = {"spark.sql.shuffle.partitions", "spark.sql.session.timeZone", "spark.sql.ansi.enabled",
                            "spark.sql.legacy.timeParserPolicy", "spark.sql.files.maxPartitionBytes",
                            "spark.databricks.execution.timeout"}
TASK_TYPES = ["notebook_task", "spark_jar_task", "spark_python_task", "python_wheel_task", "spark_submit_task",
              "sql_task", "dbt_task", "pipeline_task", "run_job_task", "condition_task", "for_each_task",
              "dashboard_task", "power_bi_task", "clean_rooms_notebook_task"]
NO_COMPUTE_TASKS = {"sql_task", "pipeline_task", "run_job_task", "condition_task", "dashboard_task", "power_bi_task"}
# STANDARD (cheaper, 4-6 min startup) and PERFORMANCE_OPTIMIZED (starts in seconds, higher price)
SERVERLESS_MODES = [("serverless_standard", "STANDARD"), ("serverless_perf_optimized", "PERFORMANCE_OPTIMIZED")]


def make_variant(name, dimension, desc, overrides, valid=True, reason=None):
    return dict(variant_name=name, dimension=dimension, description=desc, overrides=overrides,
                is_valid=valid, invalid_reason=reason)


def per_cluster(clusters, name, dimension, desc, fn):
    """Apply fn to every cluster; the variant is invalid if fn returns None for any of them."""
    overrides = {}
    for c in clusters:
        r = fn(c)
        if r is None:
            return make_variant(name, dimension, desc, {}, False,
                                f"No matching node type for {c['job_cluster_key']} ({c['worker_node_type']})")
        overrides[c["job_cluster_key"]] = r
    return make_variant(name, dimension, desc, overrides)


def task_type(task):
    return next((x for x in TASK_TYPES if x in task), "unknown")


def serverless_variant(settings, clusters, variant_name, perf, env_version="4", cluster_dbr=None):
    """Move every task to serverless jobs compute. Builds a per-task plan that 03_job_copier applies.
    Anything that can't run on serverless becomes a blocker (is_valid = false with the reason).

    settings     Jobs API settings of the source job
    cluster_dbr  optional callable(existing_cluster_id) -> spark_version, for tasks on all-purpose clusters
    """
    cl = {c["job_cluster_key"]: c for c in clusters}
    blockers, notes, plan = [], [], {}
    for t in settings.get("tasks", []):
        k = t["task_key"]
        ttype = task_type(t)
        libs = t.get("libraries") or []
        p = {"type": ttype}
        if t.get("job_cluster_key"):
            dbr = (cl.get(t["job_cluster_key"]) or {}).get("spark_version") or ""
        elif t.get("new_cluster"):
            dbr = t["new_cluster"].get("spark_version", "")
        elif t.get("existing_cluster_id"):
            try:
                dbr = (cluster_dbr(t["existing_cluster_id"]) if cluster_dbr else "") or ""
            except Exception:
                dbr = ""
            notes.append(f"{k}: was on an all-purpose cluster")
        else:
            dbr = ""
        if ttype == "spark_jar_task":
            if "scala2.13" not in dbr:
                blockers.append(f"{k}: JAR built for {dbr or 'unknown DBR/Scala'}; serverless env {env_version} "
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
                if "whl" in l:
                    deps.append(l["whl"])
                elif "pypi" in l and not l["pypi"].get("repo"):
                    deps.append(l["pypi"]["package"])
                elif "requirements" in l:
                    deps.append(f"-r {l['requirements']}")
                else:
                    blockers.append(f"{k}: library {l} has no serverless environment equivalent")
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
            if sess:
                msg += f" (session-settable in code: {', '.join(sess)})"
            if other:
                msg += f" (not available on serverless: {', '.join(other)})"
            notes.append(msg)
        if json.loads(c.get("init_scripts_json") or "[]"):
            notes.append(f"{c['job_cluster_key']}: init scripts dropped (not supported on serverless)")
    notes.append("node types, Spot, DBR, Photon (always on) and access mode are managed by serverless")
    desc = (f"All tasks on serverless jobs compute, env v{env_version}, {perf} mode"
            + (" (4-6 min startup)" if perf == "STANDARD" else " (fast startup, higher price)")
            + " | " + "; ".join(notes))
    return make_variant(variant_name, "serverless", desc,
                        {"_serverless": {"performance_target": perf, "environment_version": env_version, "tasks": plan}},
                        not blockers, "; ".join(blockers) or None)


def _shape(clusters, name, factor, desc):
    """Worker size x factor, worker counts rescaled by the core ratio so total worker cores match.
    Fixed counts and autoscale min/max are scaled separately; inexact bounds are rounded (min 1 worker)
    and written into the description. The driver keeps its size, so only the worker shape varies."""
    mism = []

    def fn(c):
        new = bc.scale_vm(c["worker_node_type"], factor)
        oc, nc = bc.cores_of(c["worker_node_type"]), bc.cores_of(new) if new else None
        if not new or not oc or not nc:
            return None
        o = {"worker_node_type": new}

        def sc(n, label):
            k, exact = bc.scale_workers(n, oc, nc)
            if not exact:
                mism.append(f"{c['job_cluster_key']} {label} {n}x{oc}c -> {k}x{nc}c")
            return k
        if c.get("min_workers") is not None:
            o["min_workers"], o["max_workers"] = sc(int(c["min_workers"]), "min"), sc(int(c["max_workers"]), "max")
        else:
            o["num_workers"] = sc(int(c.get("num_workers") or 1), "workers")
        return o
    r = per_cluster(clusters, name, "shape", desc, fn)
    if r["is_valid"] and mism:
        r["description"] += " | worker capacity differs from baseline: " + "; ".join(mism)
    return r


def generate_variants(job_id, clusters, *, benchmark_id, vm_generations=(4, 5, 6),
                      dbr_candidates=("15.4.x-scala2.12", "16.4.x-scala2.12"), include_serverless=False,
                      serverless_env_version="4", job_settings=None, cluster_dbr=None):
    """One-factor-at-a-time variants for one source job.

    clusters      rows of bench_source_clusters for the job (dicts)
    job_settings  Jobs API settings of the source job (required when include_serverless)
    """
    if include_serverless and job_settings is None:
        raise ValueError("job_settings is required when include_serverless is true")
    v = [make_variant("baseline", "baseline", "Original config (availability as in the source job)", {})]

    v.append(_shape(clusters, "half_workers_2x_size", 2.0, "Half the workers, each 2x the size"))
    v.append(_shape(clusters, "2x_workers_half_size", 0.5, "Double the workers, each half the size"))

    # Autoscaling off: autoscaling clusters fixed at max_workers. Skipped when nothing autoscales.
    asc = [c for c in clusters if c.get("min_workers") is not None and c.get("max_workers") is not None]
    if asc:
        v.append(make_variant("autoscale_off", "autoscale", "Autoscaling off, fixed at max_workers (" + ", ".join(
            f"{c['job_cluster_key']} {int(c['min_workers'])}-{int(c['max_workers'])} -> {int(c['max_workers'])}" for c in asc) + ")",
            {c["job_cluster_key"]: {"num_workers": int(c["max_workers"])} for c in asc}))

    # VM generation (driver follows when available). Skipped for the generation every cluster already uses.
    gens = {bc.parse_vm(c["worker_node_type"])["gen"] for c in clusters if bc.parse_vm(c["worker_node_type"])}
    for g in vm_generations:
        if len(gens) == 1 and g in gens:
            continue

        def fn(c, g=g):
            nw = bc.change_gen(c["worker_node_type"], g)
            return None if not nw else {"worker_node_type": nw,
                                        "driver_node_type": bc.change_gen(c["driver_node_type"], g) or c["driver_node_type"]}
        v.append(per_cluster(clusters, f"vm_v{g}", "vm_gen", f"VM generation v{g}", fn))

    # Delta cache (local SSD 'd' VMs) toggled from the first cluster's current state
    p0 = bc.parse_vm(clusters[0]["worker_node_type"])
    if p0:
        en = not p0["has_cache"]
        st = "on" if en else "off"

        def fn(c, en=en):
            nw = bc.toggle_cache(c["worker_node_type"], en)
            return None if not nw else {"worker_node_type": nw,
                                        "driver_node_type": bc.toggle_cache(c["driver_node_type"], en) or c["driver_node_type"]}
        v.append(per_cluster(clusters, f"cache_{st}", "cache", f"Delta cache {st} (local SSD {'enabled' if en else 'disabled'})", fn))

    # Availability: on-demand -> Spot with fallback only (Spot clusters are never benchmarked as on-demand)
    od = [c for c in clusters if "SPOT" not in (c.get("azure_availability") or "ON_DEMAND_AZURE")]
    if od:
        v.append(make_variant("spot_with_fallback", "availability",
                              f"Spot with fallback for on-demand clusters ({', '.join(c['job_cluster_key'] for c in od)})",
                              {c["job_cluster_key"]: {"azure_availability": "SPOT_WITH_FALLBACK_AZURE",
                                                      "first_on_demand": int(c.get("first_on_demand") or 1)} for c in od}))

    # Photon toggle
    engines = {c.get("runtime_engine") or "STANDARD" for c in clusters}
    new_engine = "STANDARD" if "PHOTON" in engines else "PHOTON"
    on = new_engine == "PHOTON"
    v.append(make_variant(f"photon_{'on' if on else 'off'}", "photon", f"Photon {'enabled' if on else 'disabled'}",
                          {c["job_cluster_key"]: {"runtime_engine": new_engine} for c in clusters}))

    # DBR versions
    current = {c.get("spark_version") for c in clusters}
    for dbr in dbr_candidates:
        if current == {dbr}:
            continue
        short = dbr.split(".x")[0]
        v.append(make_variant(f"dbr_{short.replace('.', '_')}", "dbr", f"DBR {short} ({dbr})",
                              {c["job_cluster_key"]: {"spark_version": dbr} for c in clusters}))

    # Shuffle partitions: auto (auto-optimized shuffle) and 200 (Spark default); skipped if already set everywhere
    def _shuffle(c):
        return json.loads(c.get("spark_conf_json") or "{}").get("spark.sql.shuffle.partitions", "200")
    for name, val, desc in [("shuffle_auto", "auto", "shuffle.partitions=auto (auto-optimized shuffle)"),
                            ("shuffle_200", "200", "shuffle.partitions=200 (Spark default)")]:
        if all(_shuffle(c) == val for c in clusters):
            continue
        v.append(make_variant(name, "shuffle", desc,
                              {c["job_cluster_key"]: {"spark_conf_add": {"spark.sql.shuffle.partitions": val}} for c in clusters}))

    if include_serverless:
        for name, mode in SERVERLESS_MODES:
            v.append(serverless_variant(job_settings, clusters, name, mode, serverless_env_version, cluster_dbr))

    for x in v:
        x.update(benchmark_id=benchmark_id, source_job_id=str(job_id), variant_id=f"{benchmark_id}_{x['variant_name']}",
                 overrides_json=json.dumps(x.pop("overrides")))
    return v
