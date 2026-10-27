"""Job copy logic used by 03_job_copier.

Pure logic: takes the source job's Jobs API 2.2 settings dict and a bench_variants row and returns the
settings for the benchmark copy. No Spark and no API calls; the notebook does the reads and the create.
"""
import copy
import json

ENV_TASKS = ("spark_jar_task", "spark_python_task", "python_wheel_task", "dbt_task")
# Removed from the copy: no schedule, triggers or notifications; run_as is dropped so the job runs as its creator
JOB_KEYS_REMOVED = ("schedule", "trigger", "continuous", "queue", "email_notifications", "webhook_notifications",
                    "notification_settings", "run_as", "edit_mode", "format", "deployment", "source")
TASK_KEYS_REMOVED = ("email_notifications", "webhook_notifications", "notification_settings")


def name_prefix(prefix, benchmark_id):
    return f"[{prefix}-{benchmark_id}]"


def apply_to_cluster(spec, changes):
    """Apply one variant's overrides for one cluster to a new_cluster spec (in place)."""
    for key, value in changes.items():
        if key == "worker_node_type":
            spec["node_type_id"] = value
        elif key == "driver_node_type":
            spec["driver_node_type_id"] = value
        elif key == "num_workers":
            spec["num_workers"] = int(value)
            spec.pop("autoscale", None)
        elif key in ("min_workers", "max_workers"):
            spec.setdefault("autoscale", {})[key] = int(value)
            spec.pop("num_workers", None)
        elif key in ("spark_version", "runtime_engine"):
            spec[key] = value
        elif key == "azure_availability":
            spec.setdefault("azure_attributes", {})["availability"] = value
        elif key == "first_on_demand":
            spec.setdefault("azure_attributes", {})["first_on_demand"] = int(value)
        elif key == "spark_conf_add":
            spec.setdefault("spark_conf", {}).update(value)
        else:
            raise ValueError(f"Unknown override key {key}")


def to_serverless(s, spec):
    """Move every task to serverless jobs compute using the per-task plan from 02 (in place): removes
    job_clusters and task compute, moves libraries into job environments, sets environment_key where a
    task needs one (not on notebook tasks) and sets the job's performance_target."""
    s.pop("job_clusters", None)
    ver, plan, envs = str(spec.get("environment_version", "4")), spec.get("tasks", {}), {}
    for t in s.get("tasks", []):
        for k in ("job_cluster_key", "new_cluster", "existing_cluster_id", "compute_key", "libraries"):
            t.pop(k, None)
        p = plan.get(t["task_key"], {})
        if p.get("type") in ENV_TASKS:
            env_spec = {"environment_version": ver}
            if p.get("java_dependencies"):
                env_spec["java_dependencies"] = p["java_dependencies"]
            if p.get("dependencies"):
                env_spec["dependencies"] = p["dependencies"]
            sig = json.dumps(env_spec, sort_keys=True)
            if sig not in envs:
                envs[sig] = {"environment_key": "default" if len(env_spec) == 1 else f"env_{len(envs) + 1}", "spec": env_spec}
            t["environment_key"] = envs[sig]["environment_key"]
    if envs:
        s["environments"] = list(envs.values())
    else:
        s.pop("environments", None)
    s["performance_target"] = spec.get("performance_target", "STANDARD")


def check_serverless(s):
    """Problems that would make a serverless job copy invalid (empty list = OK)."""
    probs = ["job_clusters still present"] if s.get("job_clusters") else []
    env_keys = {e["environment_key"] for e in s.get("environments", [])}
    for t in s.get("tasks", []):
        probs += [f"{t['task_key']}: {k} still set" for k in ("job_cluster_key", "new_cluster", "existing_cluster_id", "libraries") if k in t]
        if any(x in t for x in ENV_TASKS) and t.get("environment_key") not in env_keys:
            probs.append(f"{t['task_key']}: missing environment_key")
        if "notebook_task" in t and "environment_key" in t:
            probs.append(f"{t['task_key']}: notebook task should not have environment_key")
    return probs


def output_schema_for(output_catalog, prefix, benchmark_id, variant_name):
    return f"{output_catalog}.{prefix.lower()}_{benchmark_id}_{variant_name}".lower()


def prepare_copy(source, variant, *, benchmark_id, prefix, output_catalog, output_param_name="output_schema",
                 timeout_seconds=14400):
    """Deep copy of the source settings with overrides, isolation, tags and no schedule.
    Returns (settings, output_schema). The source dict is not modified."""
    s = copy.deepcopy(source)
    overrides = json.loads(variant["overrides_json"] or "{}")
    cluster_map = {jc["job_cluster_key"]: jc["new_cluster"] for jc in s.get("job_clusters", [])}
    cluster_map.update({f"_task_{t['task_key']}": t["new_cluster"] for t in s.get("tasks", []) if "new_cluster" in t})
    if "_serverless" in overrides:
        to_serverless(s, overrides.pop("_serverless"))
        cluster_map = {}
    for ck, changes in overrides.items():
        for spec in (cluster_map.values() if ck == "_all" else [cluster_map[ck]] if ck in cluster_map else []):
            apply_to_cluster(spec, changes)

    s["name"] = f"{name_prefix(prefix, benchmark_id)} {source.get('name', 'job')} | {variant['variant_name']}"
    for key in JOB_KEYS_REMOVED:
        s.pop(key, None)
    for t in s.get("tasks", []):
        for key in TASK_KEYS_REMOVED:
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

    out_schema = output_schema_for(output_catalog, prefix, benchmark_id, variant["variant_name"])
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


def preflight_messages(job_id, s, output_param_name):
    """Warnings about a source job before copying it (output parameter, git_source)."""
    msgs = []
    names = [p["name"] for p in s.get("parameters", [])]
    if output_param_name and output_param_name not in names:
        msgs.append(f"WARNING job {job_id}: parameter '{output_param_name}' not found (has {names}). It will be added, but "
                    f"the tasks must read it, or the variants will write to the production outputs.")
    else:
        msgs.append(f"OK job {job_id}: parameter '{output_param_name}' exists")
    if s.get("git_source"):
        msgs.append(f"NOTE job {job_id}: code comes from git_source {s['git_source'].get('git_url')} - all variants run the same ref")
    return msgs
