"""Run queue, batching, run metrics and code fingerprints used by 04_orchestrator.

Pure logic: no Spark and no API calls. run_metrics() takes a Jobs SDK Run object (or anything with the
same attributes), code_manifest() takes job settings plus two callables that fingerprint workspace items
and files, so tests can pass fakes.
"""
import hashlib
import json
from datetime import datetime, timezone

import bench_common as bc

TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def ts(ms):
    """Epoch milliseconds -> UTC datetime (None for 0/None)."""
    return datetime.fromtimestamp(ms / 1000, timezone.utc) if ms else None


def union_seconds(intervals):
    """Total seconds covered by the union of (start, end) datetime intervals; open or empty ones are ignored."""
    total, cs, ce = 0.0, None, None
    for s, e in sorted(i for i in intervals if i[0] and i[1] and i[1] > i[0]):
        if ce is None or s > ce:
            if ce is not None:
                total += (ce - cs).total_seconds()
            cs, ce = s, e
        else:
            ce = max(ce, e)
    if ce is not None:
        total += (ce - cs).total_seconds()
    return total


def run_status(life_cycle_state, result_state):
    """SUCCESS / FAILED / SKIPPED for finished runs, RUNNING otherwise.
    SKIPPED (e.g. max concurrent runs reached) never ran: not a failure, and it is re-queued."""
    if life_cycle_state not in TERMINAL:
        return "RUNNING"
    if life_cycle_state == "SKIPPED":
        return "SKIPPED"
    return "SUCCESS" if result_state == "SUCCESS" else "FAILED"


def run_metrics(r):
    """Timing of one job run and its task attempts.

    duration_seconds  = wall time incl. cluster startup
    task_only_seconds = wall time while at least one task was executing (union of execution intervals)
    overhead_seconds  = duration - task_only
    """
    lcs, rs = bc.ev(r.state.life_cycle_state), bc.ev(r.state.result_state)
    st, et = ts(r.start_time), ts(r.end_time)
    terminal = lcs in TERMINAL
    intervals, exec_sum, max_setup, tasks = [], 0, 0, []
    for t in (r.tasks or []):
        setup_ms, exec_ms = t.setup_duration or 0, t.execution_duration or 0
        xs = ts(t.start_time + setup_ms) if t.start_time else None
        xe = ts(t.start_time + setup_ms + exec_ms) if t.start_time and exec_ms else None
        intervals.append((xs, xe))
        exec_sum += exec_ms // 1000
        max_setup = max(max_setup, setup_ms // 1000)
        ci = getattr(t, "cluster_instance", None)
        tasks.append(dict(
            task_run_id=str(t.run_id), task_key=t.task_key, attempt_number=t.attempt_number or 0,
            job_cluster_key=getattr(t, "job_cluster_key", None), cluster_id=ci.cluster_id if ci else None,
            task_start=ts(t.start_time), exec_start=xs, exec_end=xe,
            queue_seconds=(getattr(t, "queue_duration", None) or 0) // 1000, setup_seconds=setup_ms // 1000,
            execution_seconds=exec_ms // 1000, cleanup_seconds=(getattr(t, "cleanup_duration", None) or 0) // 1000,
            result_state=bc.ev(t.state.result_state) if getattr(t, "state", None) else None))
    dur = int((et - st).total_seconds()) if st and et else None
    task_only = int(union_seconds(intervals)) if terminal else None
    return dict(
        run_id=str(r.run_id), start_time=st, end_time=et, duration_seconds=dur, task_only_seconds=task_only,
        task_exec_sum_seconds=exec_sum if terminal else None,
        overhead_seconds=dur - task_only if dur is not None and task_only is not None else None,
        max_setup_seconds=max_setup, n_tasks=len(r.tasks or []), life_cycle_state=lcs, result_state=rs,
        state_message=(r.state.state_message or "")[:500], status=run_status(lcs, rs), tasks=tasks)


def build_queue(bench_jobs, existing_counts, max_runs):
    """(variant_name, job, run_number) for every run still needed to reach max_runs per variant.
    existing_counts = SUCCESS + RUNNING runs per variant (FAILED and SKIPPED runs are retried)."""
    queue = []
    for vname, job in bench_jobs.items():
        done = existing_counts.get(vname, 0)
        queue += [(vname, job, n) for n in range(done + 1, max_runs + 1)]
    return queue


def make_batches(queue, batch_size):
    """Split the queue into batches of at most batch_size runs, with a variant at most once per batch
    (benchmark jobs allow one concurrent run). Queue order is kept as far as possible."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    chunks, left = [], list(queue)
    while left:
        chunk, seen = [], set()
        for item in list(left):
            if len(chunk) < batch_size and item[0] not in seen:
                chunk.append(item)
                seen.add(item[0])
                left.remove(item)
        chunks.append(chunk)
    return chunks


def code_manifest(s, ws_item, file_item):
    """Manifest of what each task runs, from job settings s.

    ws_item(path)   -> (fingerprint, modified_ms) for a workspace notebook/file
    file_item(path) -> (fingerprint, modified_ms) for a JAR/wheel/requirements/volume file
    Returns (manifest dict, newest modified_ms).
    """
    man, newest = {}, 0
    gs = s.get("git_source")
    if gs:
        man["git_source"] = f"{gs.get('git_url')}@" + (gs.get("git_commit") or gs.get("git_tag") or f"branch:{gs.get('git_branch')}")
    for t in s.get("tasks", []):
        k, paths = t["task_key"], []
        if "notebook_task" in t and t["notebook_task"].get("source", "WORKSPACE") == "WORKSPACE":
            paths.append(("ws", t["notebook_task"]["notebook_path"]))
        if "spark_python_task" in t and t["spark_python_task"].get("source", "WORKSPACE") == "WORKSPACE":
            p = t["spark_python_task"]["python_file"]
            paths.append(("file", p) if p.startswith("/Volumes/") else ("ws", p))
        if "spark_jar_task" in t:
            man[f"{k}:main_class"] = t["spark_jar_task"].get("main_class_name")
        for lib in t.get("libraries") or []:
            for typ in ("jar", "whl", "egg", "requirements"):
                if typ in lib:
                    paths.append(("file", lib[typ]))
            if "maven" in lib:
                man[f"{k}:maven"] = lib["maven"].get("coordinates")
            if "pypi" in lib:
                man[f"{k}:pypi"] = lib["pypi"].get("package")
        for env in s.get("environments", []):
            if env["environment_key"] == t.get("environment_key"):
                for dep in env["spec"].get("java_dependencies", []) + env["spec"].get("dependencies", []):
                    if dep.startswith("/"):
                        paths.append(("file", dep))
                    else:
                        man[f"{k}:dep:{dep}"] = dep
        for kind, p in paths:
            fp, mtime = ws_item(p) if kind == "ws" else file_item(p)
            man[f"{k}:{p}"] = fp
            newest = max(newest, mtime or 0)
    return man, newest


def code_version(man):
    """First 12 hex chars of sha256 over the manifest (key order does not matter)."""
    return hashlib.sha256(json.dumps(man, sort_keys=True).encode()).hexdigest()[:12]
