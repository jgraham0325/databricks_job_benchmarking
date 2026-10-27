# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Run orchestrator
# MAGIC
# MAGIC Starts runs of the benchmark jobs, polls until they finish and records every run (and task attempt) in `bench_runs` / `bench_run_tasks`. The Jobs API is the source of truth: runs are synced on submit, on completion and at startup, so a restart loses nothing; re-run the notebook to resume.
# MAGIC
# MAGIC * Modes: **batched** (default; at most one run per variant per batch), **parallel**, **sequential**. Order is shuffled by default to spread time-of-day effects.
# MAGIC * Idempotent: variants that already have `max_runs` successful or running runs are skipped.
# MAGIC * Optional input guard: list the input tables in `input_tables` and the notebook checks that their Delta version did not change while the benchmark ran.
# MAGIC * Records a code fingerprint per run in `bench_run_code_versions`.
# MAGIC
# MAGIC **This starts job runs that cost money.** `dry_run = true` (default) only shows the queue.

# COMMAND ----------

# DBTITLE 1,Configuration, tracking tables, sync and run queue
import os, sys, json, time, random
sys.path.insert(0, os.getcwd())
import bench_common as bc
from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import _parse_datatype_string

cfg = bc.setup(dbutils, spark)
w = bc.w
benchmark_id = bc.benchmark_id_widget(dbutils)
dbutils.widgets.dropdown("run_mode", "batched", ["batched", "parallel", "sequential"], "Run mode")
dbutils.widgets.text("batch_size", "3", "Batch size (batched mode)")
dbutils.widgets.text("batch_pause_seconds", "0", "Pause between batches (seconds)")
dbutils.widgets.dropdown("shuffle_order", "true", ["true", "false"], "Shuffle run order")
dbutils.widgets.text("max_runs", "1", "Successful runs wanted per variant")
dbutils.widgets.text("variants_filter", "", "Variants to run (comma-separated, blank = all)")
dbutils.widgets.text("input_tables", "", "Input tables to check unchanged (comma-separated, optional)")
dbutils.widgets.text("code_label", "", "Code label (optional, e.g. git tag)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (preview queue only)")

run_mode = dbutils.widgets.get("run_mode")
batch_size = int(dbutils.widgets.get("batch_size"))
batch_pause = int(dbutils.widgets.get("batch_pause_seconds"))
shuffle = dbutils.widgets.get("shuffle_order") == "true"
max_runs = int(dbutils.widgets.get("max_runs"))
variant_filter = [v.strip() for v in dbutils.widgets.get("variants_filter").split(",") if v.strip()] or None
input_tables = [t.strip() for t in dbutils.widgets.get("input_tables").split(",") if t.strip()]
code_label = dbutils.widgets.get("code_label").strip() or None
dry_run = dbutils.widgets.get("dry_run") == "true"
POLL_INTERVAL = 30

# Run level: duration_seconds = wall time incl. cluster startup; task_only_seconds = wall time while at least one
# task was executing (union of execution intervals); overhead_seconds = duration - task_only.
RUNS_TBL = f"{cfg.fqn}.bench_runs"
RUNS_SCHEMA = ("benchmark_id STRING, variant_name STRING, variant_id STRING, new_job_id STRING, "
               "run_id STRING, run_number INT, output_schema STRING, start_time TIMESTAMP, "
               "end_time TIMESTAMP, duration_seconds BIGINT, task_only_seconds BIGINT, "
               "task_exec_sum_seconds BIGINT, overhead_seconds BIGINT, max_setup_seconds BIGINT, "
               "n_tasks INT, life_cycle_state STRING, result_state STRING, state_message STRING, "
               "status STRING, synced_at TIMESTAMP")
TASKS_TBL = f"{cfg.fqn}.bench_run_tasks"
TASKS_SCHEMA = ("benchmark_id STRING, variant_name STRING, run_id STRING, task_run_id STRING, "
                "task_key STRING, attempt_number INT, job_cluster_key STRING, cluster_id STRING, "
                "task_start TIMESTAMP, exec_start TIMESTAMP, exec_end TIMESTAMP, "
                "queue_seconds BIGINT, setup_seconds BIGINT, execution_seconds BIGINT, "
                "cleanup_seconds BIGINT, result_state STRING, synced_at TIMESTAMP")
# Tracking tables are never dropped automatically; on a schema mismatch, stop and let the user decide
# (both are rebuilt from the Jobs API, so a manual DROP is safe within the 60-day run history).
for tbl, ddl in [(RUNS_TBL, RUNS_SCHEMA), (TASKS_TBL, TASKS_SCHEMA)]:
    want = [f.name for f in _parse_datatype_string(ddl).fields]
    if spark.catalog.tableExists(tbl):
        have = spark.table(tbl).columns
        if have != want:
            raise RuntimeError(f"{tbl} schema differs.\n  have: {have}\n  want: {want}\nDROP it manually and re-run.")
    else:
        spark.sql(f"CREATE TABLE {tbl} ({ddl})")

all_jobs = {r.variant_name: r.asDict() for r in spark.table(f"{cfg.fqn}.bench_jobs")
            .filter(f"benchmark_id = '{benchmark_id}'").filter(F.col("status").isin("created", "exists"))
            .filter(F.col("new_job_id").isNotNull()).collect()}
assert all_jobs, f"No benchmark jobs for {benchmark_id}: run 03_job_copier with dry_run=false first"
if variant_filter:
    unknown = set(variant_filter) - set(all_jobs)
    assert not unknown, f"Unknown variants in variants_filter: {sorted(unknown)} (have {sorted(all_jobs)})"
bench_jobs = {k: v for k, v in all_jobs.items() if not variant_filter or k in variant_filter}

def _ts(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc) if ms else None

def _union_seconds(intervals):
    total, cs, ce = 0.0, None, None
    for s, e in sorted(i for i in intervals if i[0] and i[1] and i[1] > i[0]):
        if ce is None or s > ce:
            if ce is not None: total += (ce - cs).total_seconds()
            cs, ce = s, e
        else:
            ce = max(ce, e)
    if ce is not None: total += (ce - cs).total_seconds()
    return total

def _merge(rows, ddl, tbl, key):
    if rows:
        (DeltaTable.forName(spark, tbl).alias("t").merge(spark.createDataFrame(rows, ddl).alias("s"), f"t.{key} = s.{key}")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def sync_runs(jobs):
    """Upsert every run and task attempt of the given benchmark jobs from the Jobs API."""
    run_rows, task_rows, now = [], [], datetime.now(timezone.utc)
    for vname, j in jobs.items():
        runs = sorted(w.jobs.list_runs(job_id=int(j["new_job_id"]), expand_tasks=True), key=lambda r: r.start_time or 0)
        for n, r in enumerate(runs, 1):
            lcs, rs = bc.ev(r.state.life_cycle_state), bc.ev(r.state.result_state)
            st, et = _ts(r.start_time), _ts(r.end_time)
            terminal = lcs in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
            # SKIPPED (e.g. max concurrent runs reached) never ran: not a failure, and it is re-queued
            status = ("SKIPPED" if lcs == "SKIPPED" else "SUCCESS" if rs == "SUCCESS" else "FAILED") if terminal else "RUNNING"
            intervals, exec_sum, max_setup = [], 0, 0
            for t in (r.tasks or []):
                setup_ms, exec_ms = t.setup_duration or 0, t.execution_duration or 0
                xs = _ts(t.start_time + setup_ms) if t.start_time else None
                xe = _ts(t.start_time + setup_ms + exec_ms) if t.start_time and exec_ms else None
                intervals.append((xs, xe))
                exec_sum += exec_ms // 1000
                max_setup = max(max_setup, setup_ms // 1000)
                task_rows.append((benchmark_id, vname, str(r.run_id), str(t.run_id), t.task_key, t.attempt_number or 0,
                                  getattr(t, "job_cluster_key", None), t.cluster_instance.cluster_id if getattr(t, "cluster_instance", None) else None,
                                  _ts(t.start_time), xs, xe, (getattr(t, "queue_duration", None) or 0) // 1000, setup_ms // 1000,
                                  exec_ms // 1000, (t.cleanup_duration or 0) // 1000,
                                  bc.ev(t.state.result_state) if t.state else None, now))
            dur = int((et - st).total_seconds()) if st and et else None
            task_only = int(_union_seconds(intervals)) if terminal else None
            run_rows.append((benchmark_id, vname, j["variant_id"], str(j["new_job_id"]), str(r.run_id), n, j["output_schema"],
                             st, et, dur, task_only, exec_sum if terminal else None,
                             dur - task_only if dur is not None and task_only is not None else None,
                             max_setup, len(r.tasks or []), lcs, rs, (r.state.state_message or "")[:500], status, now))
    _merge(run_rows, RUNS_SCHEMA, RUNS_TBL, "run_id")
    _merge(task_rows, TASKS_SCHEMA, TASKS_TBL, "task_run_id")
    return len(run_rows)

print(f"Synced {sync_runs(all_jobs)} existing run(s) from the Jobs API")
existing_counts = {r["variant_name"]: r["count"] for r in spark.table(RUNS_TBL).filter(f"benchmark_id = '{benchmark_id}'")
                   .filter(F.col("status").isin("SUCCESS", "RUNNING")).groupBy("variant_name").count().collect()}
run_queue = []
for vname, job in bench_jobs.items():
    done = existing_counts.get(vname, 0)
    run_queue += [(vname, job, n) for n in range(done + 1, max_runs + 1)]
if shuffle:
    random.shuffle(run_queue)
print(f"Benchmark {benchmark_id} | mode {run_mode} | batch {batch_size} | runs/variant {max_runs} | dry run {dry_run}")
print(f"Queue: {len(run_queue)} run(s) for {len(bench_jobs)} variant(s)")
for i, (vn, _, rn) in enumerate(run_queue, 1):
    print(f"  {i:>3}. {vn} (run {rn})")

# COMMAND ----------

# DBTITLE 1,Code version tracking
# code_version = first 12 hex chars of sha256 over a manifest of what each task runs:
#   Git folder or git_source -> commit/ref; workspace notebook/file -> sha256 of the source;
#   JAR/wheel/file library -> path + size + modified time (content not read).
import hashlib, base64
import pandas as pd
from databricks.sdk.service.workspace import ExportFormat, ObjectType

CODE_TBL = f"{cfg.fqn}.bench_run_code_versions"
CODE_SCHEMA = ("benchmark_id STRING, variant_name STRING, run_id STRING, code_version STRING, "
               "code_label STRING, manifest_json STRING, version_source STRING, captured_at TIMESTAMP")
if not spark.catalog.tableExists(CODE_TBL):
    spark.sql(f"CREATE TABLE {CODE_TBL} ({CODE_SCHEMA})")

def _git_commit(path):
    parts = path.rstrip("/").split("/")
    for i in range(len(parts) - 1, 1, -1):
        try:
            st = w.workspace.get_status("/".join(parts[:i]))
        except Exception:
            continue
        if st.object_type == ObjectType.REPO:
            return w.repos.get(st.object_id).head_commit_id
    return None

def _ws_item(path):
    try:
        sha, st = _git_commit(path), w.workspace.get_status(path)
        if sha:
            return f"git:{sha}", st.modified_at
        src = base64.b64decode(w.workspace.export(path, format=ExportFormat.SOURCE).content or b"")
        return "sha256:" + hashlib.sha256(src).hexdigest()[:16], st.modified_at
    except Exception as e:
        return f"unreadable:{type(e).__name__}", None

def _file_item(path):
    try:
        if path.startswith("/Volumes/"):
            m = w.files.get_metadata(path)
            ts = pd.Timestamp(m.last_modified).value // 10**6 if m.last_modified else None
            return f"size:{m.content_length},mtime:{m.last_modified}", ts
        st = w.workspace.get_status(path.replace("/Workspace", "", 1) if path.startswith("/Workspace/") else path)
        return f"size:{st.size},mtime:{st.modified_at}", st.modified_at
    except Exception as e:
        return f"unreadable:{type(e).__name__}", None

def code_manifest(job_id):
    s = w.api_client.do("GET", "/api/2.2/jobs/get", query={"job_id": int(job_id)})["settings"]
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
                if typ in lib: paths.append(("file", lib[typ]))
            if "maven" in lib: man[f"{k}:maven"] = lib["maven"].get("coordinates")
            if "pypi" in lib: man[f"{k}:pypi"] = lib["pypi"].get("package")
        for env in s.get("environments", []):
            if env["environment_key"] == t.get("environment_key"):
                for dep in env["spec"].get("java_dependencies", []) + env["spec"].get("dependencies", []):
                    if dep.startswith("/"): paths.append(("file", dep))
                    else: man[f"{k}:dep:{dep}"] = dep
        for kind, p in paths:
            fp, ts = _ws_item(p) if kind == "ws" else _file_item(p)
            man[f"{k}:{p}"] = fp
            newest = max(newest, ts or 0)
    return man, newest

def _version(man):
    return hashlib.sha256(json.dumps(man, sort_keys=True).encode()).hexdigest()[:12]

def record_code_version(vname, run_id, man, source="captured_at_submit"):
    _merge([(benchmark_id, vname, str(run_id), _version(man), code_label, json.dumps(man, sort_keys=True), source,
             datetime.now(timezone.utc))], CODE_SCHEMA, CODE_TBL, "run_id")
    return _version(man)

# Backfill runs recorded without a version: the current code is assigned only if nothing changed since the run started.
known = {r.run_id for r in spark.table(CODE_TBL).filter(f"benchmark_id = '{benchmark_id}'").collect()}
old = [r for r in spark.table(RUNS_TBL).filter(f"benchmark_id = '{benchmark_id}'").collect() if r.run_id not in known and r.start_time]
_cache = {}
for r in old:
    if r.variant_name not in all_jobs:
        continue
    jid = all_jobs[r.variant_name]["new_job_id"]
    _cache.setdefault(jid, code_manifest(jid))
    man, newest = _cache[jid]
    if newest and newest <= r.start_time.timestamp() * 1000:
        record_code_version(r.variant_name, r.run_id, man, "backfill_verified")
    else:
        _merge([(benchmark_id, r.variant_name, r.run_id, "unknown", None, None, "backfill_code_changed_since",
                 datetime.now(timezone.utc))], CODE_SCHEMA, CODE_TBL, "run_id")
print(f"Backfilled code version for {len(old)} earlier run(s)")
cur_man, _ = code_manifest(next(iter(all_jobs.values()))["new_job_id"])
print(f"Current code version: {_version(cur_man)}" + (f" (label {code_label})" if code_label else ""))

# COMMAND ----------

# DBTITLE 1,Input guard: record input table versions
def table_versions(tables):
    out = {}
    for t in tables:
        try:
            out[t] = spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first().version
        except Exception as e:
            out[t] = f"unreadable: {type(e).__name__}"
    return out

inputs_before = table_versions(input_tables)
if input_tables:
    print("Input table versions before the runs:", inputs_before)
else:
    print("No input_tables given: input guard skipped (outputs are isolated per variant, inputs are read directly).")

# COMMAND ----------

# DBTITLE 1,Execute benchmark runs
results = []
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

def _submit(vname, job, run_number):
    try:
        man, _ = code_manifest(job["new_job_id"])
        run = w.jobs.run_now(job_id=int(job["new_job_id"]))
        ver = record_code_version(vname, run.run_id, man)
        print(f"    Submitted {vname} run {run_number} -> run_id {run.run_id}  code {ver}")
        sync_runs({vname: job})
        results.append((vname, str(run.run_id)))
        return str(run.run_id)
    except Exception as e:
        print(f"    FAILED to submit {vname}: {e}")
        return None

def _poll(active):
    pending, t0 = {rid: vn for vn, rid in active if rid}, time.time()
    while pending:
        time.sleep(POLL_INTERVAL)
        for rid in list(pending):
            try:
                run = w.jobs.get_run(int(rid))
                if bc.ev(run.state.life_cycle_state) in TERMINAL:
                    vn = pending.pop(rid)
                    sync_runs({vn: bench_jobs[vn]})
                    print(f"    done {vn}: {bc.ev(run.state.result_state)} in {((run.end_time or 0) - (run.start_time or 0)) / 60000:.1f} min")
            except Exception as e:
                print(f"    poll error for run {rid}: {e}")
        if pending:
            print(f"    [{(time.time() - t0) / 60:.0f}m] waiting on {len(pending)}: {', '.join(pending.values())}")

if dry_run:
    print("DRY RUN - nothing submitted. Set dry_run=false to start the runs.")
elif run_mode == "sequential":
    for i, (vn, j, rn) in enumerate(run_queue, 1):
        print(f"\n[{i}/{len(run_queue)}] {vn} run {rn}")
        rid = _submit(vn, j, rn)
        if rid:
            _poll([(vn, rid)])
elif run_mode == "parallel":
    _poll([(vn, _submit(vn, j, rn)) for vn, j, rn in run_queue])
else:
    # A variant appears at most once per batch (benchmark jobs allow one concurrent run)
    chunks, left = [], list(run_queue)
    while left:
        chunk, seen = [], set()
        for item in list(left):
            if len(chunk) < batch_size and item[0] not in seen:
                chunk.append(item); seen.add(item[0]); left.remove(item)
        chunks.append(chunk)
    for bi, chunk in enumerate(chunks, 1):
        print(f"\nBatch {bi}/{len(chunks)} ({len(chunk)} runs)")
        _poll([(vn, _submit(vn, j, rn)) for vn, j, rn in chunk])
        if bi < len(chunks) and batch_pause > 0:
            time.sleep(batch_pause)
print(f"\n{len(results)} run(s) submitted in this session; all recorded in {RUNS_TBL}")

# COMMAND ----------

# DBTITLE 1,Input guard check and recorded runs
if input_tables and not dry_run:
    after = table_versions(input_tables)
    changed = {t: (inputs_before[t], after[t]) for t in input_tables if inputs_before[t] != after[t]}
    if changed:
        print(f"WARNING: input tables changed during the benchmark (before, after): {changed}. "
              f"A variant may have written to an input, or another writer was active; results may not be comparable.")
    else:
        print(f"Input guard OK: {len(input_tables)} input table(s) unchanged")

sync_runs(all_jobs)
display(spark.table(RUNS_TBL).filter(f"benchmark_id = '{benchmark_id}'")
        .join(spark.table(CODE_TBL).select("run_id", "code_version", "code_label"), "run_id", "left")
        .select("variant_name", "run_number", "run_id", "code_version", "status", "result_state", "start_time",
                "duration_seconds", "task_only_seconds", "overhead_seconds", "output_schema")
        .orderBy("variant_name", "run_number"))
