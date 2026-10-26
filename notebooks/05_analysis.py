# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Analysis
# MAGIC
# MAGIC Ranks the variants by time and estimated cost. **Total time** = wall clock including cluster startup; **task-only time** = wall clock while at least one task was executing. Cost comes from the job run cost view (DBU + VM, whole run). Produces Pareto charts, fastest / cheapest / best-balance picks, confirmation candidates, an AI-written recommendation (`ai_query`), and the reporting views used by the dashboard and Genie agent (`rpt_variant_results`, `rpt_runs`, `rpt_recommendations`).
# MAGIC
# MAGIC System tables lag by a few hours: re-run this notebook later if cost is still pending.

# COMMAND ----------

# DBTITLE 1,Configuration and runs with cost
import os, sys, json
sys.path.insert(0, os.getcwd())
import bench_common as bc
import pandas as pd, numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

cfg = bc.setup(dbutils, spark)
FQN = cfg.fqn
benchmark_id = bc.benchmark_id_widget(dbutils)
dbutils.widgets.text("cost_view", cfg.cost_view, "Cost view (from 00_setup_cost_model)")
dbutils.widgets.dropdown("time_basis", "total", ["total", "task_only"], "Time basis for confirmation picks")
dbutils.widgets.text("noise_pct", "5", "Noise threshold (%)")
dbutils.widgets.text("baseline_variant", "baseline", "Baseline variant")
dbutils.widgets.text("code_version", "", "Code version label (optional; default = recorded per run)")
dbutils.widgets.text("ai_model", "", "Chat endpoint for ai_query (blank = auto-detect, 'none' = skip)")
cost_view = dbutils.widgets.get("cost_view").strip()
time_basis = dbutils.widgets.get("time_basis")
noise_pct = float(dbutils.widgets.get("noise_pct"))
baseline = dbutils.widgets.get("baseline_variant").strip()
code_version = dbutils.widgets.get("code_version").strip() or None
ai_model = dbutils.widgets.get("ai_model").strip()
if ai_model.lower() == "none":
    ai_model = ""
elif not ai_model:
    # Prefer a ready Foundation Model API chat endpoint present in this workspace
    try:
        eps = [e for e in bc.w.serving_endpoints.list()
               if e.state and bc.ev(e.state.ready) == "READY" and (e.task or "").endswith("chat")]
        names = [e.name for e in eps]
        for pref in ("databricks-claude-sonnet", "databricks-claude", "databricks-meta-llama", "databricks-gpt", "databricks-"):
            match = sorted((n for n in names if n.startswith(pref)), reverse=True)
            if match:
                ai_model = match[0]
                break
        ai_model = ai_model or (names[0] if names else "")
    except Exception as e:
        print(f"Could not list serving endpoints ({type(e).__name__}); AI recommendation skipped")
print(f"AI model: {ai_model or 'none (AI recommendation skipped)'}")
assert spark.catalog.tableExists(cost_view), f"{cost_view} not found: run 00_setup_cost_model first"

runs = spark.sql(f"""
  SELECT r.variant_name, v.dimension, v.description, r.run_number, r.run_id, r.new_job_id,
         r.status, r.result_state, r.start_time,
         r.duration_seconds, r.task_only_seconds, r.task_exec_sum_seconds, r.overhead_seconds,
         r.max_setup_seconds, r.n_tasks,
         c.total_estimated_usd, c.dbu_usd, c.vm_usd, c.dbus, c.vm_cost_incomplete,
         c.avg_cpu_pct, c.peak_cpu_pct, c.avg_worker_cpu_pct, c.max_mem_pct, c.max_swap_pct,
         c.utilization_hint, c.node_hours, c.spot_node_hours,
         c.compute_type, c.serverless_dbu_usd, cv.code_version AS run_code_version
  FROM {FQN}.bench_runs r
  LEFT JOIN {FQN}.bench_run_code_versions cv ON cv.run_id = r.run_id
  LEFT JOIN {FQN}.bench_variants v ON v.benchmark_id = r.benchmark_id AND v.variant_name = r.variant_name
  LEFT JOIN {cost_view} c ON c.job_id = r.new_job_id AND c.run_id = r.run_id
  WHERE r.benchmark_id = '{benchmark_id}'
""").toPandas()
assert len(runs), f"No runs recorded for {benchmark_id}: run 04_orchestrator first"

num_cols = ["duration_seconds", "task_only_seconds", "task_exec_sum_seconds", "overhead_seconds", "max_setup_seconds",
            "total_estimated_usd", "dbu_usd", "vm_usd", "dbus", "avg_cpu_pct", "peak_cpu_pct", "avg_worker_cpu_pct",
            "max_mem_pct", "max_swap_pct", "node_hours", "spot_node_hours", "serverless_dbu_usd"]
for c in num_cols:
    runs[c] = pd.to_numeric(runs[c], errors="coerce").astype(float)
runs["total_min"] = runs.duration_seconds / 60
runs["task_only_min"] = runs.task_only_seconds / 60
runs["overhead_min"] = runs.overhead_seconds / 60
runs["task_share_pct"] = 100 * runs.task_only_seconds / runs.duration_seconds
runs["cost_pending"] = (runs.status == "SUCCESS") & runs.total_estimated_usd.isna()
# Serverless: all cost is serverless DBUs (VMs are in the DBU price), so vm_usd = 0 and no node_timeline rows is expected
runs["is_serverless"] = (runs.compute_type == "SERVERLESS") | (runs.dimension == "serverless")
runs.loc[runs.is_serverless & runs.total_estimated_usd.notna(), "vm_usd"] = 0.0
# VM cost is suspect when billed node-hours are far below the classic median (node_timeline rows missing)
_classic = runs[(runs.status == "SUCCESS") & ~runs.is_serverless]
_med_nh = _classic.node_hours.median()
runs["vm_cost_suspect"] = ~runs.is_serverless & ((runs.vm_cost_incomplete == True) | (pd.notna(_med_nh) & (runs.node_hours < 0.4 * _med_nh)))
runs["vm_cost_suspect"] = runs.vm_cost_suspect.fillna(False).astype(bool)

_vers = sorted(runs.loc[runs.status == "SUCCESS", "run_code_version"].dropna().unique())
if code_version is None and _vers:
    code_version = ", ".join(_vers)
if len(_vers) > 1:
    print(f"WARNING runs used {len(_vers)} code versions ({code_version}): results mix code and config changes")

ok = runs[runs.status == "SUCCESS"]
print(f"Benchmark {benchmark_id}: {len(runs)} run(s), {runs.variant_name.nunique()} variant(s), status {runs.status.value_counts().to_dict()}")
print(f"Code version: {code_version or 'not recorded'}")
if runs.cost_pending.any():
    print("WARNING no cost yet (system tables lag a few hours), re-run later: " + ", ".join(runs.loc[runs.cost_pending, "variant_name"]))
if runs.vm_cost_suspect.any():
    print("WARNING VM cost incomplete or suspect: " + ", ".join(runs.loc[runs.vm_cost_suspect, "variant_name"].unique()))
if baseline not in set(ok.variant_name):
    print(f"WARNING baseline '{baseline}' has no successful run: deltas vs baseline will be empty")

# COMMAND ----------

# DBTITLE 1,Per-run detail
display(runs[["variant_name", "dimension", "run_number", "run_id", "status", "total_min", "task_only_min", "overhead_min",
              "task_share_pct", "total_estimated_usd", "dbu_usd", "vm_usd", "avg_cpu_pct", "avg_worker_cpu_pct",
              "max_mem_pct", "utilization_hint", "cost_pending", "vm_cost_suspect"]].sort_values(["variant_name", "run_number"]).round(2))

# COMMAND ----------

# DBTITLE 1,Per-variant summary (median of successful runs)
def _spread(s):
    s = s.dropna()
    return 100 * (s.max() - s.min()) / s.median() if len(s) > 1 and s.median() else np.nan

g = ok.groupby("variant_name")
summary = pd.DataFrame({
    "n_runs": g.size(), "total_min": g.total_min.median(), "task_only_min": g.task_only_min.median(),
    "overhead_min": g.overhead_min.median(), "task_share_pct": g.task_share_pct.median(),
    "cost_usd": g.total_estimated_usd.median(), "dbu_usd": g.dbu_usd.median(), "vm_usd": g.vm_usd.median(),
    "avg_cpu_pct": g.avg_cpu_pct.median(), "avg_worker_cpu_pct": g.avg_worker_cpu_pct.median(),
    "max_mem_pct": g.max_mem_pct.max(), "vm_cost_suspect": g.vm_cost_suspect.any(),
    "first_run_at": g.start_time.min(), "last_run_at": g.start_time.max(),
    "total_spread_pct": g.total_min.agg(_spread), "task_only_spread_pct": g.task_only_min.agg(_spread),
})
summary = runs.groupby("variant_name")[["dimension", "description"]].first().join(summary, how="left")
summary["n_runs"] = summary.n_runs.fillna(0).astype(int)
summary["n_failed"] = runs[runs.status == "FAILED"].groupby("variant_name").size().reindex(summary.index).fillna(0).astype(int)
for m, out_col in [("total_min", "total_vs_base_pct"), ("task_only_min", "task_only_vs_base_pct"), ("cost_usd", "cost_vs_base_pct")]:
    b = summary.loc[baseline, m] if baseline in summary.index else np.nan
    summary[out_col] = 100 * (summary[m] / b - 1) if pd.notna(b) and b else np.nan
summary.index.name = "variant_name"
summary = summary.sort_values(f"{time_basis}_min")
display(summary.reset_index()[["variant_name", "dimension", "n_runs", "n_failed", "total_min", "task_only_min", "overhead_min",
        "task_share_pct", "total_vs_base_pct", "task_only_vs_base_pct", "cost_usd", "cost_vs_base_pct", "dbu_usd", "vm_usd",
        "total_spread_pct", "task_only_spread_pct", "avg_cpu_pct", "avg_worker_cpu_pct", "max_mem_pct", "vm_cost_suspect",
        "description"]].round(2))

# COMMAND ----------

# DBTITLE 1,Chart: total vs task-only time
plot = summary.dropna(subset=["total_min"]).sort_values(f"{time_basis}_min")
if plot.empty:
    print("No successful runs to plot yet.")
else:
    y, h = np.arange(len(plot)), 0.38
    fig, ax = plt.subplots(figsize=(10, max(2.5, 0.65 * len(plot) + 1.2)))
    ax.barh(y - h / 2, plot.total_min, h, label="Total (incl. cluster startup)", color="#9bb7d4")
    ax.barh(y + h / 2, plot.task_only_min, h, label="Task-only (executing)", color="#1f4e79")
    for i, (t, k) in enumerate(zip(plot.total_min, plot.task_only_min)):
        ax.text(t, i - h / 2, f" {t:.1f}", va="center", fontsize=8)
        if pd.notna(k):
            ax.text(k, i + h / 2, f" {k:.1f}", va="center", fontsize=8)
    ax.set_yticks(y)
    ax.set_yticklabels([f"{n} (baseline)" if n == baseline else n for n in plot.index])
    ax.invert_yaxis()
    ax.set_xlabel("Minutes (median of successful runs)")
    ax.set_title(f"Benchmark {benchmark_id}: total vs task-only time")
    ax.legend(loc="lower right"); ax.grid(axis="x", alpha=0.3)
    plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Chart: cost vs time Pareto
def pareto_front(df, x, y):
    """Variants not beaten on both x and y (lower is better)."""
    d = df.dropna(subset=[x, y]).sort_values([x, y])
    front, best = [], np.inf
    for idx, r in d.iterrows():
        if r[y] < best:
            front.append(idx); best = r[y]
    return front

pc = summary.dropna(subset=["cost_usd", "total_min"])
if pc.empty:
    print("No cost data yet (system tables lag a few hours): re-run later.")
else:
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.5), sharey=True)
    for ax, (col, label) in zip(axes, [("total_min", "Total minutes (incl. startup)"), ("task_only_min", "Task-only minutes")]):
        front = pareto_front(pc, col, "cost_usd")
        others = pc.drop(index=front)
        ax.scatter(others[col], others.cost_usd, color="#9bb7d4", s=60, label="Dominated")
        f = pc.loc[front].sort_values(col)
        ax.plot(f[col], f.cost_usd, "-o", color="#c0392b", ms=8, label="Pareto front")
        if baseline in pc.index:
            bx, by = pc.loc[baseline, col], pc.loc[baseline, "cost_usd"]
            ax.scatter([bx], [by], marker="*", s=260, color="black", zorder=5, label="Baseline")
            ax.axvline(bx, ls="--", lw=0.8, color="grey"); ax.axhline(by, ls="--", lw=0.8, color="grey")
        for n, r in pc.iterrows():
            ax.annotate(n, (r[col], r.cost_usd), textcoords="offset points", xytext=(6, 4), fontsize=8)
        ax.set_xlabel(label); ax.grid(alpha=0.3)
    axes[0].set_ylabel("Estimated cost per run (USD, DBU + VM)")
    axes[0].legend(loc="best", fontsize=8)
    plt.suptitle(f"Benchmark {benchmark_id}: lower-left is better", y=1.02)
    plt.tight_layout(); plt.show()

# COMMAND ----------

# DBTITLE 1,Picks and confirmation candidates
def make_picks(df, tcol):
    """Fastest, cheapest, best balance (Pareto point closest to the origin, time and cost normalised by the baseline).
    Variants with suspect VM cost are excluded: their cost is understated, not cheaper."""
    c = df[~df.vm_cost_suspect.fillna(False).astype(bool)].dropna(subset=[tcol, "cost_usd"])
    if c.empty:
        return {}
    bt = c.loc[baseline, tcol] if baseline in c.index else c[tcol].min()
    bcost = c.loc[baseline, "cost_usd"] if baseline in c.index else c.cost_usd.min()
    front = pareto_front(c, tcol, "cost_usd")
    score = np.sqrt((c.loc[front, tcol] / bt) ** 2 + (c.loc[front, "cost_usd"] / bcost) ** 2)
    return {"fastest": c[tcol].idxmin(), "cheapest": c.cost_usd.idxmin(), "best_balance": score.idxmin()}

def comparable_others(basis):
    c = summary.dropna(subset=[f"{basis}_min", "cost_usd"])
    return [v for v in c.index if v != baseline]

def confidence(v, basis):
    r, notes = summary.loc[v], []
    if r.n_runs < 2: notes.append("screening only (1 run)")
    if r.n_failed: notes.append(f"{r.n_failed} failed run(s)")
    spread = r[f"{basis}_spread_pct"]
    if pd.notna(spread) and spread > noise_pct: notes.append(f"run-to-run spread {spread:.0f}%")
    if v == baseline:
        notes.append("no variant beat the baseline" if comparable_others(basis) else "no other variants with results yet")
    else:
        dt, dc = r[f"{basis}_vs_base_pct"], r.cost_vs_base_pct
        if pd.notna(dt) and pd.notna(dc) and abs(dt) < noise_pct and abs(dc) < noise_pct:
            notes.append(f"within {noise_pct:.0f}% of baseline")
    return "; ".join(notes) or "ok"

rows, picks_by_basis = [], {}
for basis in ["total", "task_only"]:
    picks = make_picks(summary, f"{basis}_min")
    picks_by_basis[basis] = picks
    for role, v in picks.items():
        r = summary.loc[v]
        rows.append(dict(time_basis=basis, pick=role, variant_name=v, dimension=r.dimension, minutes=round(r[f"{basis}_min"], 2),
                         time_vs_base_pct=round(r[f"{basis}_vs_base_pct"], 1), cost_usd=round(r.cost_usd, 4),
                         cost_vs_base_pct=round(r.cost_vs_base_pct, 1), confidence=confidence(v, basis)))
if not rows:
    print("No variants with both timing and cost yet: nothing to pick.")
else:
    display(pd.DataFrame(rows))
    confirm = [v for v in dict.fromkeys(picks_by_basis[time_basis].values()) if v != baseline][:3]
    if confirm:
        print(f"Confirmation candidates ({time_basis} basis): {', '.join(confirm)}")
        print(f"In 04_orchestrator set variants_filter = {','.join([baseline] + confirm)} and max_runs = 2 (or 3)")
    elif not comparable_others(time_basis):
        print("Only the baseline has timing and cost so far: run the other variants, then re-run this notebook.")
    else:
        print(f"No variant beat the baseline on the {time_basis} basis: nothing to confirm.")

# COMMAND ----------

# DBTITLE 1,Save summary to bench_results
RESULTS_TBL = f"{FQN}.bench_results"
out = summary.reset_index()
out.insert(0, "benchmark_id", benchmark_id)
out["analysed_at"] = pd.Timestamp.now(tz="UTC")
src = spark.sql(f"""SELECT concat_ws(',', collect_set(job_id)) AS ids, concat_ws(', ', collect_set(job_name)) AS names
                     FROM {FQN}.bench_source_jobs WHERE benchmark_id = '{benchmark_id}'""").first()
if code_version is None and spark.catalog.tableExists(RESULTS_TBL) and "code_version" in spark.table(RESULTS_TBL).columns:
    code_version = spark.sql(f"SELECT max(code_version) v FROM {RESULTS_TBL} WHERE benchmark_id = '{benchmark_id}'").first().v
out["source_job_ids"], out["source_job_names"], out["code_version"] = src.ids, src.names, code_version

clean = summary[~summary.vm_cost_suspect.fillna(False).astype(bool)]
for basis in ["total", "task_only"]:
    front = set(pareto_front(clean, f"{basis}_min", "cost_usd"))
    out[f"is_pareto_{basis}"] = out.variant_name.isin(front)
    roles = {}
    for role, v in picks_by_basis.get(basis, {}).items():
        roles.setdefault(v, []).append(role)
    out[f"picks_{basis}"] = out.variant_name.map(lambda v: ", ".join(roles.get(v, [])) or None)
out["is_baseline"] = out.variant_name == baseline
out["confidence_task_only"] = out.variant_name.map(
    lambda v: confidence(v, "task_only") if pd.notna(summary.loc[v, "task_only_min"]) else "no successful run")
ov = spark.sql(f"SELECT variant_name, overrides_json FROM {FQN}.bench_variants WHERE benchmark_id = '{benchmark_id}'").toPandas()
out = out.merge(ov, on="variant_name", how="left")
STR_COLS = ["dimension", "description", "overrides_json", "code_version", "picks_total", "picks_task_only",
            "source_job_ids", "source_job_names", "confidence_task_only"]
for c in STR_COLS:
    out[c] = out[c].where(out[c].notna(), None).astype(object)
out["vm_cost_suspect"] = out.vm_cost_suspect.fillna(False).astype(bool)
sdf = spark.createDataFrame(out)
for c in STR_COLS:
    sdf = sdf.withColumn(c, F.col(c).cast("string"))  # all-null columns would otherwise be void
bc.replace_benchmark_rows(spark, sdf, RESULTS_TBL, benchmark_id)
print(f"Saved {len(out)} variant summaries to {RESULTS_TBL}")

# COMMAND ----------

# DBTITLE 1,AI recommendation
RECS_TBL = f"{FQN}.bench_recommendations"
src_clusters = spark.sql(f"""
  SELECT job_name, job_cluster_key, worker_node_type, driver_node_type, num_workers, min_workers, max_workers,
         spark_version, runtime_engine, azure_availability, tasks_on_cluster, spark_conf_json
  FROM {FQN}.bench_source_clusters WHERE benchmark_id = '{benchmark_id}'""").toPandas()
res_cols = ["variant_name", "dimension", "n_runs", "n_failed", "task_only_min", "total_min", "overhead_min", "cost_usd",
            "task_only_vs_base_pct", "total_vs_base_pct", "cost_vs_base_pct", "task_only_spread_pct", "avg_worker_cpu_pct",
            "max_mem_pct", "vm_cost_suspect", "is_pareto_task_only", "picks_task_only", "confidence_task_only",
            "overrides_json", "description"]
res = out[res_cols].copy()

# Every variant is classified in code so the model doesn't have to. A delta counts only beyond the noise
# threshold; a time delta smaller than the run-to-run spread is reported as noisy. Suspect VM cost is never ranked.
def _cls(d, better, worse):
    if pd.isna(d): return "unknown"
    return better if d < -noise_pct else worse if d > noise_pct else "neutral"

def verdict(r):
    if r.variant_name == baseline:
        return "baseline", "baseline", "baseline"
    t = _cls(r.task_only_vs_base_pct, "faster", "slower")
    if t in ("faster", "slower"):
        noisy = pd.notna(r.task_only_spread_pct) and r.task_only_spread_pct > abs(r.task_only_vs_base_pct)
        t += " (within run-to-run spread)" if noisy else " (confirmed: larger than run-to-run spread)"
    c = "unranked (vm_cost_suspect)" if r.vm_cost_suspect else _cls(r.cost_vs_base_pct, "cheaper", "costlier")
    tf, cf, ts, cs = t.startswith("faster"), c == "cheaper", t.startswith("slower"), c == "costlier"
    if tf and cf: v = "WIN: faster and cheaper"
    elif cf and not ts: v = "COST WIN: cheaper, time neutral"
    elif tf and not cs: v = "TIME WIN: faster" + (", cost unranked" if c.startswith("unranked") else ", cost neutral")
    elif tf and cs: v = "TRADE-OFF: faster but costlier"
    elif cf and ts: v = "TRADE-OFF: cheaper but slower"
    elif ts or cs: v = "WORSE"
    else: v = "NEUTRAL: within noise on both"
    return t, c, v

res[["time_class", "cost_class", "verdict"]] = res.apply(lambda r: pd.Series(verdict(r)), axis=1)
res["n_suspect_cost_runs"] = res.variant_name.map(ok.groupby("variant_name").vm_cost_suspect.sum()).fillna(0).astype(int)
for c in res.select_dtypes("number").columns:
    res[c] = res[c].round(2)
display(res[["variant_name", "n_runs", "task_only_vs_base_pct", "cost_vs_base_pct", "time_class", "cost_class", "verdict"]])
task_types = spark.sql(f"SELECT tasks FROM {FQN}.bench_source_jobs WHERE benchmark_id = '{benchmark_id}'").toPandas().tasks.tolist()

prompt = f"""You are a Databricks performance engineer. Recommend changes to a Databricks job's
cluster configuration based ONLY on the benchmark data below. Changes will be applied by editing the
job definition in git, so be concrete (cluster key, setting, old value -> new value).

Rules:
- Primary time metric is task-only minutes (cluster startup is excluded because it is noisy and
  dominated by VM provisioning). Mention total time only if it changes the conclusion.
- Cost is estimated USD per run (DBU + VM) and includes startup.
- Each variant has a precomputed verdict (noise threshold {noise_pct:.0f}% of baseline, judged separately
  for task-only time and cost). Use the verdict exactly as given; never reclassify a variant.
  * WIN, COST WIN and TIME WIN variants must each appear under Changes to apply in git (if worth adopting)
    or Next experiments. Never list them under Not worth changing.
  * Not worth changing may contain ONLY variants whose verdict is WORSE or NEUTRAL.
  * TRADE-OFF variants go under Confidence and caveats, stating what is gained and what is lost.
  * "(within run-to-run spread)" means the time difference is smaller than run-to-run variation: say
    it needs more runs, not that it is a win or a loss. "(confirmed: larger than run-to-run spread)" means
    the difference is real: state it as fact and do not ask for more runs to confirm it.
- For each change in git, take the cluster keys and the new values ONLY from that variant's overrides_json,
  and the old values ONLY from the baseline clusters JSON. Name only clusters that overrides_json touches and
  whose value actually changes; never write "X/Y" or guess a value.
- n_runs = 1 means screening only; recommend confirmation runs before adopting. State the baseline's
  n_runs if it differs from the other variants.
- vm_cost_suspect = true means VM cost is understated (missing telemetry) in n_suspect_cost_runs of the
  runs: do not rank its cost, and say how many runs are affected.
- The job's tasks and their types are listed below. Recommend compute/config changes only; do not tell
  the user to change task types that are already correct.
- dimension = serverless: all tasks on serverless compute; its cost is serverless DBUs only (vm_usd = 0 is
  expected, VMs are included in the DBU price). serverless_standard is cheaper with slower startup,
  serverless_perf_optimized starts faster at a higher price. Cluster Spark confs are dropped on serverless
  (see description). Compare them to the baseline like any other variant.
- One-factor-at-a-time benchmark: suggest which winning changes to combine in a follow-up
  experiment, but do not claim combined effects.
- Use only numbers from the data. Do not invent values.

Output GitHub markdown, under 300 words, with no title and no horizontal rules, and exactly these bold headings:
**Recommendation** (2-3 sentences: best balance of cost and task-only runtime)
**Changes to apply in git** (bullets)
**Not worth changing** (one bullet listing only WORSE or NEUTRAL variants)
**Confidence and caveats** (bullets)
**Next experiments** (bullets)

Benchmark: {benchmark_id}; baseline variant: {baseline}; code version: {code_version or 'not recorded'}
Baseline clusters (JSON): {src_clusters.to_json(orient='records')}
Source job tasks (JSON): {task_types}
Variant results (JSON): {res.to_json(orient='records')}
"""

rec = None
if ai_model:
    try:
        rec = spark.sql("SELECT ai_query(:m, :p) AS txt", args={"m": ai_model, "p": prompt}).first().txt
    except Exception as e:
        print(f"WARNING ai_query with endpoint '{ai_model}' failed ({type(e).__name__}: {str(e)[:300]}). "
              f"Set ai_model to a chat endpoint available in this workspace, or 'none' to skip.")
if rec is None:
    rec = ("_No AI recommendation generated._ Verdicts per variant: "
           + "; ".join(f"{r.variant_name}: {r.verdict}" for r in res.itertuples()))
rec_df = spark.createDataFrame([(benchmark_id, code_version, ai_model or None, time_basis, rec)],
    "benchmark_id string, code_version string, model string, time_basis string, recommendation_md string"
).withColumn("generated_at", F.current_timestamp())
bc.replace_benchmark_rows(spark, rec_df, RECS_TBL, benchmark_id)
displayHTML(f"<pre style='white-space:pre-wrap;font-family:inherit'>{rec}</pre>")

# COMMAND ----------

# DBTITLE 1,Reporting views for the dashboard and Genie agent
spark.sql(f"""
CREATE OR REPLACE VIEW {FQN}.rpt_variant_results
COMMENT 'One row per benchmark x variant (median of successful runs). Task-only time excludes cluster startup; total time includes it. Cost is estimated USD per run (DBU + VM). *_vs_base_pct: negative = faster/cheaper than the baseline. vm_cost_suspect = VM cost understated; exclude from cost rankings. code_version labels the job code under test, for comparing benchmarks over time.'
AS SELECT
  benchmark_id, code_version, source_job_ids, source_job_names, variant_name, dimension, description,
  is_baseline, n_runs, n_failed,
  round(task_only_min, 2) AS task_only_min, round(total_min, 2) AS total_min,
  round(overhead_min, 2) AS startup_overhead_min, round(task_share_pct, 1) AS task_share_pct,
  round(cost_usd, 4) AS cost_usd, round(dbu_usd, 4) AS dbu_usd, round(vm_usd, 4) AS vm_usd,
  round(task_only_vs_base_pct, 1) AS task_only_vs_base_pct, round(total_vs_base_pct, 1) AS total_vs_base_pct,
  round(cost_vs_base_pct, 1) AS cost_vs_base_pct,
  round(task_only_spread_pct, 1) AS task_only_spread_pct, round(total_spread_pct, 1) AS total_spread_pct,
  round(avg_worker_cpu_pct, 1) AS avg_worker_cpu_pct, round(max_mem_pct, 1) AS max_mem_pct,
  vm_cost_suspect, is_pareto_task_only, is_pareto_total, picks_task_only, picks_total,
  confidence_task_only, overrides_json,
  CASE WHEN is_baseline THEN 'Baseline' WHEN vm_cost_suspect THEN 'Cost suspect'
       WHEN is_pareto_task_only THEN 'Pareto front' ELSE 'Dominated' END AS pareto_class_task_only,
  first_run_at, last_run_at, analysed_at
FROM {FQN}.bench_results""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FQN}.rpt_runs
COMMENT 'One row per benchmark job run, with timings from the Jobs API and estimated cost/utilisation from system tables (cost lags a few hours).'
AS SELECT r.benchmark_id, r.variant_name, v.dimension, r.run_number, r.run_id, r.new_job_id AS job_id,
  r.status, r.start_time, r.end_time,
  round(r.duration_seconds / 60, 2) AS total_min, round(r.task_only_seconds / 60, 2) AS task_only_min,
  round(r.overhead_seconds / 60, 2) AS startup_overhead_min, r.n_tasks,
  round(c.total_estimated_usd, 4) AS cost_usd, round(c.dbu_usd, 4) AS dbu_usd, round(c.vm_usd, 4) AS vm_usd,
  c.dbus, c.node_hours, c.avg_cpu_pct, c.avg_worker_cpu_pct, c.max_mem_pct, c.utilization_hint,
  cv.code_version, c.compute_type
FROM {FQN}.bench_runs r
LEFT JOIN {FQN}.bench_run_code_versions cv ON cv.run_id = r.run_id
LEFT JOIN {FQN}.bench_variants v ON v.benchmark_id = r.benchmark_id AND v.variant_name = r.variant_name
LEFT JOIN {cost_view} c ON c.job_id = r.new_job_id AND c.run_id = r.run_id""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FQN}.rpt_recommendations
COMMENT 'Latest AI-generated recommendation (markdown) per benchmark, grounded in rpt_variant_results.'
AS SELECT benchmark_id, code_version, generated_at, model, recommendation_md FROM {FQN}.bench_recommendations""")

for v in ["rpt_variant_results", "rpt_runs", "rpt_recommendations"]:
    print(v, spark.table(f"{FQN}.{v}").where(f"benchmark_id = '{benchmark_id}'").count(), "rows")
