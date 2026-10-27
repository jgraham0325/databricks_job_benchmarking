"""Ranking, picks and verdicts used by 05_analysis.

Pure pandas logic: no Spark and no API calls. `summary` is the per-variant DataFrame built in 05
(index = variant_name; columns such as total_min, task_only_min, cost_usd, *_vs_base_pct, *_spread_pct,
n_runs, n_failed, vm_cost_suspect). Lower time and cost are better; *_vs_base_pct < 0 = better than baseline.
"""
import numpy as np
import pandas as pd

AI_MODEL_PREFERENCE = ("databricks-claude-sonnet", "databricks-claude", "databricks-meta-llama", "databricks-gpt", "databricks-")


def choose_ai_model(names):
    """Pick a chat endpoint from the names of READY chat endpoints ('' if none)."""
    for pref in AI_MODEL_PREFERENCE:
        match = sorted((n for n in names if n.startswith(pref)), reverse=True)
        if match:
            return match[0]
    return names[0] if names else ""


def mark_cost_flags(runs):
    """Add is_serverless and vm_cost_suspect to the per-run DataFrame (in place) and return it.

    Serverless: all cost is serverless DBUs (VMs are in the DBU price), so vm_usd = 0 and no node_timeline
    rows is expected. Classic VM cost is suspect when the cost view says it is incomplete, or billed
    node-hours are below 40% of the median of successful classic runs (node_timeline rows missing).
    """
    runs["is_serverless"] = (runs.compute_type == "SERVERLESS") | (runs.dimension == "serverless")
    runs.loc[runs.is_serverless & runs.total_estimated_usd.notna(), "vm_usd"] = 0.0
    classic = runs[(runs.status == "SUCCESS") & ~runs.is_serverless]
    med_nh = classic.node_hours.median()
    runs["vm_cost_suspect"] = ~runs.is_serverless & ((runs.vm_cost_incomplete == True)  # noqa: E712 (nullable column)
                                                     | (pd.notna(med_nh) & (runs.node_hours < 0.4 * med_nh)))
    runs["vm_cost_suspect"] = runs.vm_cost_suspect.fillna(False).astype(bool)
    return runs


def spread_pct(s):
    """Run-to-run spread: (max - min) / median in %, NaN with fewer than 2 values."""
    s = s.dropna()
    return 100 * (s.max() - s.min()) / s.median() if len(s) > 1 and s.median() else np.nan


def pareto_front(df, x, y):
    """Index labels of the rows not beaten on both x and y (lower is better)."""
    d = df.dropna(subset=[x, y]).sort_values([x, y])
    front, best = [], np.inf
    for idx, r in d.iterrows():
        if r[y] < best:
            front.append(idx)
            best = r[y]
    return front


def make_picks(df, tcol, baseline):
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


def comparable_others(summary, basis, baseline):
    """Non-baseline variants that have both time (on this basis) and cost."""
    c = summary.dropna(subset=[f"{basis}_min", "cost_usd"])
    return [v for v in c.index if v != baseline]


def confidence(summary, v, basis, baseline, noise_pct):
    """Caveats for a picked variant ('ok' if none)."""
    r, notes = summary.loc[v], []
    if r.n_runs < 2:
        notes.append("screening only (1 run)")
    if r.n_failed:
        notes.append(f"{r.n_failed} failed run(s)")
    spread = r[f"{basis}_spread_pct"]
    if pd.notna(spread) and spread > noise_pct:
        notes.append(f"run-to-run spread {spread:.0f}%")
    if v == baseline:
        notes.append("no variant beat the baseline" if comparable_others(summary, basis, baseline) else "no other variants with results yet")
    else:
        dt, dc = r[f"{basis}_vs_base_pct"], r.cost_vs_base_pct
        if pd.notna(dt) and pd.notna(dc) and abs(dt) < noise_pct and abs(dc) < noise_pct:
            notes.append(f"within {noise_pct:.0f}% of baseline")
    return "; ".join(notes) or "ok"


def classify(d, better, worse, noise_pct):
    """better / worse / neutral for a % delta vs baseline (only beyond the noise threshold counts)."""
    if pd.isna(d):
        return "unknown"
    return better if d < -noise_pct else worse if d > noise_pct else "neutral"


def verdict(r, baseline, noise_pct):
    """(time_class, cost_class, verdict) for one variant row (task-only time basis).

    A time delta smaller than the run-to-run spread is reported as noisy. Suspect VM cost is never ranked.
    Verdicts: WIN, COST WIN, TIME WIN, TRADE-OFF, WORSE, NEUTRAL (and 'baseline' for the baseline).
    """
    if r.variant_name == baseline:
        return "baseline", "baseline", "baseline"
    t = classify(r.task_only_vs_base_pct, "faster", "slower", noise_pct)
    if t in ("faster", "slower"):
        noisy = pd.notna(r.task_only_spread_pct) and r.task_only_spread_pct > abs(r.task_only_vs_base_pct)
        t += " (within run-to-run spread)" if noisy else " (confirmed: larger than run-to-run spread)"
    c = "unranked (vm_cost_suspect)" if r.vm_cost_suspect else classify(r.cost_vs_base_pct, "cheaper", "costlier", noise_pct)
    tf, cf, ts, cs = t.startswith("faster"), c == "cheaper", t.startswith("slower"), c == "costlier"
    if tf and cf:
        v = "WIN: faster and cheaper"
    elif cf and not ts:
        v = "COST WIN: cheaper, time neutral"
    elif tf and not cs:
        v = "TIME WIN: faster" + (", cost unranked" if c.startswith("unranked") else ", cost neutral")
    elif tf and cs:
        v = "TRADE-OFF: faster but costlier"
    elif cf and ts:
        v = "TRADE-OFF: cheaper but slower"
    elif ts or cs:
        v = "WORSE"
    else:
        v = "NEUTRAL: within noise on both"
    return t, c, v
