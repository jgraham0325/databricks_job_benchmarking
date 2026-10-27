"""Ranking, picks, verdicts and cost flags (05_analysis)."""
from types import SimpleNamespace as NS

import numpy as np
import pandas as pd
import pytest

import bench_analysis as ba

NAN = np.nan


def summary(rows, baseline="baseline"):
    """Per-variant summary like 05 builds: rows = {name: (task_only_min, cost_usd, extra...)}."""
    defaults = dict(n_runs=2, n_failed=0, task_only_spread_pct=NAN, total_spread_pct=NAN, vm_cost_suspect=False)
    df = pd.DataFrame([{"variant_name": n, "task_only_min": r[0], "total_min": r[0], "cost_usd": r[1],
                        **defaults, **(r[2] if len(r) > 2 else {})}
                       for n, r in rows.items()]).set_index("variant_name")
    for m, out in [("total_min", "total_vs_base_pct"), ("task_only_min", "task_only_vs_base_pct"), ("cost_usd", "cost_vs_base_pct")]:
        b = df.loc[baseline, m] if baseline in df.index else NAN
        df[out] = 100 * (df[m] / b - 1) if pd.notna(b) and b else NAN
    return df


# Spread and Pareto --------------------------------------------------------------------------------------
@pytest.mark.parametrize("values, expected", [([10, 12], 100 * 2 / 11), ([10, 12, NAN], 100 * 2 / 11), ([10], NAN), ([], NAN)])
def test_spread_pct(values, expected):
    got = ba.spread_pct(pd.Series(values, dtype=float))
    assert (np.isnan(got) and np.isnan(expected)) or got == pytest.approx(expected)


def test_pareto_front_keeps_only_unbeaten_variants():
    df = summary({"baseline": (10, 1.0), "fast": (5, 2.0), "cheap": (12, 0.5), "dominated": (11, 1.5), "no_cost": (1, NAN)})
    assert ba.pareto_front(df, "task_only_min", "cost_usd") == ["fast", "baseline", "cheap"]


# Picks --------------------------------------------------------------------------------------------------
def test_picks_fastest_cheapest_and_best_balance():
    df = summary({"baseline": (10, 1.0), "fast": (5, 2.0), "cheap": (12, 0.5), "balanced": (7, 0.7)})
    assert ba.make_picks(df, "task_only_min", "baseline") == {"fastest": "fast", "cheapest": "cheap", "best_balance": "balanced"}


def test_picks_ignore_variants_with_suspect_vm_cost():
    df = summary({"baseline": (10, 1.0), "suspect": (9, 0.1, {"vm_cost_suspect": True}), "ok": (11, 0.9)})
    picks = ba.make_picks(df, "task_only_min", "baseline")
    assert "suspect" not in picks.values()
    assert picks["cheapest"] == "ok"


def test_picks_without_a_baseline_normalise_by_the_best_values():
    # normalised by min time 6 and min cost 1.0: a = (1.67, 1.0) is closer to the origin than b = (1.0, 2.0)
    df = summary({"a": (10, 1.0), "b": (6, 2.0)}, baseline="zz")
    assert ba.make_picks(df, "task_only_min", "zz") == {"fastest": "b", "cheapest": "a", "best_balance": "a"}


def test_no_picks_without_cost():
    assert ba.make_picks(summary({"baseline": (10, NAN)}), "task_only_min", "baseline") == {}


# Confidence ---------------------------------------------------------------------------------------------
def test_confidence_notes():
    df = summary({"baseline": (10, 1.0), "screen": (5, 0.5, {"n_runs": 1, "n_failed": 1, "task_only_spread_pct": 12.0}),
                  "same": (10.2, 1.01)})
    assert ba.confidence(df, "screen", "task_only", "baseline", 5) == \
        "screening only (1 run); 1 failed run(s); run-to-run spread 12%"
    assert ba.confidence(df, "same", "task_only", "baseline", 5) == "within 5% of baseline"
    assert ba.confidence(df, "baseline", "task_only", "baseline", 5) == "no variant beat the baseline"
    alone = summary({"baseline": (10, 1.0), "x": (NAN, NAN)})
    assert ba.confidence(alone, "baseline", "task_only", "baseline", 5) == "no other variants with results yet"


# Verdicts -----------------------------------------------------------------------------------------------
@pytest.mark.parametrize("d, expected", [(-10, "faster"), (10, "slower"), (-5, "neutral"), (5, "neutral"), (0, "neutral"),
                                         (NAN, "unknown")])
def test_classify_only_counts_deltas_beyond_the_noise_threshold(d, expected):
    assert ba.classify(d, "faster", "slower", 5) == expected


def row(dt, dc, spread=NAN, suspect=False, name="v"):
    return NS(variant_name=name, task_only_vs_base_pct=dt, cost_vs_base_pct=dc, task_only_spread_pct=spread,
              vm_cost_suspect=suspect)


@pytest.mark.parametrize("dt, dc, suspect, expected", [
    (-20, -20, False, "WIN: faster and cheaper"),
    (0, -20, False, "COST WIN: cheaper, time neutral"),
    (-20, 0, False, "TIME WIN: faster, cost neutral"),
    (-20, -50, True, "TIME WIN: faster, cost unranked"),
    (-20, 20, False, "TRADE-OFF: faster but costlier"),
    (20, -20, False, "TRADE-OFF: cheaper but slower"),
    (20, 0, False, "WORSE"),
    (0, 20, False, "WORSE"),
    (20, 20, False, "WORSE"),
    (1, -1, False, "NEUTRAL: within noise on both"),
    (0, -50, True, "NEUTRAL: within noise on both"),     # suspect cost is never a cost win
    (NAN, NAN, False, "NEUTRAL: within noise on both"),
])
def test_verdicts(dt, dc, suspect, expected):
    assert ba.verdict(row(dt, dc, suspect=suspect), "baseline", 5)[2] == expected


def test_time_delta_smaller_than_spread_is_reported_as_noisy():
    t, _, _ = ba.verdict(row(-10, 0, spread=15), "baseline", 5)
    assert t == "faster (within run-to-run spread)"
    t, _, _ = ba.verdict(row(-10, 0, spread=3), "baseline", 5)
    assert t == "faster (confirmed: larger than run-to-run spread)"


def test_suspect_cost_is_unranked():
    assert ba.verdict(row(0, -50, suspect=True), "baseline", 5)[1] == "unranked (vm_cost_suspect)"


def test_baseline_verdict():
    assert ba.verdict(row(0, 0, name="baseline"), "baseline", 5) == ("baseline", "baseline", "baseline")


# Cost flags ---------------------------------------------------------------------------------------------
def runs_frame(rows):
    cols = ["variant_name", "compute_type", "dimension", "status", "total_estimated_usd", "vm_usd", "node_hours", "vm_cost_incomplete"]
    return pd.DataFrame(rows, columns=cols)


def test_low_node_hours_mark_vm_cost_as_suspect():
    r = ba.mark_cost_flags(runs_frame([
        ("a", "CLASSIC", "shape", "SUCCESS", 1.0, 0.5, 1.0, False),
        ("b", "CLASSIC", "shape", "SUCCESS", 1.0, 0.5, 1.2, False),
        ("dbr_16_4", "CLASSIC", "dbr", "SUCCESS", 0.3, 0.01, 0.1, False),   # missing node_timeline rows
        ("c", "CLASSIC", "shape", "SUCCESS", 1.0, 0.5, 1.1, True),         # cost view says incomplete
        ("d", "CLASSIC", "shape", "SUCCESS", 1.0, 0.5, 1.0, None),
    ]))
    assert r.set_index("variant_name").vm_cost_suspect.to_dict() == {"a": False, "b": False, "dbr_16_4": True, "c": True, "d": False}
    assert r.vm_cost_suspect.dtype == bool


def test_serverless_runs_have_no_vm_cost_and_are_never_suspect():
    r = ba.mark_cost_flags(runs_frame([
        ("a", "CLASSIC", "shape", "SUCCESS", 1.0, 0.5, 1.0, False),
        ("s1", "SERVERLESS", "serverless", "SUCCESS", 0.2, 0.3, NAN, True),
        ("s2", None, "serverless", "SUCCESS", NAN, NAN, NAN, None),          # cost pending
    ])).set_index("variant_name")
    assert r.is_serverless.to_dict() == {"a": False, "s1": True, "s2": True}
    assert r.loc["s1", "vm_usd"] == 0.0 and np.isnan(r.loc["s2", "vm_usd"])
    assert not r.loc[["s1", "s2"], "vm_cost_suspect"].any()


# AI model choice ----------------------------------------------------------------------------------------
@pytest.mark.parametrize("names, expected", [
    ([], ""),
    (["my-endpoint"], "my-endpoint"),
    (["databricks-gpt-5", "databricks-claude-sonnet-4-5", "databricks-claude-sonnet-4-6"], "databricks-claude-sonnet-4-6"),
    (["databricks-meta-llama-3-3", "databricks-claude-3-7"], "databricks-claude-3-7"),
    (["my-endpoint", "databricks-bge-large"], "databricks-bge-large"),
])
def test_choose_ai_model(names, expected):
    assert ba.choose_ai_model(names) == expected
