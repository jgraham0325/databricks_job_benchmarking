# Interpreting results

## Metrics

* **task_only_min** (primary time metric): wall-clock time while at least one task was executing (union of task execution intervals). Excludes cluster startup and queueing, which are noisy and dominated by VM provisioning.
* **total_min**: wall clock of the run, including startup. Use it when the user cares about end-to-end SLA or when comparing serverless (fast or slow startup) to classic.
* **startup_overhead_min** = total - task-only. On classic clusters this is often 5-15 minutes and can dominate short jobs.
* **cost_usd**: estimated DBU + VM per run, including startup (see cost-model.md).
* **\*_vs_base_pct**: percent vs the baseline variant. Negative = faster / cheaper.
* **\*_spread_pct**: (max - min) / median across successful runs of the variant. Only defined with 2+ runs.
* Per-variant values are **medians** of successful runs.

## Noise and confidence

* `noise_pct` (default 5%): differences smaller than this vs baseline are treated as neutral.
* `n_runs = 1` is **screening only**. Do not tell the user to adopt a change based on one run; propose confirmation runs (baseline + 2-3 candidates, `max_runs` 2-3).
* A time difference smaller than the variant's run-to-run spread is "within run-to-run spread" (unconfirmed). A difference larger than the spread is "confirmed": state it as fact.
* `vm_cost_suspect = true`: VM cost understated; never call such a variant the cheapest.
* Runs of the baseline and variants on different days can differ because of platform or data changes. 04 shuffles the order and batches runs to spread time-of-day effects; mention it if the baseline ran much earlier than the variants.
* `code_version` differs between runs: results mix code changes with config changes. 05 warns about it.

## Verdicts (computed in code by 05)

| Verdict | Meaning | Where it belongs in a recommendation |
| --- | --- | --- |
| WIN: faster and cheaper | both beyond noise | Changes to apply (after confirmation if n_runs = 1) |
| COST WIN | cheaper, time neutral | Changes to apply |
| TIME WIN | faster, cost neutral or unranked | Changes to apply if the user values time |
| TRADE-OFF | faster but costlier, or cheaper but slower | Caveats: state what is gained and lost |
| WORSE | slower or costlier, no gain | Not worth changing |
| NEUTRAL | within noise on both | Not worth changing |

Never reclassify a variant from the raw numbers; use the verdict.

## Picks

For each time basis (`task_only`, `total`), excluding cost-suspect variants:

* **fastest**: lowest time. **cheapest**: lowest cost.
* **best_balance**: on the cost-vs-time Pareto front, closest to the origin with time and cost normalised by the baseline.
* **Pareto front**: variants that no other variant beats on both time and cost. Dominated variants have no reason to be chosen.

## Combining winners

The benchmark is one-factor-at-a-time, so effects of combining winners are not measured (they may not add up; e.g. Photon changes the DBU rate, which changes what a shape change saves). Suggest a follow-up benchmark: change the source job (or a copy) to the combined config, run 01-05 with a new `benchmark_id`, and compare against the previous baseline.

## Writing the recommendation for the user

1. One or two sentences: best-balance variant, task-only and cost delta vs baseline, confidence.
2. Concrete changes: cluster key, setting, old -> new, taken only from `overrides_json` (new) and `bench_source_clusters` (old). Skip clusters whose value does not change.
3. What is not worth changing (WORSE / NEUTRAL only).
4. Caveats: screening only, cost suspect, shape capacity mismatch, serverless blockers or dropped Spark confs, list-price estimate.
5. Next experiments: confirmation runs, combined config.

The user applies changes in their own job definition (git / bundle). Do not edit the source job.

## Example: reading rpt_variant_results

A typical classic job: startup 12-20 min, task-only 2-5 min. Serverless performance-optimized may then win on total time and cost because there is no VM startup; serverless standard is usually cheapest but has a 4-6+ min startup. Photon off can be cheaper for workloads that do not benefit from Photon (lower DBU rate), while a newer DBR or VM generation often improves task-only time at the same cost.
