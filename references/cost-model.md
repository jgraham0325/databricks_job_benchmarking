# Cost model

Cost per run comes from the view `<bench_catalog>.<bench_schema>.job_run_estimated_cost`, created by `00_setup_cost_model`. It is an **estimate at list prices**, good for comparing variants with each other; it is not an invoice.

## Components

`total_estimated_usd = dbu_usd + vm_usd`

* **DBU cost**: `system.billing.usage` (rows with `billing_origin_product = 'JOBS'` and a `job_run_id`) x `system.billing.list_prices` (USD, price valid at usage time). Serverless runs have only DBU cost (`serverless_dbu_usd`); VM cost is included in the serverless DBU price.
* **VM cost (classic)**: for every node in `system.compute.node_timeline` of the run's clusters, node-hours x Linux pay-as-you-go hourly price for that VM size in `compute_region`, from the Azure Retail Prices API (table `azure_vm_retail_prices`). Each node is costed only for its own uptime, so autoscaling is handled. Cluster startup is included (the VMs are billed while starting).
* **Spot**: on Spot clusters the first `first_on_demand` nodes (driver first) are on-demand, the rest are Spot and priced at pay-as-you-go x `spot_multiplier` (default 0.25). Real Spot prices vary by size, region and time, and evictions are not modelled. Set the multiplier from the customer's actual bills if known.
* A cluster billed to several runs has its VM cost split evenly between them (does not happen for benchmark jobs, which use job clusters).

The view also exposes utilisation (avg/peak CPU, worker CPU, memory, swap, JVM heap, network) and a `utilization_hint`, plus `vm_cost_incomplete` and `unpriced_node_hours` (VM sizes without a price row).

## What it does not include

* Negotiated discounts, reservations, savings plans, Azure Hybrid Benefit.
* Storage, network egress, ADLS transactions, managed disks beyond the VM price.
* Non-USD currencies (prices are loaded in USD).

Because every variant gets the same treatment, the relative ranking is robust even when absolute numbers differ from the bill. Say so when presenting results.

## System table lag

`system.billing.usage` and `system.compute.node_timeline` typically lag a few hours. `05_analysis` prints `cost pending` for successful runs without cost: re-run it later. The dashboard and Genie agent read the views, so they update on the next 05 run.

## Known gaps and checks

* **Missing node_timeline rows**: occasionally a cluster has DBUs in `billing.usage` but no (or very few) `node_timeline` rows. VM cost and utilisation are then understated and the variant looks cheaper than it is. 05 flags runs with node-hours below 40% of the classic median as `vm_cost_suspect` and excludes such variants from cost picks. If it persists for a cluster ID, raise a support ticket; do not "fix" the data.
* **Region**: VM prices are for `compute_region` (auto-detected from the notebook compute). Run 00 in the workspace where the benchmark runs. If the clusters run in another region, set `compute_region` explicitly.
* **Price refresh**: re-run 00 with `refresh_prices = true` occasionally; the view always uses the latest price per VM size.
* **Lookback**: the view reads the last 90 days. Benchmarks older than that lose their cost (timings in `bench_runs` remain).

## Cross-checking a number

To verify one run, sum `usage_quantity` for its `usage_metadata.job_run_id` in `system.billing.usage` (DBUs), and compare node-hours from `system.compute.node_timeline` for its cluster IDs (from `bench_run_tasks.cluster_id`) with `rpt_runs.node_hours`.
