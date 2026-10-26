# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: VM prices and job run cost view
# MAGIC
# MAGIC Run once per workspace (and again when prices change). It does two things:
# MAGIC
# MAGIC 1. Loads Linux pay-as-you-go hourly VM prices for the compute region from the public [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) into `<bench_catalog>.<bench_schema>.azure_vm_retail_prices`.
# MAGIC 2. Creates the view `<bench_catalog>.<bench_schema>.job_run_estimated_cost`: one row per job run with DBU cost (`system.billing.usage` x `system.billing.list_prices`) plus VM cost (`system.compute.node_timeline` node-hours x VM price).
# MAGIC
# MAGIC **Requirements:** Azure workspace; read access to `system.billing`, `system.compute`, `system.lakeflow` and `system.access`; outbound HTTPS to `prices.azure.com` (or pre-load the price table yourself).
# MAGIC
# MAGIC **Spot VM prices are an estimate**: Spot node-hours are priced as pay-as-you-go x `spot_multiplier` (default 0.25). Real Spot prices vary by size, region and time. See `references/cost-model.md`.

# COMMAND ----------

# DBTITLE 1,Configuration
import os, sys
sys.path.insert(0, os.getcwd())
import bench_common as bc

cfg = bc.setup(dbutils, spark)

detected = bc.workspace_region(spark)
dbutils.widgets.text("compute_region", detected or "", "Azure region of the job clusters (armRegionName, e.g. northeurope)")
dbutils.widgets.text("extra_price_regions", "", "Extra regions to load prices for (comma-separated, optional)")
# Spot multiplier: Azure listed Linux Spot at roughly 0.15-0.25x pay-as-you-go for D/E/F series in late 2026;
# 0.25 (75% discount) is deliberately conservative. Set it to your real discount if you know it.
dbutils.widgets.text("spot_multiplier", "0.25", "Spot price multiplier vs pay-as-you-go")
dbutils.widgets.dropdown("refresh_prices", "true", ["true", "false"], "Reload VM prices from the API")

compute_region = dbutils.widgets.get("compute_region").strip().lower()
assert compute_region, ("Set compute_region (could not detect it from this compute). It is the Azure armRegionName "
                        "of the workspace, e.g. northeurope, westeurope, eastus2.")
regions = sorted({compute_region} | {r.strip().lower() for r in dbutils.widgets.get("extra_price_regions").split(",") if r.strip()})
spot_multiplier = float(dbutils.widgets.get("spot_multiplier"))
assert 0 < spot_multiplier <= 1, "spot_multiplier must be in (0, 1]"
refresh_prices = dbutils.widgets.get("refresh_prices") == "true"
price_table, cost_view = cfg.price_table, cfg.cost_view
print(f"Region: {compute_region} (detected: {detected or 'n/a'})  |  price regions: {regions}  |  Spot x{spot_multiplier}")
print(f"Price table: {price_table}\nCost view  : {cost_view}")

# COMMAND ----------

# DBTITLE 1,Load VM prices from the Azure Retail Prices API
import json, time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

API_URL = "https://prices.azure.com/api/retail/prices"
API_VERSION = "2023-01-01-preview"  # 1000 items/page
FIELDS = ["currencyCode", "tierMinimumUnits", "retailPrice", "unitPrice", "armRegionName", "location",
          "effectiveStartDate", "meterId", "meterName", "productId", "skuId", "productName", "skuName",
          "serviceName", "serviceId", "serviceFamily", "unitOfMeasure", "type", "isPrimaryMeterRegion",
          "armSkuName", "reservationTerm", "savingsPlan"]

def _keep(item):
    """Linux pay-as-you-go VM meters only (no Windows, Spot/Low Priority, Dedicated Host, Cloud Services)."""
    product, meter = item.get("productName", ""), item.get("meterName", "")
    return bool(item.get("armSkuName")) and not any(x in product for x in ("Dedicated Host", "DedicatedHost", "Cloud Services", "Windows")) \
        and not any(x in meter for x in ("Spot", "Low Priority"))

def _normalize(item):
    row = {f: item.get(f) for f in FIELDS}
    row["savingsPlan"] = json.dumps(item["savingsPlan"]) if item.get("savingsPlan") else None
    for f in ("tierMinimumUnits", "retailPrice", "unitPrice"):
        row[f] = float(row[f]) if row[f] is not None else None
    return row

if refresh_prices or not spark.catalog.tableExists(price_table):
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=8, backoff_factor=2, allowed_methods=["GET"],
                  status_forcelist=[429, 500, 502, 503, 504], respect_retry_after_header=True)))
    odata = ("serviceName eq 'Virtual Machines' and priceType eq 'Consumption' and ("
             + " or ".join(f"armRegionName eq '{r}'" for r in regions) + ")")
    rows, url, params, t0 = [], API_URL, {"api-version": API_VERSION, "currencyCode": "'USD'", "$filter": odata}, time.time()
    while url:
        resp = session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(_normalize(i) for i in data.get("Items", []) if _keep(i))
        url, params = data.get("NextPageLink"), None  # NextPageLink carries the query
    print(f"Fetched {len(rows):,} price rows in {time.time() - t0:.0f}s")
    assert rows, f"No VM prices returned for {regions}: check the region names (armRegionName)"
    schema = StructType([StructField(f, DoubleType() if f in ("tierMinimumUnits", "retailPrice", "unitPrice")
                                     else BooleanType() if f == "isPrimaryMeterRegion" else StringType(), True)
                         for f in FIELDS])
    (spark.createDataFrame(rows, schema)
     .withColumn("effectiveStartDate", F.to_timestamp("effectiveStartDate"))
     .withColumn("ingested_at", F.current_timestamp())
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(price_table))

n = spark.table(price_table).filter(f"armRegionName = '{compute_region}'").count()
assert n, f"{price_table} has no rows for {compute_region}"
print(f"{price_table}: {n:,} price rows for {compute_region}")

# COMMAND ----------

# DBTITLE 1,Create the job run cost view
spark.sql(f"""
CREATE OR REPLACE VIEW {cost_view}
COMMENT 'Estimated cost per job run: Databricks DBU list cost + Azure VM pay-as-you-go cost from node_timeline ({compute_region}); Spot nodes at {spot_multiplier}x on-demand'
AS
WITH vm_prices AS (            -- one Linux on-demand hourly price per VM size
  SELECT armSkuName AS node_type, retailPrice AS vm_usd_per_hour
  FROM {price_table}
  WHERE armRegionName = '{compute_region}' AND type = 'Consumption'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY armSkuName ORDER BY effectiveStartDate DESC, retailPrice DESC) = 1
),
job_usage AS (
  SELECT u.workspace_id, u.usage_metadata.job_id AS job_id, u.usage_metadata.job_run_id AS run_id,
         u.usage_metadata.cluster_id AS cluster_id, u.sku_name, u.usage_quantity,
         u.identity_metadata.run_as AS run_as,
         u.usage_quantity * p.pricing.`default` AS usage_usd
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
   AND p.price_start_time <= u.usage_start_time
   AND (p.price_end_time >= u.usage_start_time OR p.price_end_time IS NULL)
   AND p.currency_code = 'USD'
  WHERE u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_run_id IS NOT NULL
    AND u.usage_date >= CURRENT_DATE() - INTERVAL 90 DAYS
),
dbu_cost AS (                  -- one row per run
  SELECT workspace_id, job_id, run_id,
         FIRST(run_as, TRUE) AS run_as,
         SUM(usage_quantity) AS dbus,
         SUM(usage_usd)      AS dbu_usd,
         SUM(IF(sku_name LIKE '%SERVERLESS%', usage_usd, 0))     AS serverless_dbu_usd,
         SUM(IF(sku_name NOT LIKE '%SERVERLESS%', usage_usd, 0)) AS classic_dbu_usd,
         MAX(sku_name LIKE '%SERVERLESS%')     AS has_serverless,
         MAX(sku_name NOT LIKE '%SERVERLESS%') AS has_classic
  FROM job_usage
  GROUP BY ALL
),
run_clusters AS (              -- classic job clusters used by each run
  SELECT DISTINCT workspace_id, job_id, run_id, cluster_id
  FROM job_usage WHERE cluster_id IS NOT NULL
),
cluster_share AS (             -- a cluster billed to more than one run: split its VM cost evenly
  SELECT *, COUNT(*) OVER (PARTITION BY workspace_id, cluster_id) AS runs_on_cluster
  FROM run_clusters
),
node_minutes AS (
  SELECT n.workspace_id, n.cluster_id, n.instance_id, n.node_type, n.driver, n.start_time, n.end_time,
         TIMESTAMPDIFF(SECOND, n.start_time, n.end_time) / 3600.0 AS node_hours,
         n.cpu_user_percent + n.cpu_system_percent        AS cpu_pct,
         n.cpu_wait_percent                               AS cpu_wait_pct,
         n.mem_used_percent                               AS mem_pct,
         n.mem_swap_percent                               AS swap_pct,
         100.0 * n.spark_jvm_heap_usage_bytes / NULLIF(n.spark_jvm_heap_limit_bytes, 0) AS jvm_heap_pct,
         COALESCE(n.network_sent_bytes, 0) + COALESCE(n.network_received_bytes, 0) AS network_bytes
  FROM system.compute.node_timeline n
  WHERE n.start_time >= CURRENT_DATE() - INTERVAL 91 DAYS
    AND (n.workspace_id, n.cluster_id) IN (SELECT workspace_id, cluster_id FROM run_clusters)
),
latest_clusters AS (
  SELECT workspace_id, cluster_id, cluster_name, create_time AS cluster_create_time,
         driver_node_type, worker_node_type, worker_count,
         min_autoscale_workers, max_autoscale_workers, dbr_version, data_security_mode, policy_id,
         (driver_instance_pool_id IS NOT NULL OR worker_instance_pool_id IS NOT NULL) AS uses_instance_pool,
         azure_attributes.availability    AS azure_availability,
         azure_attributes.first_on_demand AS first_on_demand
  FROM system.compute.clusters
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1
),
node_first AS (                -- order workers by when they joined the cluster
  SELECT workspace_id, cluster_id, instance_id, driver,
         ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id, driver
                            ORDER BY MIN(start_time), instance_id) AS node_rank
  FROM node_minutes
  GROUP BY workspace_id, cluster_id, instance_id, driver
),
node_spot AS (                 -- Spot clusters: the first `first_on_demand` nodes (driver first) are on-demand, the rest Spot
  SELECT nf.workspace_id, nf.cluster_id, nf.instance_id,
         COALESCE(lc.azure_availability, '') LIKE 'SPOT%'
         AND NOT (
           (nf.driver AND COALESCE(lc.first_on_demand, 1) >= 1)
           OR (NOT nf.driver AND nf.node_rank <= COALESCE(lc.first_on_demand, 1) - 1)
         ) AS is_spot
  FROM node_first nf
  LEFT JOIN latest_clusters lc USING (workspace_id, cluster_id)
),
node_specs AS (
  SELECT node_type, MAX(core_count) AS core_count, MAX(memory_mb) AS memory_mb
  FROM system.compute.node_types GROUP BY node_type
),
cluster_vm AS (                -- autoscaling handled here: every node is costed only for its own uptime
  SELECT m.workspace_id, m.cluster_id,
         SUM(m.node_hours)                                   AS node_hours,
         SUM(m.node_hours * nt.core_count)                   AS vcpu_hours,
         SUM(IF(m.driver, m.node_hours, 0))                  AS driver_hours,
         SUM(IF(m.driver, 0, m.node_hours))                  AS worker_hours,
         SUM(IF(ns.is_spot, m.node_hours, 0))                AS spot_node_hours,
         COUNT(DISTINCT IF(m.driver, NULL, m.instance_id))   AS distinct_workers,
         SUM(m.node_hours * vp.vm_usd_per_hour * IF(ns.is_spot, {spot_multiplier}, 1)) AS vm_usd,
         SUM(m.node_hours * vp.vm_usd_per_hour)              AS vm_usd_on_demand,
         SUM(IF(vp.vm_usd_per_hour IS NULL, m.node_hours, 0)) AS unpriced_node_hours,
         ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(m.node_type)), ', ') AS node_types
  FROM node_minutes m
  LEFT JOIN vm_prices vp USING (node_type)
  LEFT JOIN node_spot ns USING (workspace_id, cluster_id, instance_id)
  LEFT JOIN node_specs nt USING (node_type)
  GROUP BY ALL
),
cluster_util AS (              -- utilisation over all node-minutes
  SELECT workspace_id, cluster_id,
         AVG(cpu_pct)                    AS avg_cpu_pct,
         MAX(cpu_pct)                    AS peak_cpu_pct,
         AVG(IF(driver, cpu_pct, NULL))  AS avg_driver_cpu_pct,
         AVG(IF(driver, NULL, cpu_pct))  AS avg_worker_cpu_pct,
         AVG(cpu_wait_pct)               AS avg_cpu_wait_pct,
         MAX(cpu_wait_pct)               AS max_cpu_wait_pct,
         AVG(mem_pct)                    AS avg_mem_pct,
         MAX(mem_pct)                    AS max_mem_pct,
         MAX(swap_pct)                   AS max_swap_pct,
         MAX(jvm_heap_pct)               AS max_jvm_heap_pct,
         SUM(network_bytes) / 1e9        AS network_gb,
         MIN(start_time)                 AS first_node_start,
         MAX(end_time)                   AS last_node_end
  FROM node_minutes
  GROUP BY ALL
),
cluster_peak AS (              -- peak concurrent workers (how far autoscaling went)
  SELECT workspace_id, cluster_id, MAX(workers) AS peak_workers
  FROM (SELECT workspace_id, cluster_id, start_time, COUNT_IF(NOT driver) AS workers
        FROM node_minutes GROUP BY ALL)
  GROUP BY ALL
),
vm_cost AS (                   -- one row per run
  SELECT s.workspace_id, s.job_id, s.run_id,
         COUNT(*)                                   AS clusters,
         SUM(c.node_hours / s.runs_on_cluster)      AS node_hours,
         SUM(c.vcpu_hours / s.runs_on_cluster)      AS vcpu_hours,
         SUM(c.driver_hours / s.runs_on_cluster)    AS driver_hours,
         SUM(c.worker_hours / s.runs_on_cluster)    AS worker_hours,
         SUM(c.spot_node_hours / s.runs_on_cluster) AS spot_node_hours,
         SUM(c.vm_usd_on_demand / s.runs_on_cluster) AS vm_usd_on_demand,
         MAX(c.distinct_workers)                    AS distinct_workers,
         MAX(pk.peak_workers)                       AS peak_workers,
         SUM(c.vm_usd / s.runs_on_cluster)          AS vm_usd,
         SUM(c.unpriced_node_hours / s.runs_on_cluster) AS unpriced_node_hours,
         ARRAY_JOIN(COLLECT_SET(c.node_types), ', ')    AS node_types,
         ARRAY_JOIN(COLLECT_SET(lc.azure_availability), ', ') AS azure_availability,
         ARRAY_JOIN(COLLECT_SET(s.cluster_id), ', ')          AS cluster_ids,
         ARRAY_JOIN(COLLECT_SET(lc.cluster_name), ', ')       AS cluster_names,
         ARRAY_JOIN(COLLECT_SET(lc.driver_node_type), ', ')   AS driver_node_type,
         ARRAY_JOIN(COLLECT_SET(lc.worker_node_type), ', ')   AS worker_node_type,
         MAX(lc.worker_count)                                 AS worker_count,
         MAX(lc.min_autoscale_workers)                        AS min_autoscale_workers,
         MAX(lc.max_autoscale_workers)                        AS max_autoscale_workers,
         ARRAY_JOIN(COLLECT_SET(lc.dbr_version), ', ')        AS dbr_version,
         ARRAY_JOIN(COLLECT_SET(lc.data_security_mode), ', ') AS data_security_mode,
         ARRAY_JOIN(COLLECT_SET(lc.policy_id), ', ')          AS policy_id,
         MAX(lc.uses_instance_pool)                           AS uses_instance_pool,
         MIN(lc.cluster_create_time)                          AS cluster_create_time,
         MIN(cu.first_node_start)                             AS first_node_start,
         MAX(cu.last_node_end)                                AS last_node_end,
         SUM(cu.avg_cpu_pct * c.node_hours) / NULLIF(SUM(IF(cu.avg_cpu_pct IS NULL, 0, c.node_hours)), 0) AS avg_cpu_pct,
         MAX(cu.peak_cpu_pct)                                 AS peak_cpu_pct,
         SUM(cu.avg_driver_cpu_pct * c.driver_hours) / NULLIF(SUM(IF(cu.avg_driver_cpu_pct IS NULL, 0, c.driver_hours)), 0) AS avg_driver_cpu_pct,
         SUM(cu.avg_worker_cpu_pct * c.worker_hours) / NULLIF(SUM(IF(cu.avg_worker_cpu_pct IS NULL, 0, c.worker_hours)), 0) AS avg_worker_cpu_pct,
         SUM(cu.avg_cpu_wait_pct * c.node_hours) / NULLIF(SUM(IF(cu.avg_cpu_wait_pct IS NULL, 0, c.node_hours)), 0) AS avg_cpu_wait_pct,
         MAX(cu.max_cpu_wait_pct)                             AS max_cpu_wait_pct,
         SUM(cu.avg_mem_pct * c.node_hours) / NULLIF(SUM(IF(cu.avg_mem_pct IS NULL, 0, c.node_hours)), 0) AS avg_mem_pct,
         MAX(cu.max_mem_pct)                                  AS max_mem_pct,
         MAX(cu.max_swap_pct)                                 AS max_swap_pct,
         MAX(cu.max_jvm_heap_pct)                             AS max_jvm_heap_pct,
         SUM(cu.network_gb / s.runs_on_cluster)               AS network_gb
  FROM cluster_share s
  LEFT JOIN cluster_vm c       USING (workspace_id, cluster_id)
  LEFT JOIN cluster_peak pk    USING (workspace_id, cluster_id)
  LEFT JOIN cluster_util cu    USING (workspace_id, cluster_id)
  LEFT JOIN latest_clusters lc USING (workspace_id, cluster_id)
  GROUP BY ALL
),
runs AS (                      -- collapse hourly timeline slices to one row per run
  SELECT workspace_id, job_id, run_id,
         MIN(period_start_time) AS run_start_time,
         MAX(period_end_time)   AS run_end_time,
         MAX_BY(result_state, period_end_time) FILTER (WHERE result_state IS NOT NULL) AS result_state
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= CURRENT_DATE() - INTERVAL 90 DAYS
  GROUP BY ALL
),
latest_jobs AS (
  SELECT workspace_id, job_id, name AS job_name
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
),
workspaces AS (
  SELECT workspace_id, workspace_url FROM system.access.workspaces_latest
)
SELECT r.workspace_id, r.job_id, j.job_name, r.run_id,
       w.workspace_url || '/jobs/' || r.job_id || '/runs/' || r.run_id AS job_run_url,
       d.run_as,
       r.run_start_time, r.run_end_time,
       ROUND(TIMESTAMPDIFF(SECOND, r.run_start_time, r.run_end_time) / 60.0, 1) AS run_minutes,
       r.result_state,
       CASE WHEN d.has_serverless AND d.has_classic THEN 'MIXED'
            WHEN d.has_classic THEN 'CLASSIC' ELSE 'SERVERLESS' END AS compute_type,
       v.cluster_ids, v.cluster_names, v.dbr_version, v.data_security_mode, v.policy_id, v.uses_instance_pool,
       v.driver_node_type, v.worker_node_type, v.node_types, v.azure_availability,
       CASE WHEN v.max_autoscale_workers IS NOT NULL
            THEN 'AUTOSCALE ' || v.min_autoscale_workers || '-' || v.max_autoscale_workers
            WHEN v.worker_count = 0 THEN 'SINGLE NODE'
            WHEN v.worker_count IS NOT NULL THEN 'FIXED ' || v.worker_count END AS cluster_size_config,
       v.peak_workers, v.distinct_workers,
       v.max_autoscale_workers - v.peak_workers AS autoscale_headroom,
       v.cluster_create_time, v.first_node_start, v.last_node_end,
       ROUND(TIMESTAMPDIFF(SECOND, v.cluster_create_time, v.first_node_start) / 60.0, 1) AS cluster_startup_minutes,
       ROUND(v.avg_cpu_pct, 1)        AS avg_cpu_pct,
       ROUND(v.peak_cpu_pct, 1)       AS peak_cpu_pct,
       ROUND(v.avg_driver_cpu_pct, 1) AS avg_driver_cpu_pct,
       ROUND(v.avg_worker_cpu_pct, 1) AS avg_worker_cpu_pct,
       ROUND(v.avg_cpu_wait_pct, 1)   AS avg_cpu_wait_pct,
       ROUND(v.max_cpu_wait_pct, 1)   AS max_cpu_wait_pct,
       ROUND(v.avg_mem_pct, 1)        AS avg_mem_pct,
       ROUND(v.max_mem_pct, 1)        AS max_mem_pct,
       ROUND(v.max_swap_pct, 1)       AS max_swap_pct,
       ROUND(v.max_jvm_heap_pct, 1)   AS max_jvm_heap_pct,
       ROUND(v.network_gb, 3)         AS network_gb,
       CASE WHEN v.node_hours IS NULL THEN NULL
            WHEN v.max_mem_pct >= 90 OR v.max_swap_pct > 0 THEN 'Memory pressure: use larger or memory-optimised nodes'
            WHEN v.avg_cpu_wait_pct >= 10 THEN 'High IO wait: storage or shuffle bound'
            WHEN v.peak_workers > 0 AND v.avg_worker_cpu_pct < 15 AND v.avg_driver_cpu_pct >= 50 THEN 'Driver-bound: workers mostly idle'
            WHEN v.avg_cpu_pct < 20 AND v.max_mem_pct < 50 THEN 'Oversized: low CPU and memory, try fewer or smaller nodes'
            WHEN v.avg_cpu_pct >= 80 THEN 'CPU saturated: more or larger nodes may cut runtime'
            ELSE 'OK' END AS utilization_hint,
       ROUND(v.vcpu_hours, 3)   AS vcpu_hours,
       ROUND(v.node_hours, 3)   AS node_hours,
       ROUND(v.driver_hours, 3) AS driver_hours,
       ROUND(v.worker_hours, 3) AS worker_hours,
       ROUND(v.spot_node_hours, 3) AS spot_node_hours,
       ROUND(d.dbus, 3)         AS dbus,
       ROUND(d.serverless_dbu_usd, 4) AS serverless_dbu_usd,
       ROUND(d.classic_dbu_usd, 4)    AS classic_dbu_usd,
       ROUND(d.dbu_usd, 4)      AS dbu_usd,
       ROUND(COALESCE(v.vm_usd_on_demand, 0), 4) AS vm_usd_if_on_demand,
       ROUND(COALESCE(v.vm_usd, 0), 4) AS vm_usd,
       ROUND(COALESCE(d.dbu_usd, 0) + COALESCE(v.vm_usd, 0), 4) AS total_estimated_usd,
       (d.has_classic AND (v.node_hours IS NULL OR v.unpriced_node_hours > 0)) AS vm_cost_incomplete
FROM runs r
JOIN dbu_cost d USING (workspace_id, job_id, run_id)
LEFT JOIN vm_cost v USING (workspace_id, job_id, run_id)
LEFT JOIN latest_jobs j USING (workspace_id, job_id)
LEFT JOIN workspaces w USING (workspace_id)
""")
print(f"Created {cost_view} ({compute_region} VM prices from {price_table}, Spot x{spot_multiplier})")

# COMMAND ----------

# DBTITLE 1,Check the view
# Recent runs in this workspace, most expensive first. An empty result usually means no system-table access.
display(spark.table(cost_view).orderBy(F.desc("run_start_time")).limit(20))
