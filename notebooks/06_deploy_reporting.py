# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Deploy reporting (dashboard + Genie agent)
# MAGIC
# MAGIC Creates (or updates) the **Job Benchmark Advisor** AI/BI dashboard and Genie agent from `../templates/`, pointed at the `rpt_*` views in your benchmarking schema. Run once after the first `05_analysis`; re-run to push template changes. Asset IDs are stored in `bench_reporting_assets`, so re-runs update in place instead of creating duplicates.


# COMMAND ----------

# DBTITLE 1,Configuration
import os, sys, json
sys.path.insert(0, os.getcwd())
import bench_common as bc
from databricks.sdk.service.sql import State

cfg = bc.setup(dbutils, spark)
w = bc.w
me = w.current_user.me().user_name
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID (blank = first serverless warehouse)")
dbutils.widgets.text("parent_path", f"/Users/{me}/job_benchmarking", "Workspace folder for the dashboard and Genie agent")
dbutils.widgets.text("asset_name", "Job Benchmark Advisor", "Dashboard / Genie agent name")
dbutils.widgets.dropdown("embed_credentials", "true", ["true", "false"], "Publish with embedded credentials")
dbutils.widgets.text("default_benchmark_id", "", "Default Benchmark filter value (blank = latest)")
parent_path = dbutils.widgets.get("parent_path").rstrip("/")
asset_name = dbutils.widgets.get("asset_name").strip()
embed = dbutils.widgets.get("embed_credentials") == "true"

warehouse_id = dbutils.widgets.get("warehouse_id").strip()
if not warehouse_id:
    whs = list(w.warehouses.list())
    pick = [x for x in whs if x.enable_serverless_compute] or whs
    assert pick, "No SQL warehouse found: create one or set warehouse_id"
    pick.sort(key=lambda x: x.state != State.RUNNING)
    warehouse_id = pick[0].id
    print(f"Using warehouse {pick[0].name} ({warehouse_id})")

for v in ["rpt_variant_results", "rpt_runs", "rpt_recommendations"]:
    assert spark.catalog.tableExists(f"{cfg.fqn}.{v}"), f"{cfg.fqn}.{v} missing: run 05_analysis first"

# Default value of the dashboard's Benchmark filter (single-select, no "All"), so the dashboard never loads empty.
# Blank = the most recently analysed benchmark. Every re-run of this notebook resets the default to the latest.
default_benchmark_id = dbutils.widgets.get("default_benchmark_id").strip().lower()
if not default_benchmark_id:
    _latest = spark.sql(f"""SELECT benchmark_id FROM {cfg.fqn}.rpt_variant_results
                            WHERE benchmark_id IS NOT NULL
                            GROUP BY benchmark_id
                            ORDER BY MAX(COALESCE(analysed_at, last_run_at)) DESC NULLS LAST
                            LIMIT 1""").collect()
    assert _latest, f"{cfg.fqn}.rpt_variant_results is empty: run 05_analysis first"
    default_benchmark_id = _latest[0].benchmark_id
print(f"Default Benchmark filter: {default_benchmark_id}")

TEMPLATES = os.path.normpath(os.path.join(os.getcwd(), "..", "templates"))
def _template(name, **tokens):
    txt = open(os.path.join(TEMPLATES, name)).read()
    for k, v in tokens.items():
        txt = txt.replace(f"__{k}__", v)
    assert not any(t in txt for t in ("__BENCH_FQN__", "__GENIE_SPACE_ID__", "__DEFAULT_BENCHMARK_ID__")), \
        f"Unresolved token in {name}"
    return txt

w.workspace.mkdirs(parent_path)
ASSETS_TBL = f"{cfg.fqn}.bench_reporting_assets"
spark.sql(f"CREATE TABLE IF NOT EXISTS {ASSETS_TBL} (asset_type STRING, asset_name STRING, asset_id STRING, updated_at TIMESTAMP)")
known = {(r.asset_type, r.asset_name): r.asset_id for r in spark.table(ASSETS_TBL).collect()}

def _save(kind, aid):
    spark.sql(f"""MERGE INTO {ASSETS_TBL} t USING (SELECT :k AS asset_type, :n AS asset_name, :i AS asset_id) s
                  ON t.asset_type = s.asset_type AND t.asset_name = s.asset_name
                  WHEN MATCHED THEN UPDATE SET asset_id = s.asset_id, updated_at = current_timestamp()
                  WHEN NOT MATCHED THEN INSERT (asset_type, asset_name, asset_id, updated_at)
                  VALUES (s.asset_type, s.asset_name, s.asset_id, current_timestamp())""",
              args={"k": kind, "n": asset_name, "i": aid})

def _exists(path):
    try:
        w.api_client.do("GET", path)
        return True
    except Exception:
        return False

# COMMAND ----------

# DBTITLE 1,Genie agent
g = json.loads(_template("genie_template.json", BENCH_FQN=cfg.fqn))
body = {"title": asset_name, "description": g["description"], "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(g["serialized_space"])}
space_id = known.get(("genie", asset_name))
if space_id and _exists(f"/api/2.0/genie/spaces/{space_id}"):
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print(f"Updated Genie agent {space_id}")
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body={**body, "parent_path": parent_path})["space_id"]
    print(f"Created Genie agent {space_id}")
_save("genie", space_id)

# COMMAND ----------

# DBTITLE 1,Dashboard (create or update, then publish)
serialized = _template("dashboard_template.json", BENCH_FQN=cfg.fqn, GENIE_SPACE_ID=space_id,
                       DEFAULT_BENCHMARK_ID=default_benchmark_id)
dash_id = known.get(("dashboard", asset_name))
if dash_id and _exists(f"/api/2.0/lakeview/dashboards/{dash_id}"):
    w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{dash_id}",
                    body={"display_name": asset_name, "serialized_dashboard": serialized, "warehouse_id": warehouse_id})
    print(f"Updated dashboard {dash_id}")
else:
    dash_id = w.api_client.do("POST", "/api/2.0/lakeview/dashboards",
                              body={"display_name": asset_name, "parent_path": parent_path,
                                    "serialized_dashboard": serialized, "warehouse_id": warehouse_id})["dashboard_id"]
    print(f"Created dashboard {dash_id}")
_save("dashboard", dash_id)
w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{dash_id}/published",
                body={"embed_credentials": embed, "warehouse_id": warehouse_id})
print(f"Published dashboard (embed_credentials={embed})")

# COMMAND ----------

# DBTITLE 1,Links
host = w.config.host.rstrip("/")
displayHTML(f"""<p>Dashboard: <a href="{host}/dashboardsv3/{dash_id}/published" target="_blank">{asset_name}</a><br>
Genie agent: <a href="{host}/genie/rooms/{space_id}" target="_blank">{asset_name}</a><br>
Data: <code>{cfg.fqn}.rpt_*</code> on warehouse <code>{warehouse_id}</code></p>
<p>Share both with your users (they need SELECT on the rpt_* views, or use embedded credentials for the dashboard).</p>""")
