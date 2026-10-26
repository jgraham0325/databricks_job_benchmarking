"""Shared configuration and helpers for the job benchmarking notebooks.

Every notebook in this folder starts with:

    import os, sys; sys.path.insert(0, os.getcwd())
    import bench_common as bc
    cfg = bc.setup(dbutils, spark)

Nothing here is tied to one workspace. The catalog, schema, output catalog and job-name prefix come
from widgets (or job parameters with the same names), and Azure node types come from the workspace
itself via the Clusters API.
"""
import json
import math
import re
import uuid
from types import SimpleNamespace

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


def _ident(value, label):
    """Catalog/schema/prefix values are put into SQL and job names, so only plain identifiers are allowed."""
    if not _IDENT.match(value or ""):
        raise ValueError(f"{label} must contain only letters, digits and underscores, got {value!r}")
    return value


# ---------------------------------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------------------------------
def setup(dbutils, spark, create_schema=True):
    """Create the shared widgets and return the configuration.

    bench_catalog   catalog for the benchmark state tables (required, no default on purpose)
    bench_schema    schema for the state tables, price table, cost view and reporting views
    output_catalog  catalog where per-variant output schemas are created (blank = bench_catalog)
    bench_prefix    benchmark jobs are named "[<prefix>-<benchmark_id>] <source job> | <variant>"
    """
    dbutils.widgets.text("bench_catalog", "", "Benchmark catalog (required)")
    dbutils.widgets.text("bench_schema", "job_benchmarking", "Benchmark schema")
    dbutils.widgets.text("output_catalog", "", "Output catalog for variant schemas (blank = bench_catalog)")
    dbutils.widgets.text("bench_prefix", "BENCH", "Benchmark job name prefix")

    catalog = dbutils.widgets.get("bench_catalog").strip()
    if not catalog:
        raise ValueError("Set the bench_catalog widget to a catalog where you can create schemas and tables")
    schema = dbutils.widgets.get("bench_schema").strip() or "job_benchmarking"
    out_catalog = dbutils.widgets.get("output_catalog").strip() or catalog
    prefix = dbutils.widgets.get("bench_prefix").strip() or "BENCH"
    for v, label in [(catalog, "bench_catalog"), (schema, "bench_schema"),
                     (out_catalog, "output_catalog"), (prefix, "bench_prefix")]:
        _ident(v, label)

    fqn = f"{catalog}.{schema}"
    if create_schema:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
    cfg = SimpleNamespace(
        catalog=catalog, schema=schema, fqn=fqn, output_catalog=out_catalog, prefix=prefix,
        price_table=f"{fqn}.azure_vm_retail_prices",
        cost_view=f"{fqn}.job_run_estimated_cost",
    )
    print(f"Benchmark tables: {fqn}  |  variant outputs in catalog: {out_catalog}  |  "
          f"job names: [{prefix}-<benchmark_id>] ...")
    return cfg


def benchmark_id_widget(dbutils, required=True, label="Benchmark ID (from 01_discovery)"):
    """benchmark_id widget. It becomes part of schema and job names, so it must be lowercase [a-z0-9_]."""
    dbutils.widgets.text("benchmark_id", "", label)
    bid = dbutils.widgets.get("benchmark_id").strip().lower()
    if required and not bid:
        raise ValueError("Set the benchmark_id widget (01_discovery prints it)")
    if bid and not re.match(r"^[a-z0-9_]+$", bid):
        raise ValueError(f"benchmark_id must be lowercase letters, digits or _, got {bid!r}")
    return bid


def new_benchmark_id():
    return uuid.uuid4().hex[:8]


def workspace_region(spark):
    """Azure region (armRegionName) of the compute this notebook runs on, or None if unknown."""
    for key in ("spark.databricks.clusterUsageTags.dataPlaneRegion", "spark.databricks.clusterUsageTags.region"):
        try:
            v = spark.conf.get(key)
            if v:
                return v.strip().lower().replace(" ", "")
        except Exception:
            pass
    return None


def replace_benchmark_rows(spark, df, table, benchmark_id):
    """Replace only this benchmark's rows in a state table; rows of other benchmarks are kept."""
    if not spark.catalog.tableExists(table):
        df.write.saveAsTable(table)
        return
    spark.sql(f"DELETE FROM {table} WHERE benchmark_id = '{benchmark_id}'")
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table)


# ---------------------------------------------------------------------------------------------------
# Azure VM node types
#   Standard_{Family}{N}{Features}[_{Accelerator}]_v{Gen}
#   Family:   D(general) E(memory) F(compute) L(storage) M(large-mem) N/NC/ND/NV(GPU)
#   Features: a=AMD  d=local disk (delta cache)  s=premium storage  i=isolated  l=low memory  p=ARM
#   N:        vCPU count (modern series)
# ---------------------------------------------------------------------------------------------------
_VM_RE = re.compile(r"^Standard_(?P<fam>[A-Z]+?)(?P<n>\d+)(?P<feat>[a-z]*)(?:_(?P<acc>[A-Z]\w+))?_v(?P<gen>\d+)$")


def parse_vm(name):
    """Parse an Azure VM name -> dict(fam, n, feat, acc, gen, has_cache, is_amd), or None."""
    m = _VM_RE.match(name or "")
    if not m:
        return None
    g = m.groupdict()
    return dict(fam=g["fam"], n=int(g["n"]), feat=g["feat"], acc=g.get("acc"), gen=int(g["gen"]),
                has_cache="d" in g["feat"], is_amd="a" in g["feat"])


def _build_vm(fam, n, feat, acc, gen):
    s = f"Standard_{fam}{n}{feat}"
    return f"{s}_{acc}_v{gen}" if acc else f"{s}_v{gen}"


def _set_feat(feat, flag, on):
    """Toggle a feature flag, keeping the canonical order (a d i l p s)."""
    fs = set(feat)
    fs.add(flag) if on else fs.discard(flag)
    order = "adilps"
    return "".join(c for c in order if c in fs) + "".join(sorted(c for c in fs if c not in order))


_catalog = {}


def load_node_catalog():
    """Node types available in this workspace (cached)."""
    if _catalog:
        return _catalog
    for nt in w.clusters.list_node_types().node_types:
        _catalog[nt.node_type_id] = dict(cores=int(nt.num_cores or 0), mem_mb=nt.memory_mb or 0,
                                         gpus=nt.num_gpus or 0, deprecated=nt.is_deprecated or False)
    active = sum(1 for v in _catalog.values() if not v["deprecated"])
    print(f"Node catalog: {len(_catalog)} types ({active} active)")
    return _catalog


def valid_node(name):
    """name if it exists in this workspace and is not deprecated, else None."""
    e = load_node_catalog().get(name)
    return name if e and not e["deprecated"] else None


def cores_of(name):
    e = load_node_catalog().get(name)
    return e["cores"] if e else None


def mem_gb_of(name):
    e = load_node_catalog().get(name)
    return round(e["mem_mb"] / 1024, 1) if e else None


def scale_vm(name, factor):
    """Same family/features/generation with the vCPU count scaled by factor, or None if unavailable."""
    p = parse_vm(name)
    if not p:
        return None
    return valid_node(_build_vm(p["fam"], max(2, round(p["n"] * factor)), p["feat"], p["acc"], p["gen"]))


def change_gen(name, gen):
    """Same VM in another generation, or None."""
    p = parse_vm(name)
    if not p:
        return None
    return valid_node(_build_vm(p["fam"], p["n"], p["feat"], p["acc"], gen))


def toggle_cache(name, enable):
    """Same VM with or without local disk (delta cache), or None."""
    p = parse_vm(name)
    if not p:
        return None
    return valid_node(_build_vm(p["fam"], p["n"], _set_feat(p["feat"], "d", enable), p["acc"], p["gen"]))


def scale_workers(n, old_cores, new_cores):
    """Worker count that keeps total worker cores equal (at least 1). Returns (count, exact_match)."""
    k = max(1, math.floor(n * old_cores / new_cores + 0.5))
    return k, k * new_cores == n * old_cores


def ev(x):
    """Enum value or str (SDK enums -> plain strings)."""
    return None if x is None else getattr(x, "value", str(x))
