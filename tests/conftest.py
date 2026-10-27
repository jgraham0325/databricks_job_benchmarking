"""Shared fixtures. The modules under test live in ../notebooks and import without Spark or a workspace."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "notebooks"))  # also set in pytest.ini; kept for runners that ignore it

import bench_common as bc  # noqa: E402


def node(cores, mem_gb=None, deprecated=False):
    return dict(cores=cores, mem_mb=(mem_gb or cores * 8) * 1024, gpus=0, deprecated=deprecated)


# A small, hand-picked workspace node catalog. Anything not listed does not exist in the "workspace".
SMALL_CATALOG = {
    "Standard_E4ds_v5": node(4), "Standard_E8ds_v5": node(8), "Standard_E16ds_v5": node(16),
    "Standard_E8s_v5": node(8), "Standard_E16s_v5": node(16),
    "Standard_E8ds_v4": node(8), "Standard_E8ds_v6": node(8),
    "Standard_D4ds_v5": node(4, 16), "Standard_D8ds_v5": node(8, 32),
    "Standard_D4s_v5": node(4, 16), "Standard_D8s_v5": node(8, 32),
    "Standard_D4ds_v4": node(4, 16, deprecated=True),
}


@pytest.fixture
def node_catalog():
    """Install SMALL_CATALOG as the workspace node catalog for one test."""
    saved = dict(bc._catalog)
    bc.set_node_catalog(SMALL_CATALOG)
    yield SMALL_CATALOG
    bc.set_node_catalog(saved)


@pytest.fixture
def bf1e865e():
    """Recorded inputs and expected variants of benchmark bf1e865e, with its node catalog installed."""
    fx = json.loads((ROOT / "tests" / "fixtures" / "bf1e865e.json").read_text())
    saved = dict(bc._catalog)
    bc.set_node_catalog(fx["node_catalog"])
    yield fx
    bc.set_node_catalog(saved)


def cluster(key="c1", worker="Standard_E8ds_v5", driver=None, num_workers=4, min_workers=None, max_workers=None,
            spark_version="15.4.x-scala2.12", runtime_engine="STANDARD", availability="ON_DEMAND_AZURE",
            first_on_demand=1, spark_conf=None, init_scripts=None):
    """A bench_source_clusters row. Pass min_workers/max_workers for an autoscaling cluster."""
    if min_workers is not None:
        num_workers = None
    return dict(job_cluster_key=key, worker_node_type=worker, driver_node_type=driver or worker,
                num_workers=float(num_workers) if num_workers is not None else None,
                min_workers=float(min_workers) if min_workers is not None else None,
                max_workers=float(max_workers) if max_workers is not None else None,
                spark_version=spark_version, runtime_engine=runtime_engine, azure_availability=availability,
                first_on_demand=first_on_demand, spark_conf_json=json.dumps(spark_conf or {}),
                init_scripts_json=json.dumps(init_scripts or []))
