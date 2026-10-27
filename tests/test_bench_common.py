"""Shared helpers (bench_common): offline import, configuration widgets and Azure VM node-type rules."""
import enum
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import bench_common as bc

NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"


class FakeWidgets:
    def __init__(self, **values):
        self.values = dict(values)

    def text(self, name, default, label=None):
        self.values.setdefault(name, default)

    def dropdown(self, name, default, choices, label=None):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


def fake_dbutils(**values):
    return SimpleNamespace(widgets=FakeWidgets(**values))


# Offline import / lazy workspace client ----------------------------------------------------------------
def test_import_does_not_create_a_workspace_client():
    code = ("import bench_common as bc, bench_variants, bench_copier, bench_orchestration, bench_analysis; "
            "assert bc._w is None")
    r = subprocess.run([sys.executable, "-c", code], cwd=NOTEBOOKS, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_w_is_created_on_first_use(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(bc, "client", lambda: sentinel)
    assert bc.w is sentinel


def test_unknown_attribute_still_raises():
    with pytest.raises(AttributeError):
        bc.does_not_exist


# Configuration -----------------------------------------------------------------------------------------
def test_setup_requires_a_catalog():
    with pytest.raises(ValueError, match="bench_catalog"):
        bc.setup(fake_dbutils(), None, create_schema=False)


def test_setup_defaults():
    cfg = bc.setup(fake_dbutils(bench_catalog="main"), None, create_schema=False)
    assert (cfg.catalog, cfg.schema, cfg.fqn) == ("main", "job_benchmarking", "main.job_benchmarking")
    assert cfg.output_catalog == "main"  # blank output_catalog falls back to bench_catalog
    assert cfg.prefix == "BENCH"
    assert cfg.price_table == "main.job_benchmarking.azure_vm_retail_prices"
    assert cfg.cost_view == "main.job_benchmarking.job_run_estimated_cost"


def test_setup_uses_widget_values():
    cfg = bc.setup(fake_dbutils(bench_catalog=" main ", bench_schema="jg_benchmarking", output_catalog="sandbox",
                                bench_prefix="PERF"), None, create_schema=False)
    assert (cfg.fqn, cfg.output_catalog, cfg.prefix) == ("main.jg_benchmarking", "sandbox", "PERF")


def test_setup_creates_the_schema():
    sql = []
    bc.setup(fake_dbutils(bench_catalog="main"), SimpleNamespace(sql=sql.append))
    assert sql == ["CREATE SCHEMA IF NOT EXISTS main.job_benchmarking"]


@pytest.mark.parametrize("widget,value", [("bench_catalog", "main; DROP"), ("bench_schema", "my-schema"),
                                          ("output_catalog", "a.b"), ("bench_prefix", "BENCH TEST")])
def test_setup_rejects_non_identifiers(widget, value):
    values = dict(bench_catalog="main")
    values[widget] = value
    with pytest.raises(ValueError, match=widget):
        bc.setup(fake_dbutils(**values), None, create_schema=False)


def test_benchmark_id_is_lowercased():
    assert bc.benchmark_id_widget(fake_dbutils(benchmark_id=" BF1E865E ")) == "bf1e865e"


def test_benchmark_id_required():
    with pytest.raises(ValueError, match="benchmark_id"):
        bc.benchmark_id_widget(fake_dbutils())
    assert bc.benchmark_id_widget(fake_dbutils(), required=False) == ""


def test_benchmark_id_must_be_a_safe_name():
    with pytest.raises(ValueError, match="lowercase"):
        bc.benchmark_id_widget(fake_dbutils(benchmark_id="bf1e-865e"))


def test_new_benchmark_ids_are_short_and_unique():
    ids = {bc.new_benchmark_id() for _ in range(100)}
    assert len(ids) == 100
    assert all(re.fullmatch(r"[0-9a-f]{8}", i) for i in ids)


def test_workspace_region_from_spark_conf():
    def get(key):
        if key.endswith("dataPlaneRegion"):
            raise Exception("not set")
        return "North Europe"
    assert bc.workspace_region(SimpleNamespace(conf=SimpleNamespace(get=get))) == "northeurope"


def test_workspace_region_unknown():
    def get(key):
        raise Exception("not set")
    assert bc.workspace_region(SimpleNamespace(conf=SimpleNamespace(get=get))) is None


# Node catalog ------------------------------------------------------------------------------------------
@pytest.fixture
def empty_catalog():
    saved = dict(bc._catalog)
    bc.set_node_catalog({})
    yield
    bc.set_node_catalog(saved)


def test_node_catalog_is_loaded_from_the_workspace_once(monkeypatch, empty_catalog):
    calls = []
    nts = [SimpleNamespace(node_type_id="Standard_D4ds_v5", num_cores=4.0, memory_mb=16384, num_gpus=None,
                           is_deprecated=None),
           SimpleNamespace(node_type_id="Standard_D4ds_v4", num_cores=4.0, memory_mb=16384, num_gpus=0,
                           is_deprecated=True)]

    def list_node_types():
        calls.append(1)
        return SimpleNamespace(node_types=nts)
    monkeypatch.setattr(bc, "client", lambda: SimpleNamespace(clusters=SimpleNamespace(list_node_types=list_node_types)))
    cat = bc.load_node_catalog()
    assert cat["Standard_D4ds_v5"] == dict(cores=4, mem_mb=16384, gpus=0, deprecated=False)
    assert cat["Standard_D4ds_v4"]["deprecated"] is True
    bc.load_node_catalog()
    assert len(calls) == 1


def test_injected_catalog_needs_no_workspace(monkeypatch, node_catalog):
    monkeypatch.setattr(bc, "client", lambda: pytest.fail("workspace client should not be used"))
    assert bc.load_node_catalog() == node_catalog


def test_valid_node(node_catalog):
    assert bc.valid_node("Standard_E8ds_v5") == "Standard_E8ds_v5"
    assert bc.valid_node("Standard_D4ds_v4") is None  # deprecated
    assert bc.valid_node("Standard_E64ds_v5") is None  # not in the workspace


def test_cores_and_memory(node_catalog):
    assert bc.cores_of("Standard_D4ds_v5") == 4
    assert bc.mem_gb_of("Standard_D4ds_v5") == 16.0
    assert bc.cores_of("Standard_X1_v1") is None
    assert bc.mem_gb_of("Standard_X1_v1") is None


# Azure VM names ----------------------------------------------------------------------------------------
@pytest.mark.parametrize("name,expected", [
    ("Standard_E8ds_v5", dict(fam="E", n=8, feat="ds", acc=None, gen=5, has_cache=True, is_amd=False)),
    ("Standard_D4s_v5", dict(fam="D", n=4, feat="s", acc=None, gen=5, has_cache=False, is_amd=False)),
    ("Standard_D16ads_v5", dict(fam="D", n=16, feat="ads", acc=None, gen=5, has_cache=True, is_amd=True)),
    ("Standard_NC24ads_A100_v4", dict(fam="NC", n=24, feat="ads", acc="A100", gen=4, has_cache=True, is_amd=True)),
])
def test_parse_vm(name, expected):
    assert bc.parse_vm(name) == expected


@pytest.mark.parametrize("name", [None, "", "m5.xlarge", "Standard_E8ds", "standard_e8ds_v5"])
def test_parse_vm_rejects_other_names(name):
    assert bc.parse_vm(name) is None


@pytest.mark.parametrize("name", ["Standard_E8ds_v5", "Standard_D16ads_v5", "Standard_NC24ads_A100_v4"])
def test_build_vm_round_trips(name):
    p = bc.parse_vm(name)
    assert bc._build_vm(p["fam"], p["n"], p["feat"], p["acc"], p["gen"]) == name


@pytest.mark.parametrize("feat,flag,on,expected", [
    ("s", "d", True, "ds"), ("ds", "d", False, "s"), ("ads", "d", False, "as"),
    ("as", "d", True, "ads"), ("ds", "d", True, "ds"), ("", "d", False, ""),
])
def test_feature_flags_keep_canonical_order(feat, flag, on, expected):
    assert bc._set_feat(feat, flag, on) == expected


@pytest.mark.parametrize("name,factor,expected", [
    ("Standard_E8ds_v5", 2.0, "Standard_E16ds_v5"),
    ("Standard_E8ds_v5", 0.5, "Standard_E4ds_v5"),
    ("Standard_D4ds_v5", 2.0, "Standard_D8ds_v5"),
    ("Standard_E16ds_v5", 2.0, None),   # E32ds_v5 not in the workspace
    ("Standard_D4ds_v5", 0.5, None),    # D2ds_v5 not in the workspace
    ("not_a_vm", 2.0, None),
])
def test_scale_vm(node_catalog, name, factor, expected):
    assert bc.scale_vm(name, factor) == expected


@pytest.mark.parametrize("name,gen,expected", [
    ("Standard_E8ds_v5", 4, "Standard_E8ds_v4"),
    ("Standard_E8ds_v5", 6, "Standard_E8ds_v6"),
    ("Standard_D4ds_v5", 4, None),      # exists but deprecated
    ("Standard_E8ds_v5", 7, None),
    ("not_a_vm", 5, None),
])
def test_change_gen(node_catalog, name, gen, expected):
    assert bc.change_gen(name, gen) == expected


@pytest.mark.parametrize("name,enable,expected", [
    ("Standard_E8ds_v5", False, "Standard_E8s_v5"),
    ("Standard_E8s_v5", True, "Standard_E8ds_v5"),
    ("Standard_D4ds_v5", False, "Standard_D4s_v5"),
    ("Standard_E8ds_v4", False, None),   # E8s_v4 not in the workspace
])
def test_toggle_cache(node_catalog, name, enable, expected):
    assert bc.toggle_cache(name, enable) == expected


@pytest.mark.parametrize("n,old,new,expected", [
    (4, 8, 16, (2, True)),
    (2, 8, 4, (4, True)),
    (3, 4, 8, (2, False)),   # 12 cores -> 16
    (1, 4, 8, (1, False)),   # never below 1 worker
    (5, 8, 16, (3, False)),  # 2.5 rounds half up
])
def test_scale_workers_keeps_total_cores(n, old, new, expected):
    assert bc.scale_workers(n, old, new) == expected


def test_ev_turns_sdk_enums_into_strings():
    class State(enum.Enum):
        DONE = "TERMINATED"
    assert bc.ev(State.DONE) == "TERMINATED"
    assert bc.ev("SUCCESS") == "SUCCESS"
    assert bc.ev(None) is None
