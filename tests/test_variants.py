"""Variant rules (02_variant_generator): one factor at a time, invalid variants explained, serverless plan."""
import json

import pytest

import bench_variants as bv
from conftest import cluster


def generate(clusters, **kw):
    kw.setdefault("benchmark_id", "b1")
    return bv.generate_variants("123", clusters, **kw)


def by_name(variants):
    return {v["variant_name"]: v for v in variants}


def overrides(v):
    return json.loads(v["overrides_json"])


def notebook_job(*task_keys, cluster_key="c1"):
    return {"tasks": [{"task_key": k, "job_cluster_key": cluster_key, "notebook_task": {"notebook_path": f"/{k}"}}
                      for k in task_keys]}


def serverless(settings, clusters=None, perf="STANDARD", **kw):
    return bv.serverless_variant(settings, clusters or [cluster()], "serverless_standard", perf, **kw)


# Golden: benchmark bf1e865e -----------------------------------------------------------------------------
def test_bf1e865e_variants_are_reproduced_exactly(bf1e865e):
    got = bv.generate_variants(bf1e865e["job_id"], bf1e865e["clusters"], benchmark_id=bf1e865e["benchmark_id"],
                               vm_generations=[4, 5, 6], dbr_candidates=["15.4.x-scala2.12", "16.4.x-scala2.12"],
                               include_serverless=True, serverless_env_version="4",
                               job_settings=bf1e865e["job_settings"])
    assert [(v["variant_name"], v["dimension"], v["is_valid"], overrides(v)) for v in got] == \
        [(e["variant_name"], e["dimension"], e["is_valid"], e["overrides"]) for e in bf1e865e["expected_variants"]]


# Common structure --------------------------------------------------------------------------------------
def test_every_variant_has_ids_and_serialised_overrides(node_catalog):
    vs = generate([cluster()])
    for v in vs:
        assert v["benchmark_id"] == "b1"
        assert v["source_job_id"] == "123"
        assert v["variant_id"] == f"b1_{v['variant_name']}"
        assert isinstance(json.loads(v["overrides_json"]), dict)
        assert "overrides" not in v
    assert len({v["variant_name"] for v in vs}) == len(vs)


def test_baseline_comes_first_and_changes_nothing(node_catalog):
    v = generate([cluster()])[0]
    assert (v["variant_name"], v["dimension"], v["is_valid"], overrides(v)) == ("baseline", "baseline", True, {})


def test_each_variant_changes_one_dimension(node_catalog):
    for v in generate([cluster()]):
        changed = {k for o in overrides(v).values() for k in o}
        allowed = {"shape": {"worker_node_type", "num_workers", "min_workers", "max_workers"},
                   "vm_gen": {"worker_node_type", "driver_node_type"},
                   "cache": {"worker_node_type", "driver_node_type"},
                   "availability": {"azure_availability", "first_on_demand"},
                   "photon": {"runtime_engine"}, "dbr": {"spark_version"}, "shuffle": {"spark_conf_add"},
                   "autoscale": {"num_workers"}, "baseline": set()}[v["dimension"]]
        assert changed <= allowed, v["variant_name"]


# Worker shape ------------------------------------------------------------------------------------------
def test_shape_keeps_total_worker_cores_for_fixed_clusters(node_catalog):
    vs = by_name(generate([cluster(worker="Standard_E8ds_v5", num_workers=4)]))
    assert overrides(vs["half_workers_2x_size"]) == {"c1": {"worker_node_type": "Standard_E16ds_v5", "num_workers": 2}}
    assert overrides(vs["2x_workers_half_size"]) == {"c1": {"worker_node_type": "Standard_E4ds_v5", "num_workers": 8}}
    assert "differs from baseline" not in vs["half_workers_2x_size"]["description"]


def test_shape_scales_autoscale_bounds_and_notes_inexact_ones(node_catalog):
    v = by_name(generate([cluster(worker="Standard_D4ds_v5", min_workers=1, max_workers=3)]))["half_workers_2x_size"]
    assert overrides(v) == {"c1": {"worker_node_type": "Standard_D8ds_v5", "min_workers": 1, "max_workers": 2}}
    assert "c1 min 1x4c -> 1x8c" in v["description"]
    assert "c1 max 3x4c -> 2x8c" in v["description"]


def test_shape_never_changes_the_driver(node_catalog):
    vs = by_name(generate([cluster(worker="Standard_E8ds_v5", driver="Standard_E16ds_v5")]))
    for name in ("half_workers_2x_size", "2x_workers_half_size"):
        assert "driver_node_type" not in overrides(vs[name])["c1"]


def test_shape_is_invalid_when_the_node_type_does_not_exist(node_catalog):
    v = by_name(generate([cluster(key="big", worker="Standard_E16ds_v5")]))["half_workers_2x_size"]
    assert v["is_valid"] is False
    assert v["invalid_reason"] == "No matching node type for big (Standard_E16ds_v5)"
    assert overrides(v) == {}


def test_shape_is_invalid_if_any_cluster_cannot_be_scaled(node_catalog):
    v = by_name(generate([cluster(key="ok"), cluster(key="big", worker="Standard_E16ds_v5")]))["half_workers_2x_size"]
    assert v["is_valid"] is False and "big" in v["invalid_reason"]


# Autoscaling -------------------------------------------------------------------------------------------
def test_autoscale_off_fixes_autoscaling_clusters_at_max(node_catalog):
    vs = by_name(generate([cluster(key="fixed"), cluster(key="auto", min_workers=2, max_workers=6)]))
    v = vs["autoscale_off"]
    assert overrides(v) == {"auto": {"num_workers": 6}}
    assert "auto 2-6 -> 6" in v["description"]


def test_no_autoscale_variant_without_autoscaling(node_catalog):
    assert "autoscale_off" not in by_name(generate([cluster()]))


# VM generation -----------------------------------------------------------------------------------------
def test_vm_generations_skip_the_current_one(node_catalog):
    names = set(by_name(generate([cluster(worker="Standard_E8ds_v5")])))
    assert {"vm_v4", "vm_v6"} <= names and "vm_v5" not in names


def test_vm_generations_all_tested_when_clusters_differ(node_catalog):
    vs = generate([cluster(key="a", worker="Standard_E8ds_v5"), cluster(key="b", worker="Standard_E8ds_v4")])
    assert {"vm_v4", "vm_v5", "vm_v6"} <= set(by_name(vs))


def test_vm_generation_moves_worker_and_driver(node_catalog):
    v = by_name(generate([cluster(worker="Standard_E8ds_v5")]))["vm_v4"]
    assert overrides(v) == {"c1": {"worker_node_type": "Standard_E8ds_v4", "driver_node_type": "Standard_E8ds_v4"}}


def test_driver_keeps_its_type_when_the_generation_does_not_exist(node_catalog):
    v = by_name(generate([cluster(worker="Standard_E8ds_v5", driver="Standard_E16ds_v5")]))["vm_v4"]
    assert overrides(v)["c1"] == {"worker_node_type": "Standard_E8ds_v4", "driver_node_type": "Standard_E16ds_v5"}


def test_vm_generation_invalid_when_worker_type_missing(node_catalog):
    v = by_name(generate([cluster(worker="Standard_D8ds_v5")]))["vm_v4"]
    assert v["is_valid"] is False and "Standard_D8ds_v5" in v["invalid_reason"]


# Delta cache -------------------------------------------------------------------------------------------
def test_cache_off_for_local_disk_vms(node_catalog):
    vs = by_name(generate([cluster(worker="Standard_E8ds_v5")]))
    assert "cache_on" not in vs
    assert overrides(vs["cache_off"]) == {"c1": {"worker_node_type": "Standard_E8s_v5", "driver_node_type": "Standard_E8s_v5"}}


def test_cache_on_for_vms_without_local_disk(node_catalog):
    vs = by_name(generate([cluster(worker="Standard_E8s_v5")]))
    assert "cache_off" not in vs
    assert overrides(vs["cache_on"])["c1"]["worker_node_type"] == "Standard_E8ds_v5"


# Spot --------------------------------------------------------------------------------------------------
def test_spot_variant_only_touches_on_demand_clusters(node_catalog):
    vs = by_name(generate([cluster(key="od", first_on_demand=2), cluster(key="spot", availability="SPOT_WITH_FALLBACK_AZURE")]))
    assert overrides(vs["spot_with_fallback"]) == {
        "od": {"azure_availability": "SPOT_WITH_FALLBACK_AZURE", "first_on_demand": 2}}


def test_spot_clusters_are_never_benchmarked_as_on_demand(node_catalog):
    vs = generate([cluster(availability="SPOT_WITH_FALLBACK_AZURE")])
    assert not [v for v in vs if v["dimension"] == "availability"]


# Photon ------------------------------------------------------------------------------------------------
def test_photon_off_when_any_cluster_uses_photon(node_catalog):
    vs = by_name(generate([cluster(key="a", runtime_engine="PHOTON"), cluster(key="b")]))
    assert "photon_on" not in vs
    assert overrides(vs["photon_off"]) == {"a": {"runtime_engine": "STANDARD"}, "b": {"runtime_engine": "STANDARD"}}


def test_photon_on_when_no_cluster_uses_photon(node_catalog):
    vs = by_name(generate([cluster(runtime_engine=None)]))
    assert overrides(vs["photon_on"]) == {"c1": {"runtime_engine": "PHOTON"}}


# DBR ---------------------------------------------------------------------------------------------------
def test_dbr_candidates_skip_the_current_version(node_catalog):
    vs = by_name(generate([cluster(spark_version="15.4.x-scala2.12")]))
    assert "dbr_15_4" not in vs
    assert overrides(vs["dbr_16_4"]) == {"c1": {"spark_version": "16.4.x-scala2.12"}}
    assert vs["dbr_16_4"]["description"] == "DBR 16.4 (16.4.x-scala2.12)"


def test_dbr_candidates_all_tested_when_clusters_differ(node_catalog):
    vs = generate([cluster(key="a", spark_version="15.4.x-scala2.12"), cluster(key="b", spark_version="14.3.x-scala2.12")])
    assert {"dbr_15_4", "dbr_16_4"} <= set(by_name(vs))


# Shuffle partitions ------------------------------------------------------------------------------------
def test_shuffle_default_is_200(node_catalog):
    vs = by_name(generate([cluster()]))
    assert "shuffle_200" not in vs
    assert overrides(vs["shuffle_auto"]) == {"c1": {"spark_conf_add": {"spark.sql.shuffle.partitions": "auto"}}}


def test_shuffle_auto_skipped_when_already_auto(node_catalog):
    vs = by_name(generate([cluster(spark_conf={"spark.sql.shuffle.partitions": "auto"})]))
    assert "shuffle_auto" not in vs and "shuffle_200" in vs


def test_both_shuffle_variants_for_a_custom_value(node_catalog):
    vs = by_name(generate([cluster(spark_conf={"spark.sql.shuffle.partitions": "64"})]))
    assert {"shuffle_auto", "shuffle_200"} <= set(vs)


# Serverless --------------------------------------------------------------------------------------------
def test_serverless_is_optional(node_catalog):
    assert not [v for v in generate([cluster()]) if v["dimension"] == "serverless"]


def test_serverless_needs_the_job_settings(node_catalog):
    with pytest.raises(ValueError, match="job_settings"):
        generate([cluster()], include_serverless=True)


def test_serverless_gives_standard_and_performance_optimized(node_catalog):
    vs = by_name(generate([cluster()], include_serverless=True, job_settings=notebook_job("t1")))
    assert overrides(vs["serverless_standard"])["_serverless"]["performance_target"] == "STANDARD"
    assert overrides(vs["serverless_perf_optimized"])["_serverless"]["performance_target"] == "PERFORMANCE_OPTIMIZED"
    assert "(4-6 min startup)" in vs["serverless_standard"]["description"]
    assert "(fast startup, higher price)" in vs["serverless_perf_optimized"]["description"]


def test_serverless_notebook_tasks_are_valid():
    v = serverless(notebook_job("a", "b"), env_version="4")
    assert v["is_valid"] and v["invalid_reason"] is None
    assert v["overrides"] == {"_serverless": {"performance_target": "STANDARD", "environment_version": "4",
                                              "tasks": {"a": {"type": "notebook_task"}, "b": {"type": "notebook_task"}}}}


def test_serverless_jar_needs_scala_213():
    job = {"tasks": [{"task_key": "j", "job_cluster_key": "c1", "spark_jar_task": {"main_class_name": "M"},
                      "libraries": [{"jar": "/Volumes/a.jar"}]}]}
    old = serverless(job, [cluster(spark_version="15.4.x-scala2.12")])
    assert not old["is_valid"] and "Scala 2.13" in old["invalid_reason"]
    new = serverless(job, [cluster(spark_version="16.4.x-scala2.13")])
    assert new["is_valid"], new["invalid_reason"]
    assert new["overrides"]["_serverless"]["tasks"]["j"] == {"type": "spark_jar_task", "java_dependencies": ["/Volumes/a.jar"]}


def test_serverless_jar_blockers():
    job = {"tasks": [{"task_key": "j", "job_cluster_key": "c1", "spark_jar_task": {"main_class_name": "M"},
                      "libraries": [{"maven": {"coordinates": "a:b:1"}}]}]}
    v = serverless(job, [cluster(spark_version="16.4.x-scala2.13")])
    assert not v["is_valid"]
    assert "libraries ['maven']" in v["invalid_reason"]
    assert "no JAR library" in v["invalid_reason"]


def test_serverless_python_dependencies():
    job = {"tasks": [{"task_key": "p", "job_cluster_key": "c1", "spark_python_task": {"python_file": "/a.py"},
                      "libraries": [{"whl": "/Volumes/a.whl"}, {"pypi": {"package": "requests==2.32"}},
                                    {"requirements": "/Volumes/req.txt"}]}]}
    v = serverless(job)
    assert v["is_valid"], v["invalid_reason"]
    assert v["overrides"]["_serverless"]["tasks"]["p"]["dependencies"] == [
        "/Volumes/a.whl", "requests==2.32", "-r /Volumes/req.txt"]


def test_serverless_blocks_pypi_from_a_private_repo():
    job = {"tasks": [{"task_key": "p", "job_cluster_key": "c1", "python_wheel_task": {"package_name": "x"},
                      "libraries": [{"pypi": {"package": "x", "repo": "https://pypi.example.com"}}]}]}
    v = serverless(job)
    assert not v["is_valid"] and "no serverless environment equivalent" in v["invalid_reason"]


@pytest.mark.parametrize("task,reason", [
    ({"notebook_task": {"notebook_path": "/a"}, "libraries": [{"pypi": {"package": "x"}}]}, "notebook task libraries"),
    ({"spark_submit_task": {"parameters": []}}, "spark_submit_task is not supported"),
    ({"for_each_task": {"inputs": "[]", "task": {}}}, "for_each_task"),
    ({"some_new_task": {}}, "task type unknown not handled"),
])
def test_serverless_blockers(task, reason):
    v = serverless({"tasks": [dict(task_key="t", job_cluster_key="c1", **task)]})
    assert not v["is_valid"] and reason in v["invalid_reason"]


def test_serverless_tasks_without_compute_are_fine():
    v = serverless({"tasks": [{"task_key": "q", "sql_task": {"query": {"query_id": "1"}}},
                              {"task_key": "r", "run_job_task": {"job_id": 1}}]})
    assert v["is_valid"], v["invalid_reason"]


def test_serverless_dbt_gets_dbt_databricks():
    v = serverless({"tasks": [{"task_key": "d", "job_cluster_key": "c1", "dbt_task": {"commands": ["dbt run"]}}]})
    assert v["is_valid"]
    assert v["overrides"]["_serverless"]["tasks"]["d"]["dependencies"] == ["dbt-databricks"]


def test_serverless_all_purpose_cluster_dbr_is_looked_up():
    job = {"tasks": [{"task_key": "j", "existing_cluster_id": "0101-abc", "spark_jar_task": {"main_class_name": "M"},
                      "libraries": [{"jar": "/Volumes/a.jar"}]}]}
    v = serverless(job, cluster_dbr=lambda cid: "16.4.x-scala2.13" if cid == "0101-abc" else None)
    assert v["is_valid"], v["invalid_reason"]
    assert "j: was on an all-purpose cluster" in v["description"]


def test_serverless_cluster_lookup_failure_is_treated_as_unknown():
    def boom(cid):
        raise RuntimeError("cluster deleted")
    job = {"tasks": [{"task_key": "j", "existing_cluster_id": "x", "spark_jar_task": {"main_class_name": "M"},
                      "libraries": [{"jar": "/Volumes/a.jar"}]}]}
    v = serverless(job, cluster_dbr=boom)
    assert not v["is_valid"] and "unknown DBR/Scala" in v["invalid_reason"]


def test_serverless_lists_dropped_cluster_settings():
    c = cluster(spark_conf={"spark.sql.shuffle.partitions": "64", "spark.databricks.io.cache.enabled": "true"},
                init_scripts=[{"workspace": {"destination": "/init.sh"}}])
    d = serverless(notebook_job("t"), [c])["description"]
    assert "c1 spark_conf dropped (session-settable in code: spark.sql.shuffle.partitions)" in d
    assert "(not available on serverless: spark.databricks.io.cache.enabled)" in d
    assert "c1: init scripts dropped" in d


# Helpers -----------------------------------------------------------------------------------------------
@pytest.mark.parametrize("task,expected", [
    ({"notebook_task": {}}, "notebook_task"), ({"spark_jar_task": {}}, "spark_jar_task"),
    ({"condition_task": {}}, "condition_task"), ({"task_key": "x"}, "unknown"),
])
def test_task_type(task, expected):
    assert bv.task_type(task) == expected


def test_per_cluster_collects_overrides_or_fails_on_the_first_miss():
    cs = [cluster(key="a"), cluster(key="b")]
    ok = bv.per_cluster(cs, "v", "d", "desc", lambda c: {"x": c["job_cluster_key"]})
    assert ok["is_valid"] and ok["overrides"] == {"a": {"x": "a"}, "b": {"x": "b"}}
    bad = bv.per_cluster(cs, "v", "d", "desc", lambda c: None if c["job_cluster_key"] == "b" else {})
    assert not bad["is_valid"] and bad["overrides"] == {}
    assert bad["invalid_reason"] == "No matching node type for b (Standard_E8ds_v5)"
