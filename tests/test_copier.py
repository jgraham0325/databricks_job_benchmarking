"""Benchmark job copies (03_job_copier): isolation, safety and override application."""
import copy
import json

import pytest

import bench_copier as bcp
import bench_variants as bv


def variant(name="v1", overrides=None, source_job_id="123", benchmark_id="b1"):
    return dict(variant_name=name, variant_id=f"{benchmark_id}_{name}", source_job_id=source_job_id,
                overrides_json=json.dumps(overrides or {}))


def source_job(**extra):
    s = {
        "name": "Nightly ETL",
        "schedule": {"quartz_cron_expression": "0 0 2 * * ?", "timezone_id": "UTC"},
        "trigger": {"file_arrival": {"url": "/Volumes/x"}},
        "continuous": {"pause_status": "UNPAUSED"},
        "email_notifications": {"on_failure": ["ops@example.com"]},
        "webhook_notifications": {"on_failure": [{"id": "w"}]},
        "notification_settings": {"no_alert_for_skipped_runs": True},
        "run_as": {"service_principal_name": "sp"},
        "max_concurrent_runs": 5,
        "timeout_seconds": 0,
        "tags": {"team": "data"},
        "parameters": [{"name": "output_schema", "default": "prod.sales"}, {"name": "run_date", "default": ""}],
        "job_clusters": [
            {"job_cluster_key": "c1", "new_cluster": {"node_type_id": "Standard_E8ds_v5", "num_workers": 4,
                                                     "spark_conf": {"a": "1"}, "custom_tags": {"cost_center": "42"}}},
            {"job_cluster_key": "c2", "new_cluster": {"node_type_id": "Standard_D4ds_v5",
                                                     "autoscale": {"min_workers": 1, "max_workers": 3}}},
        ],
        "tasks": [
            {"task_key": "t1", "job_cluster_key": "c1", "notebook_task": {"notebook_path": "/a"},
             "email_notifications": {"on_failure": ["x@example.com"]}},
            {"task_key": "t2", "new_cluster": {"node_type_id": "Standard_D8ds_v5", "num_workers": 2},
             "spark_jar_task": {"main_class_name": "Main"}, "libraries": [{"jar": "/Volumes/a.jar"}]},
        ],
    }
    s.update(extra)
    return s


def copy_of(source, v=None, **kw):
    kw.setdefault("benchmark_id", "b1")
    kw.setdefault("prefix", "BENCH")
    kw.setdefault("output_catalog", "main")
    return bcp.prepare_copy(source, v or variant(), **kw)


# Isolation and safety -----------------------------------------------------------------------------------
def test_copy_never_modifies_the_source():
    src = source_job()
    before = copy.deepcopy(src)
    copy_of(src, variant(overrides={"c1": {"num_workers": 8}, "_all": {"spark_conf_add": {"b": "2"}}}))
    copy_of(src, variant(overrides={"_serverless": {"tasks": {}}}))
    assert src == before


def test_copy_has_no_schedule_triggers_notifications_or_run_as():
    s, _ = copy_of(source_job(edit_mode="UI_LOCKED", deployment={"kind": "BUNDLE"}, format="MULTI_TASK"))
    for key in bcp.JOB_KEYS_REMOVED:
        assert key not in s, key
    for t in s["tasks"]:
        assert not set(bcp.TASK_KEYS_REMOVED) & set(t)


def test_copy_allows_one_run_at_a_time():
    s, _ = copy_of(source_job())
    assert s["max_concurrent_runs"] == 1


@pytest.mark.parametrize("source_timeout, expected", [(0, 14400), (None, 14400), (600, 600)])
def test_copy_timeout_defaults_only_when_the_source_has_none(source_timeout, expected):
    s, _ = copy_of(source_job(timeout_seconds=source_timeout), timeout_seconds=14400)
    assert s["timeout_seconds"] == expected


def test_copy_is_named_with_the_benchmark_prefix():
    s, _ = copy_of(source_job(), variant("photon_off"), prefix="BENCH", benchmark_id="ab12")
    assert s["name"] == "[BENCH-ab12] Nightly ETL | photon_off"
    assert s["name"].startswith(bcp.name_prefix("BENCH", "ab12"))


def test_benchmark_tags_on_the_job_and_every_cluster():
    s, _ = copy_of(source_job(), variant("v1", source_job_id="123"))
    tags = {"benchmark_id": "b1", "variant_id": "b1_v1", "variant_name": "v1", "source_job_id": "123"}
    assert s["tags"] == {"team": "data", **tags}
    assert s["job_clusters"][0]["new_cluster"]["custom_tags"] == {"cost_center": "42", **tags}
    assert s["job_clusters"][1]["new_cluster"]["custom_tags"] == tags
    assert s["tasks"][1]["new_cluster"]["custom_tags"] == tags


def test_output_parameter_points_at_an_isolated_schema():
    s, out = copy_of(source_job(), variant("Cache_Off"), output_catalog="Bench_Out", prefix="BENCH", benchmark_id="b1")
    assert out == "bench_out.bench_b1_cache_off"
    params = {p["name"]: p["default"] for p in s["parameters"]}
    assert params == {"output_schema": out, "run_date": ""}


def test_output_parameter_is_added_when_missing():
    s, out = copy_of(source_job(parameters=[]))
    assert s["parameters"] == [{"name": "output_schema", "default": out}]


def test_custom_output_parameter_name():
    s, out = copy_of(source_job(), output_param_name="target_schema")
    assert {"name": "target_schema", "default": out} in s["parameters"]
    assert {"name": "output_schema", "default": "prod.sales"} in s["parameters"]


def test_every_variant_writes_to_a_different_schema():
    outs = {bcp.output_schema_for("main", "BENCH", "b1", n) for n in ["baseline", "vm_v4", "vm_v6", "photon_off"]}
    assert len(outs) == 4


def test_preflight_warns_when_outputs_are_not_parameterised():
    msgs = bcp.preflight_messages("123", source_job(parameters=[{"name": "x", "default": ""}]), "output_schema")
    assert msgs[0].startswith("WARNING job 123: parameter 'output_schema' not found")
    assert "production outputs" in msgs[0]
    assert bcp.preflight_messages("123", source_job(), "output_schema") == ["OK job 123: parameter 'output_schema' exists"]


def test_preflight_notes_git_source():
    msgs = bcp.preflight_messages("1", source_job(git_source={"git_url": "https://github.com/x/y"}), "output_schema")
    assert any(m.startswith("NOTE job 1: code comes from git_source https://github.com/x/y") for m in msgs)


# Overrides ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize("changes, expected", [
    ({"worker_node_type": "W"}, {"node_type_id": "W", "num_workers": 4}),
    ({"driver_node_type": "D"}, {"driver_node_type_id": "D", "num_workers": 4}),
    ({"num_workers": 6.0}, {"num_workers": 6}),
    ({"min_workers": 1, "max_workers": 2}, {"autoscale": {"min_workers": 1, "max_workers": 2}}),
    ({"spark_version": "16.4.x-scala2.12"}, {"spark_version": "16.4.x-scala2.12", "num_workers": 4}),
    ({"runtime_engine": "PHOTON"}, {"runtime_engine": "PHOTON", "num_workers": 4}),
    ({"azure_availability": "SPOT_WITH_FALLBACK_AZURE", "first_on_demand": 1.0},
     {"azure_attributes": {"availability": "SPOT_WITH_FALLBACK_AZURE", "first_on_demand": 1}, "num_workers": 4}),
    ({"spark_conf_add": {"b": "2"}}, {"spark_conf": {"a": "1", "b": "2"}, "num_workers": 4}),
])
def test_apply_to_cluster(changes, expected):
    spec = {"num_workers": 4}
    if "spark_conf_add" in changes:
        spec["spark_conf"] = {"a": "1"}
    bcp.apply_to_cluster(spec, changes)
    assert spec == expected


def test_fixed_workers_replace_autoscale():
    spec = {"autoscale": {"min_workers": 1, "max_workers": 3}}
    bcp.apply_to_cluster(spec, {"num_workers": 3})
    assert spec == {"num_workers": 3}


def test_unknown_override_key_fails_loudly():
    with pytest.raises(ValueError, match="Unknown override key"):
        bcp.apply_to_cluster({}, {"node_type": "x"})


def test_overrides_target_job_clusters_task_clusters_and_all():
    s, _ = copy_of(source_job(), variant(overrides={"c2": {"num_workers": 3}, "_task_t2": {"runtime_engine": "PHOTON"},
                                                    "missing": {"num_workers": 99}}))
    assert s["job_clusters"][1]["new_cluster"]["num_workers"] == 3
    assert "autoscale" not in s["job_clusters"][1]["new_cluster"]
    assert s["tasks"][1]["new_cluster"]["runtime_engine"] == "PHOTON"
    assert s["job_clusters"][0]["new_cluster"]["num_workers"] == 4      # untouched; unknown keys are ignored
    s, _ = copy_of(source_job(), variant(overrides={"_all": {"spark_version": "16.4.x-scala2.12"}}))
    assert [c["new_cluster"]["spark_version"] for c in s["job_clusters"]] == ["16.4.x-scala2.12"] * 2
    assert s["tasks"][1]["new_cluster"]["spark_version"] == "16.4.x-scala2.12"


# Serverless ---------------------------------------------------------------------------------------------
SERVERLESS = {"_serverless": {"performance_target": "PERFORMANCE_OPTIMIZED", "environment_version": "4", "tasks": {
    "t1": {"type": "notebook_task"},
    "t2": {"type": "spark_jar_task", "java_dependencies": ["/Volumes/a.jar"]},
}}}


def test_serverless_copy_has_no_cluster_compute():
    s, _ = copy_of(source_job(), variant(overrides=SERVERLESS))
    assert "job_clusters" not in s
    for t in s["tasks"]:
        assert not {"job_cluster_key", "new_cluster", "existing_cluster_id", "libraries"} & set(t)
    assert s["performance_target"] == "PERFORMANCE_OPTIMIZED"
    assert bcp.check_serverless(s) == []


def test_serverless_environments_only_on_tasks_that_need_them():
    s, _ = copy_of(source_job(), variant(overrides=SERVERLESS))
    t1, t2 = s["tasks"]
    assert "environment_key" not in t1                                   # notebook tasks use the notebook env
    assert s["environments"] == [{"environment_key": "env_1",
                                  "spec": {"environment_version": "4", "java_dependencies": ["/Volumes/a.jar"]}}]
    assert t2["environment_key"] == "env_1"


def test_serverless_tasks_with_the_same_dependencies_share_an_environment():
    s = {"tasks": [{"task_key": k, "spark_python_task": {}} for k in ("a", "b", "c")]}
    bcp.to_serverless(s, {"tasks": {"a": {"type": "spark_python_task"}, "b": {"type": "spark_python_task"},
                                    "c": {"type": "spark_python_task", "dependencies": ["x"]}}})
    assert [t["environment_key"] for t in s["tasks"]] == ["default", "default", "env_2"]
    assert len(s["environments"]) == 2
    assert s["performance_target"] == "STANDARD"


def test_check_serverless_finds_leftovers():
    bad = {"job_clusters": [{}], "environments": [{"environment_key": "e"}],
           "tasks": [{"task_key": "a", "job_cluster_key": "c1", "spark_jar_task": {}},
                     {"task_key": "b", "notebook_task": {}, "environment_key": "e"}]}
    assert bcp.check_serverless(bad) == ["job_clusters still present", "a: job_cluster_key still set",
                                         "a: missing environment_key", "b: notebook task should not have environment_key"]


# Golden test: a real benchmark --------------------------------------------------------------------------
def _bf1e865e_copies(fx):
    vs = bv.generate_variants(fx["job_id"], fx["clusters"], benchmark_id=fx["benchmark_id"], include_serverless=True,
                              job_settings=fx["job_settings"])
    return {v["variant_name"]: copy_of(fx["job_settings"], v, benchmark_id=fx["benchmark_id"])[0]
            for v in vs if v["is_valid"]}


def test_bf1e865e_baseline_copies_the_source_cluster_config_exactly(bf1e865e):
    base = _bf1e865e_copies(bf1e865e)["baseline"]
    strip = lambda c: {k: v for k, v in c["new_cluster"].items() if k != "custom_tags"}  # noqa: E731
    assert [strip(c) for c in base["job_clusters"]] == [c["new_cluster"] for c in bf1e865e["job_settings"]["job_clusters"]]


def test_bf1e865e_copies_are_valid_and_isolated(bf1e865e):
    copies = _bf1e865e_copies(bf1e865e)
    assert len(copies) == 12
    outs = set()
    for name, s in copies.items():
        out = {p["name"]: p["default"] for p in s["parameters"]}["output_schema"]
        assert out == f"main.bench_bf1e865e_{name}"
        outs.add(out)
        assert s["max_concurrent_runs"] == 1 and "queue" not in s and "schedule" not in s
        if name.startswith("serverless"):
            assert bcp.check_serverless(s) == []
    assert len(outs) == len(copies)
    assert copies["vm_v6"]["job_clusters"][1]["new_cluster"]["node_type_id"] == "Standard_E8ds_v6"
    assert copies["autoscale_off"]["job_clusters"][0]["new_cluster"]["num_workers"] == 3
    assert "autoscale" not in copies["autoscale_off"]["job_clusters"][0]["new_cluster"]
