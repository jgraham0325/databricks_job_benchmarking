"""Run queue, batching, run timing and code fingerprints (04_orchestrator)."""
from collections import Counter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

import pytest

import bench_orchestration as bo

T0 = datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc)
MS0 = int(T0.timestamp() * 1000)


def at(s):
    return T0 + timedelta(seconds=s)


# Task-only time = union of execution intervals ------------------------------------------------------------
@pytest.mark.parametrize("intervals, expected", [
    ([], 0),
    ([(at(0), at(10))], 10),
    ([(at(0), at(10)), (at(20), at(30))], 20),          # gap is not counted
    ([(at(0), at(10)), (at(5), at(15))], 15),           # overlap counted once
    ([(at(0), at(30)), (at(5), at(10))], 30),           # nested
    ([(at(0), at(10)), (at(10), at(20))], 20),          # touching
    ([(at(0), None), (None, at(5)), (at(9), at(3)), (at(0), at(4))], 4),  # open / inverted ignored
])
def test_union_seconds(intervals, expected):
    assert bo.union_seconds(intervals) == expected


@pytest.mark.parametrize("lcs, rs, expected", [
    ("TERMINATED", "SUCCESS", "SUCCESS"),
    ("TERMINATED", "FAILED", "FAILED"),
    ("TERMINATED", "CANCELED", "FAILED"),
    ("INTERNAL_ERROR", None, "FAILED"),
    ("SKIPPED", None, "SKIPPED"),          # never ran: not a failure
    ("RUNNING", None, "RUNNING"),
    ("PENDING", None, "RUNNING"),
    ("QUEUED", None, "RUNNING"),
])
def test_run_status(lcs, rs, expected):
    assert bo.run_status(lcs, rs) == expected


def enum(v):
    return NS(value=v) if v else None


def task(key, start_s, setup_s, exec_s, cleanup_s=0, result="SUCCESS", cluster_id="c-1"):
    return NS(run_id=hash(key) % 10**6, task_key=key, attempt_number=0, job_cluster_key="jc",
              cluster_instance=NS(cluster_id=cluster_id), start_time=MS0 + start_s * 1000,
              setup_duration=setup_s * 1000, execution_duration=exec_s * 1000, cleanup_duration=cleanup_s * 1000,
              queue_duration=None, state=NS(result_state=enum(result)))


def run(tasks, lcs="TERMINATED", rs="SUCCESS", start_s=0, end_s=None, msg=""):
    return NS(run_id=42, start_time=MS0 + start_s * 1000, end_time=MS0 + end_s * 1000 if end_s is not None else 0,
              tasks=tasks, state=NS(life_cycle_state=enum(lcs), result_state=enum(rs), state_message=msg))


def test_run_metrics_separates_startup_from_task_time():
    # a: setup 0-600, executes 600-700. b: setup 0-500, executes 500-650. c runs after a: setup 700-760, executes 760-800
    r = run([task("a", 0, 600, 100), task("b", 0, 500, 150), task("c", 700, 60, 40)], end_s=830)
    m = bo.run_metrics(r)
    assert m["status"] == "SUCCESS" and m["run_id"] == "42"
    assert m["duration_seconds"] == 830
    assert m["task_only_seconds"] == 240          # 500-700 and 760-800
    assert m["overhead_seconds"] == 590
    assert m["task_exec_sum_seconds"] == 290
    assert m["max_setup_seconds"] == 600
    assert m["n_tasks"] == 3
    a = m["tasks"][0]
    assert (a["exec_start"], a["exec_end"], a["setup_seconds"], a["execution_seconds"]) == (at(600), at(700), 600, 100)
    assert (a["cluster_id"], a["result_state"], a["queue_seconds"]) == ("c-1", "SUCCESS", 0)


def test_run_metrics_for_a_running_run_has_no_task_only_time():
    m = bo.run_metrics(run([task("a", 0, 600, 0)], lcs="RUNNING", rs=None))
    assert m["status"] == "RUNNING"
    assert m["duration_seconds"] is None and m["task_only_seconds"] is None and m["overhead_seconds"] is None
    assert m["tasks"][0]["exec_end"] is None


def test_run_metrics_skipped_run():
    m = bo.run_metrics(run([], lcs="SKIPPED", rs=None, end_s=1, msg="x" * 900))
    assert m["status"] == "SKIPPED" and m["n_tasks"] == 0 and m["task_only_seconds"] == 0
    assert len(m["state_message"]) == 500


def test_run_metrics_task_without_cluster_or_state():
    t = task("a", 0, 0, 10)
    t.cluster_instance, t.state = None, None
    m = bo.run_metrics(run([t], end_s=10))
    assert m["tasks"][0]["cluster_id"] is None and m["tasks"][0]["result_state"] is None


# Queue and batches --------------------------------------------------------------------------------------
def test_queue_tops_up_each_variant_to_max_runs():
    jobs = {"baseline": {"id": 1}, "vm_v6": {"id": 2}, "photon_off": {"id": 3}}
    q = bo.build_queue(jobs, {"baseline": 1, "vm_v6": 3}, max_runs=2)
    assert [(v, n) for v, _, n in q] == [("baseline", 2), ("photon_off", 1), ("photon_off", 2)]
    assert q[0][1] == {"id": 1}


def test_queue_is_empty_when_every_variant_has_enough_runs():
    assert bo.build_queue({"a": {}}, {"a": 2}, max_runs=2) == []


def q(*names):
    return [(n, None, i) for i, n in enumerate(names)]


def test_batches_hold_at_most_one_run_per_variant():
    # Regression: two baseline runs in one batch made the second one SKIPPED (max_concurrent_runs = 1)
    batches = bo.make_batches(q("baseline", "baseline", "vm_v6", "baseline"), batch_size=3)
    assert [[x[0] for x in b] for b in batches] == [["baseline", "vm_v6"], ["baseline"], ["baseline"]]


@pytest.mark.parametrize("names, size", [(("a", "b", "c", "d", "e"), 2), (("a", "a", "b", "b", "c"), 3),
                                         (("a",) * 4, 5), ((), 3), (("a", "b", "c"), 1)])
def test_batches_keep_every_run_exactly_once_and_respect_the_size(names, size):
    queue = q(*names)
    batches = bo.make_batches(queue, size)
    flat = [x for b in batches for x in b]
    assert Counter(flat) == Counter(queue)
    assert all(0 < len(b) <= size and len({x[0] for x in b}) == len(b) for b in batches)


def test_batches_preserve_queue_order_when_possible():
    assert bo.make_batches(q("a", "b", "c", "d"), 2) == [q("a", "b"), [("c", None, 2), ("d", None, 3)]]


def test_batch_size_must_be_positive():
    with pytest.raises(ValueError):
        bo.make_batches(q("a"), 0)


# Code version -------------------------------------------------------------------------------------------
def ws_item(path):
    return f"sha256:{path}", 1000


def file_item(path):
    return f"size:1,mtime:{path}", 2000 if path.endswith(".jar") else 500


JOB = {
    "git_source": {"git_url": "https://github.com/x/y", "git_branch": "main"},
    "tasks": [
        {"task_key": "nb", "notebook_task": {"notebook_path": "/Repos/x/nb"}},
        {"task_key": "git_nb", "notebook_task": {"notebook_path": "nb", "source": "GIT"}},
        {"task_key": "py", "spark_python_task": {"python_file": "/Volumes/a/p.py"}},
        {"task_key": "jar", "spark_jar_task": {"main_class_name": "com.x.Main"},
         "libraries": [{"jar": "/Volumes/a/app.jar"}, {"maven": {"coordinates": "g:a:1"}}, {"pypi": {"package": "p"}}]},
        {"task_key": "env", "python_wheel_task": {}, "environment_key": "e"},
    ],
    "environments": [{"environment_key": "e", "spec": {"dependencies": ["pandas", "/Volumes/a/w.whl"]}}],
}


def test_code_manifest_records_what_each_task_runs():
    man, newest = bo.code_manifest(JOB, ws_item, file_item)
    assert man == {
        "git_source": "https://github.com/x/y@branch:main",
        "nb:/Repos/x/nb": "sha256:/Repos/x/nb",
        "py:/Volumes/a/p.py": "size:1,mtime:/Volumes/a/p.py",
        "jar:main_class": "com.x.Main",
        "jar:maven": "g:a:1",
        "jar:pypi": "p",
        "jar:/Volumes/a/app.jar": "size:1,mtime:/Volumes/a/app.jar",
        "env:dep:pandas": "pandas",
        "env:/Volumes/a/w.whl": "size:1,mtime:/Volumes/a/w.whl",
    }
    assert newest == 2000


def test_git_source_prefers_commit_then_tag_then_branch():
    for gs, want in [({"git_url": "u", "git_commit": "abc", "git_tag": "t"}, "u@abc"),
                     ({"git_url": "u", "git_tag": "v1", "git_branch": "main"}, "u@v1")]:
        assert bo.code_manifest({"git_source": gs}, ws_item, file_item)[0] == {"git_source": want}


def test_code_version_is_stable_and_sensitive_to_changes():
    man, _ = bo.code_manifest(JOB, ws_item, file_item)
    v = bo.code_version(man)
    assert len(v) == 12 and int(v, 16) >= 0
    assert bo.code_version(dict(reversed(list(man.items())))) == v
    assert bo.code_version({**man, "nb:/Repos/x/nb": "sha256:changed"}) != v
