"""The local CLI knows every step and its pipeline orders reference existing steps."""

import pytest

from smogcast.cli.main import ORDERS, PACKAGES, main


def test_orders_reference_existing_steps():
    for order in ORDERS.values():
        for pkg, step in order:
            assert step in PACKAGES[pkg]


def test_steps_command_lists_every_package(capsys):
    assert main(["steps"]) == 0
    out = capsys.readouterr().out
    for pkg in PACKAGES:
        assert f"smogcast-{pkg}" in out


def test_unknown_step_is_rejected():
    with pytest.raises(SystemExit) as exc:
        main(["run", "bronze", "no-such-step"])
    assert exc.value.code == 2


@pytest.mark.parametrize("job_file, order", [("job_smogcast.yml", "run-batch"), ("job_smogcast_live.yml", "run-live")])
def test_lakeflow_jobs_run_the_same_steps_as_the_cli(job_file, order):
    from pathlib import Path

    import yaml

    resources = Path(__file__).resolve().parents[3] / "resources"
    job = next(iter(yaml.safe_load((resources / job_file).read_text(encoding="utf-8"))["resources"]["jobs"].values()))
    steps = set()
    for task in job["tasks"]:
        t = task["python_wheel_task"]
        params = t["parameters"]
        steps.add((t["entry_point"].removeprefix("smogcast-"), params[params.index("--step") + 1]))
        keys = {d["task_key"] for d in task.get("depends_on", [])}
        assert keys <= {x["task_key"] for x in job["tasks"]}, task["task_key"]
    assert steps == set(ORDERS[order])
