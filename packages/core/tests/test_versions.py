"""All smogcast wheels are versioned in lockstep with the root VERSION file.

Step wheels pin ``smogcast-core==<version>``; if one pyproject drifts, a Databricks Job
would install mismatched wheels — this test catches it before CI builds them.
(Plain regexes instead of tomllib: tomllib is stdlib only from Python 3.11.)
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
VERSION = (ROOT / "VERSION").read_text().strip()


def _pyprojects():
    return sorted((ROOT / "packages").glob("*/pyproject.toml"))


def test_every_package_has_the_root_version():
    found = {}
    for p in _pyprojects():
        m = re.search(r'^version = "([^"]+)"', p.read_text(encoding="utf-8"), re.MULTILINE)
        found[p.parent.name] = m.group(1) if m else None
    assert len(found) == 9, found
    assert set(found.values()) == {VERSION}, found


def test_internal_dependencies_pin_the_same_version():
    for p in _pyprojects():
        for name, version in re.findall(r'"(smogcast-[a-z-]+)==([^"]+)"', p.read_text(encoding="utf-8")):
            assert version == VERSION, f"{p.parent.name}: {name}=={version}"


def test_wheels_do_not_pull_spark_onto_databricks():
    # The runtime ships PySpark and Delta; a wheel dependency could install a second copy next to them.
    # Both belong only to the `local` extra of smogcast-core (Docker, CI).
    for p in _pyprojects():
        text = p.read_text(encoding="utf-8")
        deps = re.search(r"^dependencies = \[(.*?)^\]", text, re.MULTILINE | re.DOTALL)
        assert deps, p
        assert not re.search(r'"(pyspark|delta-spark)\b', deps.group(1)), f"{p.parent.name} depends on Spark"
    local = re.search(r"^local = \[(.*?)^\]", (ROOT / "packages/core/pyproject.toml").read_text(encoding="utf-8"),
                      re.MULTILINE | re.DOTALL)
    assert local and '"pyspark' in local.group(1) and '"delta-spark' in local.group(1)


def test_no_package_owns_the_namespace_init():
    # PEP 420: an __init__.py in src/smogcast would hide the other wheels' sub-packages.
    assert not list((ROOT / "packages").glob("*/src/smogcast/__init__.py"))
