"""Run context: configuration + Spark session + storage layer.

Every pipeline step receives this one object instead of three arguments. Steps never
create a session or read configuration themselves — so the same step runs from the
local CLI, from a Databricks Job task (wheel entry point) and from pytest (which passes
a small configuration and a temporary data directory).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from smogcast.core.config import OFFLINE_OVERRIDES, load_config
from smogcast.core.storage import Storage


@dataclass
class Context:
    cfg: dict
    spark: SparkSession
    storage: Storage

    @property
    def run(self) -> dict:
        return self.cfg["run"]


def build_context(env: str | None = None, offline: bool = False, overrides: dict | None = None) -> Context:
    # Spark is imported lazily inside get_spark's module, so `--help` stays instant.
    from smogcast.core.config import deep_merge
    from smogcast.core.session import get_spark

    extra = deep_merge(OFFLINE_OVERRIDES, overrides or {}) if offline else overrides
    cfg = load_config(env, extra)
    spark = get_spark(cfg)
    return Context(cfg, spark, Storage(spark, cfg))
