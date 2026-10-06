"""silver_clean steps: each is ``(Context) -> None``."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from smogcast.core.config import active_cities
from smogcast.core.context import Context
from smogcast.core.dq_metrics import log_metrics
from smogcast.core.timeutil import day_cet_col
from smogcast.silver_clean.pm_clean import flag_patterns, split_hard_rules
from smogcast.silver_clean.station_city import build_station_city, cities_frame
from smogcast.silver_clean.stations_scd2 import apply_scd2, changes_from_snapshots

log = logging.getLogger(__name__)


def step_pm_hourly(ctx: Context) -> None:
    """bronze.pm_hourly (archive) → silver.station_city, silver.pm_hourly, ops.pm_quarantine.

    Only stations of the configured cities are kept. Renamed stations are unified under their
    CURRENT code (``source_station_code`` keeps what the archive said).
    """
    st, dq = ctx.storage, ctx.cfg["pm_dq"]
    station_city = build_station_city(
        st.read("bronze", "gios_stations"), cities_frame(ctx.spark, active_cities(ctx.cfg))
    )
    st.overwrite(station_city, "silver", "station_city")
    station_city = st.read("silver", "station_city")

    pm = (
        st.read("bronze", "pm_hourly")
        .withColumnRenamed("station_code", "source_station_code")
        .join(F.broadcast(station_city.select(
            F.col("station_code").alias("source_station_code"), "current_station_code", "city_id", "jurisdiction_code"
        )), "source_station_code")
        .withColumnRenamed("current_station_code", "station_code")
        .select("station_code", "source_station_code", "city_id", "jurisdiction_code", "pollutant",
                "time_utc", "value", "source_year")
        .withColumn("source", F.lit("archive"))
        # A renamed station could report the same hour under both codes — keep one.
        .dropDuplicates(dq["dedup"]["keys"])
    )
    ok, quarantined = split_hard_rules(pm, dq)
    st.overwrite(quarantined, "ops", "pm_quarantine", replace_where="source = 'archive'")

    clean = (
        flag_patterns(ok, dq)
        .withColumn("day_cet", day_cet_col(F.col("time_utc")))
        .withColumn("hour_cet", F.hour(F.col("time_utc") + F.expr("INTERVAL 1 HOUR")))
    )
    st.overwrite(clean, "silver", "pm_hourly", partition_by=["source_year"])

    out = st.read("silver", "pm_hourly")
    metrics = {
        "stations_matched": station_city.where("code_kind = 'current'").count(),
        "old_codes_mapped": station_city.where("code_kind = 'old'").count(),
        "quarantined_archive": st.read("ops", "pm_quarantine").where("source = 'archive'").count(),
    }
    for r in out.groupBy("dq_flag").count().collect():
        metrics[f"rows_{r['dq_flag']}"] = r["count"]
    log_metrics(st, "silver_clean_pm_hourly", metrics)


def step_stations_scd2(ctx: Context) -> None:
    """bronze.station_snapshots → silver.station_changes (CDC feed) → silver.stations (SCD2)."""
    st = ctx.storage
    changes = changes_from_snapshots(st.read("bronze", "station_snapshots"))
    st.overwrite(changes, "silver", "station_changes")
    st.overwrite(apply_scd2(st.read("silver", "station_changes")), "silver", "stations")
    metrics = {f"changes_{r['registry_source']}_{r['op']}": r["count"]
               for r in st.read("silver", "station_changes").groupBy("registry_source", "op").count().collect()}
    metrics["current_stations"] = st.read("silver", "stations").where("is_current").count()
    log_metrics(st, "silver_stations_scd2", metrics)


# ----------------------------------------------------------------------------- stream (E6d)
SOURCES = {"live": "gios_live", "fault_replay": "fault_replay"}  # source -> landing folder


def _keys_or_empty(ctx: Context, layer: str, name: str, source: str, cols: list[str]):
    st = ctx.storage
    if st.exists(layer, name):
        return st.read(layer, name).where(F.col("source") == source).select(*cols)
    return ctx.spark.createDataFrame([], "source STRING, station_code STRING, pollutant STRING, "
                                         "time_utc TIMESTAMP, value DOUBLE").select(*cols)


def _stream_mapping(ctx: Context):
    from smogcast.silver_clean.pm_stream import stream_station_city

    st = ctx.storage
    registry = (st.read("silver", "stations").where("registry_source = 'gios_api' AND is_current")
                if st.exists("silver", "stations") else
                ctx.spark.createDataFrame([], "station_code STRING, city_name STRING"))
    return stream_station_city(st.read("silver", "station_city"), registry,
                               cities_frame(ctx.spark, active_cities(ctx.cfg)))


def _stream_id(ctx: Context, source: str) -> str:
    """Id of the streaming query, stored by Spark in its checkpoint (``metadata``) before the first batch.

    Idempotent appends are keyed by (app id, batch id). Batch ids restart at 0 with a new checkpoint
    (after fault-reset / live-reset), and Delta silently SKIPS an append whose (app id, version) it
    has already seen — so the app id must change with the checkpoint, i.e. include this id.
    """
    import json
    from pathlib import Path

    meta = Path(ctx.storage.checkpoint(f"pm_stream_{source}")) / "metadata"
    return json.loads(meta.read_text(encoding="utf-8").splitlines()[0])["id"]


def _process_batch(ctx: Context, source: str, batch, batch_id: int) -> None:
    from smogcast.silver_clean import pm_stream as ps

    st, dq = ctx.storage, ctx.cfg["pm_dq"]
    batch = batch.cache()
    if batch.isEmpty():
        batch.unpersist()
        return
    app = f"{source}_{_stream_id(ctx, source)}"
    st.append_idempotent(batch, "bronze", "pm_stream", f"pm_stream_bronze_{app}", batch_id)

    stored = _keys_or_empty(ctx, "silver", "pm_stream", source, [*ps.KEY, "value"])
    late_seen = _keys_or_empty(ctx, "ops", "pm_late_rejected", source, ps.KEY)
    quarantine_seen = _keys_or_empty(ctx, "ops", "pm_quarantine", source, [*ps.KEY, "value"])
    # localCheckpoint (eager): freeze the classification BEFORE writing. A lazily re-evaluated plan
    # would re-read silver after the merge below and report every accepted row as a duplicate.
    classified = ps.classify(batch, stored, late_seen, quarantine_seen,
                             dq["late_arrival"]["max_lag_hours"]).localCheckpoint()
    ok, late, bad = ps.split_new(classified, _stream_mapping(ctx), dq)
    ok, late, bad = ok.localCheckpoint(), late.localCheckpoint(), bad.localCheckpoint()

    st.append_idempotent(late, "ops", "pm_late_rejected", f"pm_late_{app}", batch_id)
    st.append_idempotent(bad, "ops", "pm_quarantine", f"pm_quarantine_{app}", batch_id)

    if not ok.isEmpty():
        bounds = ok.agg(F.min("time_utc").alias("lo"), F.max("time_utc").alias("hi")).first()
        d = dq["drift"]
        context_hours = d["window_hours"] + d["baseline_hours"] + 2
        cities = [r["city_id"] for r in ok.select("city_id").distinct().collect()]
        cols = ["source", "station_code", "city_id", "jurisdiction_code", "pollutant", "time_utc", "value",
                "sent_at", "lag_hours", "status"]
        fresh = ok.select(*cols)
        if st.exists("silver", "pm_stream"):
            history = (
                st.read("silver", "pm_stream")
                .where((F.col("source") == source) & F.col("city_id").isin(cities)
                       & (F.col("time_utc") >= F.lit(bounds["lo"]) - F.expr(f"INTERVAL {context_hours} HOURS"))
                       & (F.col("time_utc") <= F.lit(bounds["hi"]) + F.expr("INTERVAL 3 HOURS")))
                .select(*cols)
                .join(fresh.select(*ps.KEY), ps.KEY, "left_anti")
            )
            fresh = history.unionByName(fresh)
        st.merge(ps.recompute_flags(fresh, dq).select(*ps.SILVER_COLUMNS), "silver", "pm_stream", ps.KEY)

    counts = {r["status"]: r["count"] for r in classified.groupBy("status").count().collect()}
    log_metrics(st, f"silver_pm_stream_{source}", {
        "received": batch.count(), "duplicates": counts.get("duplicate", 0), "corrections": counts.get("correction", 0),
        "late_rejected": late.count(), "quarantined": bad.count(), "accepted": ok.count(),
    })
    batch.unpersist()


def step_pm_stream(ctx: Context) -> None:
    """Landing batches (live API and fault replay) → cleaned silver.pm_stream — Structured Streaming.

    trigger(availableNow): process every waiting file, then stop. The checkpoint remembers which
    files were done, so a re-run only picks up new batches (exactly-once with idempotent sinks).
    """
    from pathlib import Path

    from smogcast.silver_clean.pm_stream import LANDING_SCHEMA, flatten

    st = ctx.storage
    for source, folder in SOURCES.items():
        path = st.landing(folder)
        if not Path(path).exists() or not any(Path(path).glob("*.json")):
            log.info("pm-stream: no batches for %s", source)
            continue
        stream = (
            ctx.spark.readStream.schema(LANDING_SCHEMA)
            .option("multiLine", True)
            .option("pathGlobFilter", "*.json")   # skip *.json.part files that are still being written
            .json(path)
        )
        query = (
            flatten(stream).writeStream
            .foreachBatch(lambda df, bid, s=source: _process_batch(ctx, s, df, bid))
            .option("checkpointLocation", st.checkpoint(f"pm_stream_{source}"))
            .trigger(availableNow=True)
            .queryName(f"pm_stream_{source}")
            .start()
        )
        query.awaitTermination()
        log.info("pm-stream: %s processed", source)


def _reset_stream_source(ctx: Context, source: str) -> None:
    """Forget everything the stream derived from one source: its checkpoint and its rows in the
    stream tables. The next pm-stream run then re-reads every landing file of that source."""
    import shutil

    st = ctx.storage
    shutil.rmtree(st.checkpoint(f"pm_stream_{source}"), ignore_errors=True)
    for layer, name in (("bronze", "pm_stream"), ("ops", "pm_late_rejected"), ("ops", "pm_quarantine"),
                        ("silver", "pm_stream")):
        if st.exists(layer, name):
            st.delta_table(layer, name).delete(F.col("source") == source)


def step_fault_reset(ctx: Context) -> None:
    """Remove everything the fault-injection demo produced, so it can be replayed from scratch."""
    import shutil

    st = ctx.storage
    for folder in ("fault_replay", "fault_replay_registry"):
        shutil.rmtree(st.landing(folder), ignore_errors=True)
    _reset_stream_source(ctx, "fault_replay")
    for layer, name in (("silver", "station_changes"), ("silver", "stations")):
        if st.exists(layer, name):
            st.delta_table(layer, name).delete("registry_source = 'fault_replay'")
    log.info("fault-reset: fault_replay data removed")


def step_live_reset(ctx: Context) -> None:
    """Re-clean the live stream from its landing files (after a fix to the cleaning rules).

    Unlike fault-reset, landing/gios_live stays: those polls cannot be repeated (the API only
    returns the last ~3 days). Needed e.g. when readings were quarantined as ``unknown_station``
    before the registry knew the station — re-sent with the same value they would otherwise
    stay duplicates of the quarantined record forever.
    """
    _reset_stream_source(ctx, "live")
    log.info("live-reset: live stream state removed (landing kept)")
