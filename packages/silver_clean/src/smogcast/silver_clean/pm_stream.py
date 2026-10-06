"""Streaming cleaning of PM measurements (live GIOŚ API and fault-injection replay).

Landing batch files (contract in smogcast/ingest/gios_live.py) are read with Structured
Streaming (trigger availableNow: process what is waiting, then stop — the same code runs as a
continuous stream with another trigger, or as a Lakeflow declarative pipeline on Databricks).
Each micro-batch goes through ``process_batch``:

1. raw rows → ``bronze.pm_stream`` (append, idempotent per batch id);
2. classification of every record against what is already known for its key
   (source, station_code, pollutant, time_utc):
   - duplicate  — same value already stored (APIs re-send the last days on every poll),
   - correction — key stored with a different value (GIOŚ re-publishes verified values),
   - late       — new key that should have been available at the PREVIOUS fetch already
                  (hour end + max lag <= previous fetch) → ``ops.pm_late_rejected``;
                  on a first fetch there is no previous one, so old hours are a backfill, not late,
   - quarantine — hard-rule violation (reuses pm_clean.split_hard_rules) → ``ops.pm_quarantine``,
   - unknown station → quarantine as well (the station is not in the registry of our cities);
3. accepted rows are merged into ``silver.pm_stream`` together with the recent history of their
   city, and the pattern flags are recomputed on that window: frozen and spike (pm_clean, same
   code as for the archive) and drift (vs the city median, see ``flag_drift``).

``silver.pm_stream`` is separate from ``silver.pm_hourly`` (the model's history) — decision D17.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from smogcast.core.timeutil import day_cet_col, local_hour_end_to_utc_start_col
from smogcast.silver_clean.pm_clean import flag_patterns, split_hard_rules

KEY = ["source", "station_code", "pollutant", "time_utc"]
LANDING_SCHEMA = (
    "source STRING, fetched_at STRING, previous_fetched_at STRING, records ARRAY<STRUCT<"
    "position_code: STRING, station_code: STRING, pollutant: STRING, sensor_id: LONG, "
    "time_local_end: STRING, value: DOUBLE, sent_at: STRING>>"
)


def flatten(batches: DataFrame) -> DataFrame:
    """Landing documents → one row per record with parsed times and lag."""
    r = batches.select(
        "source", "fetched_at", "previous_fetched_at", F.col("_metadata.file_path").alias("source_file"),
        F.explode("records").alias("r"),
    )
    hour_end_local = F.to_timestamp("r.time_local_end", "yyyy-MM-dd HH:mm:ss")
    return (
        r.select(
            "source", "source_file",
            F.col("r.position_code").alias("position_code"),
            F.col("r.station_code").alias("station_code"),
            F.col("r.pollutant").alias("pollutant"),
            F.col("r.sensor_id").alias("sensor_id"),
            F.col("r.value").alias("value"),
            local_hour_end_to_utc_start_col(hour_end_local).alias("time_utc"),
            F.to_timestamp("r.sent_at").alias("sent_at"),
            F.to_timestamp("previous_fetched_at").alias("previous_fetched_at"),
        )
        .withColumn("lag_hours", (F.col("sent_at").cast("long") - (F.col("time_utc").cast("long") + 3600)) / 3600.0)
    )


def classify(batch: DataFrame, stored: DataFrame, late_seen: DataFrame, quarantine_seen: DataFrame,
             max_lag_hours: float) -> DataFrame:
    """Add ``status``: duplicate | correction | late | new (hard rules are applied afterwards).

    ``stored`` = silver keys with values, ``late_seen`` = keys already rejected as late (any re-send
    is a duplicate), ``quarantine_seen`` = quarantined keys with their values (a re-send with the
    same value is a duplicate; a new value — e.g. a filled-in empty hour — is judged again).
    """
    # Several polls can land in one micro-batch (a missed run, a re-clean after live-reset). A key is
    # judged by its FIRST arrival — a later poll re-sending it must not make it look late — but carries
    # the NEWEST value: GIOŚ fills in empty hours later, and occasionally withdraws a value.
    first = Window.partitionBy(*KEY).orderBy(F.col("sent_at").asc())
    newest_value = F.last("value").over(first.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    b = batch.withColumn("_rn", F.row_number().over(first)).withColumn("_newest", newest_value)
    in_batch_dup = b.where("_rn > 1").withColumn("status", F.lit("duplicate")).drop("_rn", "_newest")
    b = b.where("_rn = 1").withColumn("value", F.col("_newest")).drop("_rn", "_newest")

    s = stored.select(*KEY, F.col("value").alias("_stored_value"))
    late_keys = late_seen.select(*KEY).distinct().withColumn("_was_late", F.lit(True))
    # A quarantined key counts as a duplicate only if it comes back with the SAME value: GIOŚ first
    # publishes an empty value for recent hours and fills it in later — the filled value must be judged.
    quarantined = (quarantine_seen.select(*KEY, F.col("value").alias("_q_value"))
                   .groupBy(*KEY).agg(F.collect_list(F.coalesce(F.col("_q_value").cast("string"), F.lit("null")))
                                      .alias("_q_values")))
    b = b.join(s, KEY, "left").join(late_keys, KEY, "left").join(quarantined, KEY, "left")
    same_as_quarantined = F.array_contains(F.col("_q_values"),
                                           F.coalesce(F.col("value").cast("string"), F.lit("null")))
    due_before_previous_fetch = (
        F.col("previous_fetched_at").isNotNull()
        & ((F.col("time_utc") + F.expr(f"INTERVAL {int(max_lag_hours * 60)} MINUTES") + F.expr("INTERVAL 1 HOUR"))
           <= F.col("previous_fetched_at"))
    )
    status = (
        F.when(F.col("_stored_value").isNotNull() & F.col("_stored_value").eqNullSafe(F.col("value")), "duplicate")
        .when(F.col("_stored_value").isNotNull(), "correction")
        .when(F.col("_was_late").eqNullSafe(True), "duplicate")
        .when(F.coalesce(same_as_quarantined, F.lit(False)), "duplicate")
        .when((F.col("lag_hours") > max_lag_hours) & due_before_previous_fetch, "late")
        .otherwise("new")
    )
    return b.withColumn("status", status).drop("_stored_value", "_was_late", "_q_values").unionByName(in_batch_dup)


def flag_drift(df: DataFrame, dq: dict) -> DataFrame:
    """Drift = the station's recent residual vs the city median rises above its own earlier level."""
    d = dq["drift"]
    city_hour = Window.partitionBy("source", "city_id", "pollutant", "time_utc")
    df = (
        df.withColumn("_n_city", F.count("value").over(city_hour))
        .withColumn("city_median", F.percentile_approx("value", 0.5).over(city_hour))
        .withColumn("_resid", F.when(F.col("_n_city") >= d["min_city_stations"],
                                     F.log((F.col("value") + 1) / (F.col("city_median") + 1))))
    )
    t = F.col("time_utc").cast("long")
    series = Window.partitionBy("source", "station_code", "pollutant").orderBy(t)
    recent = series.rangeBetween(-(d["window_hours"] - 1) * 3600, 0)
    base = series.rangeBetween(-(d["window_hours"] + d["baseline_hours"] - 1) * 3600, -d["window_hours"] * 3600)
    return (
        df.withColumn("resid_recent", F.avg("_resid").over(recent))
        .withColumn("resid_baseline", F.avg("_resid").over(base))
        .withColumn("_n_base", F.count("_resid").over(base))
        .withColumn("is_drift", F.coalesce(
            (F.col("_n_base") >= d["min_baseline_hours"])
            & (F.col("resid_recent") - F.col("resid_baseline") > d["log_residual_increase"]), F.lit(False)))
        .drop("_n_city", "_resid", "_n_base")
    )


def recompute_flags(window_rows: DataFrame, dq: dict) -> DataFrame:
    """frozen / spike (shared with the archive cleaning) + drift on a city window of clean rows."""
    flagged = flag_drift(flag_patterns(window_rows, dq), dq)
    return (
        flagged.withColumn("dq_flag", F.when(F.col("is_frozen"), "frozen").when(F.col("is_spike"), "spike")
                           .when(F.col("is_drift"), "drift").otherwise("ok"))
        .withColumn("is_valid", F.col("dq_flag") == "ok")
        .withColumn("day_cet", day_cet_col(F.col("time_utc")))
        .withColumn("hour_cet", F.hour(F.col("time_utc") + F.expr("INTERVAL 1 HOUR")))
    )


SILVER_COLUMNS = [
    "source", "station_code", "city_id", "jurisdiction_code", "pollutant", "time_utc", "day_cet", "hour_cet",
    "value", "dq_flag", "is_valid", "run_length", "is_frozen", "is_spike", "is_drift", "city_median",
    "resid_recent", "resid_baseline", "sent_at", "lag_hours", "status",
]


def stream_station_city(station_city: DataFrame, registry_stations: DataFrame, cities: DataFrame) -> DataFrame:
    """Station → city for live data: archive metadata (historic stations) PLUS the current API
    registry (stations opened after the metadata file was published, e.g. Lublin ul. Okopowa)."""
    archive = station_city.where("code_kind = 'current'").select("station_code", "city_id", "jurisdiction_code")
    registry = (
        registry_stations.join(F.broadcast(cities), registry_stations.city_name == cities.gios_city_name)
        .select("station_code", "city_id", "jurisdiction_code")
    )
    return archive.unionByName(registry).dropDuplicates(["station_code"])


def split_new(classified: DataFrame, mapping: DataFrame, dq: dict) -> tuple[DataFrame, DataFrame, DataFrame]:
    """(accepted rows with city, late rows, quarantined rows incl. unknown stations).
    ``mapping``: station_code, city_id, jurisdiction_code (see ``stream_station_city``)."""
    late = classified.where("status = 'late'")
    candidates = classified.where("status IN ('new', 'correction')")
    known = candidates.join(F.broadcast(mapping), "station_code", "left")
    unknown = (known.where(F.col("city_id").isNull())
               .withColumn("failed_rules", F.array(F.lit("unknown_station")))
               .withColumn("quarantine_reason", F.lit("unknown_station")))
    ok, bad = split_hard_rules(known.where(F.col("city_id").isNotNull()), dq)
    return ok, late, bad.unionByName(unknown)
