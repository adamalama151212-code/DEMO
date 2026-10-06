"""Stream cleaning: record classification, drift vs a real episode, and an end-to-end replay
through Structured Streaming (late, duplicate batch, quarantine, spike, idempotent re-run)."""

import datetime as dt
import math

from pyspark.sql import functions as F

from smogcast.core.config import load_config
from smogcast.ingest.fault_replay import build_batches
from smogcast.ingest.gios_live import write_batch
from smogcast.silver_clean.pm_stream import classify, flag_drift
from smogcast.silver_clean.steps import step_pm_stream

DQ = load_config("local")["pm_dq"]
T0 = dt.datetime(2025, 1, 17)
H = dt.timedelta(hours=1)
ROW = ("source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, value DOUBLE, "
       "sent_at TIMESTAMP, previous_fetched_at TIMESTAMP, lag_hours DOUBLE")
KEYS = "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, value DOUBLE"


def test_classify_duplicate_correction_late_backfill(spark):
    prev = T0 + 10 * H
    rows = [
        ("live", "A", "PM10", T0 + H, 10.0, T0 + 12 * H, prev, 10.0),      # stored with 10 -> duplicate
        ("live", "A", "PM10", T0 + 2 * H, 99.0, T0 + 12 * H, prev, 9.0),   # stored with 20 -> correction
        ("live", "B", "PM10", T0 + 3 * H, 5.0, T0 + 12 * H, prev, 8.0),    # due before previous fetch -> late
        ("live", "B", "PM10", T0 + 9 * H, 5.0, T0 + 12 * H, prev, 2.0),    # fresh -> new
        ("live", "C", "PM10", T0 + 3 * H, 5.0, T0 + 12 * H, None, 8.0),    # first fetch ever -> backfill, new
    ]
    stored = spark.createDataFrame([("live", "A", "PM10", T0 + H, 10.0), ("live", "A", "PM10", T0 + 2 * H, 20.0)], KEYS)
    empty = spark.createDataFrame([], KEYS)
    out = {(r["station_code"], r["time_utc"].hour): r["status"]
           for r in classify(spark.createDataFrame(rows, ROW), stored, empty, empty, 3).collect()}
    assert out == {("A", 1): "duplicate", ("A", 2): "correction", ("B", 3): "late", ("B", 9): "new", ("C", 3): "new"}


def test_filled_in_empty_value_is_judged_again(spark):
    rows = [("live", "A", "PM10", T0 + H, 33.0, T0 + 5 * H, T0 + 4 * H, 3.0),    # was quarantined as empty
            ("live", "B", "PM10", T0 + H, None, T0 + 5 * H, T0 + 4 * H, 3.0)]    # still empty -> duplicate
    quarantined = spark.createDataFrame([("live", "A", "PM10", T0 + H, None), ("live", "B", "PM10", T0 + H, None)], KEYS)
    empty = spark.createDataFrame([], KEYS)
    out = {r["station_code"]: r["status"]
           for r in classify(spark.createDataFrame(rows, ROW), empty, empty, quarantined, 3).collect()}
    assert out == {"A": "new", "B": "duplicate"}


def test_two_polls_in_one_batch_are_judged_by_the_first_arrival(spark):
    # Poll 1 (first ever, backfill) and poll 2 re-sending the same hours land in ONE micro-batch.
    p1, p2 = T0 + 10 * H, T0 + 15 * H
    rows = [("live", "A", "PM10", T0 + H, None, p1, None, 9.0),      # empty in poll 1 ...
            ("live", "A", "PM10", T0 + H, 30.0, p2, p1, 14.0),       # ... filled in by poll 2
            ("live", "B", "PM10", T0 + H, 12.0, p1, None, 9.0),
            ("live", "B", "PM10", T0 + H, 12.0, p2, p1, 14.0)]
    empty = spark.createDataFrame([], KEYS)
    out = classify(spark.createDataFrame(rows, ROW), empty, empty, empty, 3).collect()
    first = {r["station_code"]: r for r in out if r["status"] != "duplicate"}
    assert {k: r["status"] for k, r in first.items()} == {"A": "new", "B": "new"}   # not "late"
    assert first["A"]["value"] == 30.0 and first["A"]["lag_hours"] == 9.0
    assert sum(r["status"] == "duplicate" for r in out) == 2


def drift_frame(spark, episode=False, drift=False):
    rows = []
    for i, st in enumerate("ABCD"):
        for h in range(130):
            v = 20.0 + i * 5                                   # D is always higher (traffic station)
            if episode and 70 <= h < 100:
                v *= 5                                         # all stations rise together: real smog
            if drift and st == "D" and h >= 60:
                v *= math.pow(1.05, h - 59)                    # only D creeps up: sensor drift
            rows.append(("live", st, "krakow", "PM10", T0 + h * H, v))
    df = spark.createDataFrame(rows, "source STRING, station_code STRING, city_id STRING, pollutant STRING, "
                                     "time_utc TIMESTAMP, value DOUBLE")
    return {(r["station_code"], int((r["time_utc"] - T0) / H)): r["is_drift"] for r in flag_drift(df, DQ).collect()}


def test_drift_detected_only_on_the_drifting_station(spark):
    f = drift_frame(spark, drift=True)
    assert any(f[("D", h)] for h in range(80, 130))
    assert not any(f[(s, h)] for s in "ABC" for h in range(130))
    assert not any(f[("D", h)] for h in range(60))           # constantly higher is not drift


def test_real_smog_episode_is_not_drift(spark):
    f = drift_frame(spark, episode=True)
    assert not any(f.values())


def test_stream_end_to_end_with_fault_replay(ctx):
    sc = ctx.spark.createDataFrame(
        [(c, "krakow", "PL-12", c, "current") for c in "ABCD"],
        "station_code STRING, city_id STRING, jurisdiction_code STRING, current_station_code STRING, code_kind STRING")
    ctx.storage.overwrite(sc, "silver", "station_city")
    values = {(s, "PM10", T0 + h * H): 20.0 + i + (h % 3) for i, s in enumerate("ABCD") for h in range(24)}
    schedule = [
        {"type": "outage", "station_index": 0, "hour": 2, "hours": 8},     # A: hours 2..9 sent at hour 9
        {"type": "duplicate_batch", "hour": 12},
        {"type": "negative", "station_index": 1, "hour": 14},
        {"type": "missing_value", "station_index": 2, "hour": 15},
        {"type": "spike", "station_index": 3, "hour": 18, "factor": 8.0},
    ]
    out = ctx.storage.landing("fault_replay")
    for b in build_batches(values, list("ABCD"), T0, 24, schedule, 20):
        write_batch(out, "fault_replay", b.records, b.fetched_at, b.previous_fetched_at, name=b.name)

    step_pm_stream(ctx)
    st = ctx.storage
    m = {r["metric"]: r["value"] for r in st.read("ops", "dq_metrics").where("step = 'silver_pm_stream_fault_replay'").collect()}
    assert (m["accepted"], m["late_rejected"], m["quarantined"]) == (90, 4, 2)   # counted BEFORE the merge
    assert m["duplicates"] >= 4                                                  # the re-sent batch
    late = st.read("ops", "pm_late_rejected").collect()
    # A's backlog for hours 2..9 is sent after hour 9: hours 2..5 have lag > 3 h and were due earlier
    assert sorted(int((r["time_utc"] - T0) / H) for r in late) == [2, 3, 4, 5]
    reasons = sorted(r["quarantine_reason"] for r in st.read("ops", "pm_quarantine").collect())
    assert reasons == ["below_min", "missing_value"]
    silver = st.read("silver", "pm_stream")
    assert silver.count() == 4 * 24 - 4 - 2                      # resent batch added nothing
    assert silver.where("station_code = 'D' AND dq_flag = 'spike'").count() == 1

    bronze_rows = st.read("bronze", "pm_stream").count()
    step_pm_stream(ctx)                                          # nothing new: checkpoint remembers the files
    assert st.read("bronze", "pm_stream").count() == bronze_rows
    assert st.read("silver", "pm_stream").count() == silver.count()
    assert st.read("silver", "pm_stream").where(F.col("source") != "fault_replay").count() == 0

    # Reset and clean the same files again: batch ids restart at 0, yet nothing may be skipped.
    from smogcast.silver_clean.steps import _reset_stream_source

    tables = [("bronze", "pm_stream"), ("ops", "pm_late_rejected"), ("ops", "pm_quarantine"), ("silver", "pm_stream")]
    before = {t: st.read(*t).count() for t in tables}
    _reset_stream_source(ctx, "fault_replay")
    step_pm_stream(ctx)
    assert {t: st.read(*t).count() for t in tables} == before


def test_live_reset_removes_only_live_state(ctx):
    from pathlib import Path

    from smogcast.silver_clean.steps import step_live_reset

    st = ctx.storage
    rows = ctx.spark.createDataFrame([(s, "A", "PM10", T0, 10.0) for s in ("live", "fault_replay")], KEYS)
    for layer, name in (("bronze", "pm_stream"), ("ops", "pm_quarantine"), ("silver", "pm_stream")):
        st.overwrite(rows, layer, name)
    Path(st.checkpoint("pm_stream_live")).mkdir(parents=True)
    Path(st.landing("gios_live")).mkdir(parents=True)

    step_live_reset(ctx)
    for layer, name in (("bronze", "pm_stream"), ("ops", "pm_quarantine"), ("silver", "pm_stream")):
        assert [r["source"] for r in st.read(layer, name).collect()] == ["fault_replay"]
    assert not Path(st.checkpoint("pm_stream_live")).exists()
    assert Path(st.landing("gios_live")).exists()          # polls cannot be repeated — keep them
