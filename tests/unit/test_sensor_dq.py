"""Jakość danych czujników na zasymulowanych, deterministycznych przypadkach (plan 3.5.3 krok 4).

Testy wołają czyste funkcje na STATYCZNYCH DataFrame'ach. Działa to tylko dlatego,
że spóźnienie liczymy z ``sent_at`` (a nie watermarkiem) — wynik nie zależy
od podziału strumienia na mikro-batche.
"""

import datetime as dt
import math
import random

import pytest
from pyspark.sql import functions as F

from radplume.silver.sensor_clean import (
    SENSOR_SCHEMA,
    apply_hard_rules,
    dedup,
    hard_rules,
    split_late,
)
from radplume.silver.sensor_quality import build_sensor_quality

T0 = dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc)


def _readings(spark, rows):
    return spark.createDataFrame(rows, SENSOR_SCHEMA)


def _r(device, minute, lag_min=0, dose=1.0, wind=5.0, battery=80.0, temp=None):
    t = T0 + dt.timedelta(minutes=minute)
    return (device, t, t + dt.timedelta(minutes=lag_min), dose, wind, battery, temp)


def test_lag_10_min_accepted_20_min_rejected(spark, cfg):
    df = _readings(spark, [_r("d1", 0, lag_min=10), _r("d1", 1, lag_min=20)])
    on_time, late = split_late(df, cfg["sensor_dq"]["late_arrival"]["max_lag_minutes"])
    assert [r["lag_minutes"] for r in on_time.collect()] == [10.0]
    assert [r["lag_minutes"] for r in late.collect()] == [20.0]


def test_duplicate_reading_kept_once(spark, cfg):
    df = _readings(spark, [_r("d1", 0), _r("d1", 0, lag_min=1)])
    out = dedup(df, 15, cfg["sensor_dq"]["dedup"]["keys"])
    assert out.count() == 1


@pytest.mark.parametrize(
    "row, reason",
    [
        (_r("d1", 0, wind=80.0), "wind_physical"),
        (_r("d1", 0, dose=12000.0), "dose_in_detector_range"),
        (_r("d1", 0, dose=None), "no_null_when_powered"),
    ],
)
def test_hard_rules_quarantine_with_reason(spark, cfg, row, reason):
    df = _readings(spark, [row, _r("d2", 0)]).withColumn("cell_id", F.lit("c"))
    clean, quarantine = apply_hard_rules(df, hard_rules(cfg["sensor_dq"]))
    assert [r["device_id"] for r in clean.collect()] == ["d2"]
    q = quarantine.collect()
    assert len(q) == 1 and reason in q[0]["quarantine_reason"]


# ------------------------------------------------------------------ reguły historyczne
def _clean_frame(spark, series: dict[str, list[float]], positions: dict[str, tuple[float, float]]):
    """Odczyty „po czyszczeniu” (z kolumnami z rejestru urządzeń)."""
    rows = []
    for dev, values in series.items():
        x, y = positions[dev]
        for m, v in enumerate(values):
            t = T0 + dt.timedelta(minutes=m)
            rows.append((dev, t, t, v, 5.0, 80.0, None, 0.0, "s", "cell", "JP-07", "v1", float(x), float(y), 0.0, 0.0))
    return spark.createDataFrame(
        rows,
        SENSOR_SCHEMA + ", lag_minutes DOUBLE, site_id STRING, cell_id STRING, jurisdiction_code STRING,"
        " firmware STRING, x_km DOUBLE, y_km DOUBLE, lat DOUBLE, lon DOUBLE",
    )


def _expected(spark, model_dose):
    rows = [("cell", T0 + dt.timedelta(hours=h), model_dose) for h in range(-1, 5)]
    return spark.createDataFrame(rows, "cell_id STRING, hour_ts TIMESTAMP, model_dose_usv_h DOUBLE")


def _noisy(n, level, rng, sigma=0.35):
    return [round(level * rng.lognormvariate(0, sigma), 4) for _ in range(n)]


def _status(df, device, minute):
    t = T0 + dt.timedelta(minutes=minute)
    return df.where(f"device_id = '{device}'").where(df.event_time == t).first()["quality_status"]


def test_single_device_drift_is_suspect_but_area_deviation_is_signal(spark, cfg):
    """P18: dryf jednego czujnika = usterka; to samo odchylenie u wszystkich sąsiadów = realny sygnał."""
    rng = random.Random(1)
    model, n = 10.0, 150
    base = {d: _noisy(n, model + 0.05, rng) for d in ("a", "b", "c", "far")}
    # „far”: dryf +1%/min od 30. minuty (osobno, bez sąsiadów)
    base["far"] = [v * (1 + 0.01 * max(0, m - 30)) for m, v in enumerate(base["far"])]
    # a, b, c: wszyscy razem ×5 od 60. minuty — obszarowe odchylenie od modelu
    for d in ("a", "b", "c"):
        base[d] = [v * (5 if m >= 60 else 1) for m, v in enumerate(base[d])]
    pos = {"a": (0, 0), "b": (1, 0), "c": (0, 1), "far": (30, 30)}

    q = build_sensor_quality(_clean_frame(spark, base, pos), _expected(spark, model), cfg["sensor_dq"], 0.05).cache()
    assert _status(q, "far", 140) == "drift_suspect"
    assert _status(q, "a", 120) == "clean"
    assert q.where("device_id = 'a' AND is_plume_signal").count() > 30


def test_frozen_reading_flagged(spark, cfg):
    rng = random.Random(2)
    values = _noisy(30, 5.0, rng)
    values[10:22] = [values[10]] * 12   # ta sama wartość 12 razy z rzędu
    q = build_sensor_quality(_clean_frame(spark, {"f": values}, {"f": (0, 0)}), _expected(spark, 5.0), cfg["sensor_dq"], 0.05)
    assert _status(q, "f", 21) == "frozen"
    assert _status(q, "f", 25) != "frozen"


def test_background_sensor_is_not_frozen(spark, cfg):
    """P17: czujnik poza smugą (model = 0) z szumem tła nie może wyglądać na zamrożony."""
    rng = random.Random(3)
    values = _noisy(40, 0.05, rng, sigma=0.15)
    q = build_sensor_quality(_clean_frame(spark, {"bg": values}, {"bg": (0, 0)}), _expected(spark, 0.0), cfg["sensor_dq"], 0.05)
    assert q.where("quality_status = 'frozen'").count() == 0


def test_warmup_first_10_minutes(spark, cfg):
    rng = random.Random(4)
    q = build_sensor_quality(
        _clean_frame(spark, {"n": _noisy(20, 1.0, rng)}, {"n": (0, 0)}), _expected(spark, 1.0), cfg["sensor_dq"], 0.05
    )
    assert _status(q, "n", 0) == "warmup"
    assert _status(q, "n", 9) == "warmup"
    assert _status(q, "n", 10) != "warmup"


def test_residual_is_zero_mean_without_drift(spark, cfg):
    rng = random.Random(5)
    q = build_sensor_quality(
        _clean_frame(spark, {"z": _noisy(200, 20.0, rng)}, {"z": (0, 0)}), _expected(spark, 20.0), cfg["sensor_dq"], 0.05
    )
    mean_res = q.agg({"residual": "avg"}).first()[0]
    # E[ln(lognormal(0, σ))] = 0 → średnia reszta ~0 (tu tolerancja ~4 błędy standardowe)
    assert abs(mean_res) < 4 * 0.35 / math.sqrt(200)
