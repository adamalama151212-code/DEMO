"""Meteo: kontrakt API (P21) i konwencje kierunku wiatru."""

import copy

import pytest
from pyspark.sql import functions as F

from radplume.ingest.meteo import (
    MeteoValidationError,
    build_params,
    synthetic_payload,
    validate_payload,
    year_chunks,
)
from radplume.silver.meteo import pasquill_class_idx, plume_bearing, wind_components


def test_request_forces_ms_and_utc():
    p = build_params(54.8, 17.8, "2020-01-01", "2020-01-02")
    assert p["wind_speed_unit"] == "ms"
    assert p["timezone"] == "GMT"


def test_kmh_payload_is_rejected():
    # Dokładnie to, co zwróciło API w ręcznym teście bez wind_speed_unit=ms.
    payload = synthetic_payload(54.8, 17.8, "2020-01-01", "2020-01-01", seed=1)
    bad = copy.deepcopy(payload)
    bad["hourly_units"]["wind_speed_10m"] = "km/h"
    with pytest.raises(MeteoValidationError, match="km/h"):
        validate_payload(bad)
    validate_payload(payload)  # poprawny przechodzi


def test_length_mismatch_is_rejected():
    payload = synthetic_payload(54.8, 17.8, "2020-01-01", "2020-01-01", seed=1)
    payload["hourly"]["precipitation"] = payload["hourly"]["precipitation"][:-1]
    with pytest.raises(MeteoValidationError):
        validate_payload(payload)


def test_synthetic_is_deterministic():
    a = synthetic_payload(54.8, 17.8, "2020-01-01", "2020-01-03", seed=7)
    b = synthetic_payload(54.8, 17.8, "2020-01-01", "2020-01-03", seed=7)
    assert a == b
    assert len(a["hourly"]["time"]) == 72


def test_year_chunks_cover_range():
    assert year_chunks("2019-06-01", "2021-02-01") == [
        ("2019-06-01", "2019-12-31"), ("2020-01-01", "2020-12-31"), ("2021-01-01", "2021-02-01"),
    ]


@pytest.mark.parametrize(
    "wind_from, plume_to, u_sign, v_sign",
    [
        (270, 90, 1, 0),    # wiatr z zachodu → smuga na wschód (u > 0)
        (0, 180, 0, -1),    # wiatr z północy → smuga na południe (v < 0)
        (311, 131, 1, -1),  # przypadek z testu API: z NW → na SE (Wejherowo/Gdynia)
    ],
)
def test_wind_direction_convention(spark, wind_from, plume_to, u_sign, v_sign):
    df = spark.createDataFrame([(5.0, float(wind_from))], "s DOUBLE, d DOUBLE")
    u, v = wind_components(F.col("s"), F.col("d"))
    r = df.select(plume_bearing(F.col("d")).alias("to"), u.alias("u"), v.alias("v")).first()
    assert r["to"] == pytest.approx(plume_to)
    for comp, sign in ((r["u"], u_sign), (r["v"], v_sign)):
        if sign == 0:
            assert abs(comp) < 1e-9
        else:
            assert comp * sign > 0


@pytest.mark.parametrize(
    "wind, rad, cloud, expected",
    [
        (1.5, 800, 10, "A"),   # słoneczny, bezwietrzny dzień → bardzo niestabilnie
        (7.0, 800, 10, "C"),   # silny wiatr w dzień → słabo niestabilnie
        (1.5, 0, 10, "F"),     # bezchmurna, bezwietrzna noc → bardzo stabilnie
        (1.5, 0, 70, "E"),     # pochmurna noc → stabilnie
        (4.0, 500, 95, "D"),   # pełne zachmurzenie → neutralnie
    ],
)
def test_pasquill_classes(spark, wind, rad, cloud, expected):
    df = spark.createDataFrame([(wind, float(rad), float(cloud))], "w DOUBLE, r DOUBLE, c DOUBLE")
    idx = df.select(pasquill_class_idx(F.col("w"), F.col("r"), F.col("c")).alias("i")).first()["i"]
    assert "ABCDEF"[idx] == expected
