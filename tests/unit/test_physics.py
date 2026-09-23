"""Fizyka na pojedynczych wierszach — wartości analityczne i bilans masy (plan etap 5).

Etapu 5 nie wolno pominąć: błąd fizyki wykryty dopiero przy walidacji = przeliczenie wszystkiego.
"""

import math

import pytest
from pyspark.sql import functions as F

from radplume.silver import dispersion as disp

D = 3  # klasa Pasquilla D (neutralna)


def _one(spark, **cols):
    names = list(cols)
    return spark.createDataFrame([tuple(float(cols[n]) for n in names)], ", ".join(f"{n} DOUBLE" for n in names))


def test_briggs_sigmas_class_d(spark):
    df = _one(spark, x=1000)
    sy, sz = disp.briggs_sigmas(F.col("x"), F.lit(D))
    r = df.select(sy.alias("sy"), sz.alias("sz")).first()
    assert r["sy"] == pytest.approx(0.08 * 1000 / math.sqrt(1.1))
    assert r["sz"] == pytest.approx(0.06 * 1000 / math.sqrt(2.5))


def test_centerline_ground_concentration_matches_formula(spark):
    u, H, sy, sz = 5.0, 50.0, 76.3, 37.9
    df = _one(spark, u=u, y=0, H=H, sy=sy, sz=sz)
    chi = df.select(disp.ground_concentration_per_rate(F.col("u"), F.col("y"), F.col("H"), F.col("sy"), F.col("sz"))).first()[0]
    expected = math.exp(-(H**2) / (2 * sz**2)) / (math.pi * sy * sz * u)
    assert chi == pytest.approx(expected, rel=1e-9)


def test_lateral_point_is_gaussian(spark):
    # W odległości 1σy od osi stężenie spada do exp(-1/2) wartości na osi.
    rows = [(5.0, y, 50.0, 76.3, 37.9) for y in (0.0, 76.3)]
    df = spark.createDataFrame(rows, "u DOUBLE, y DOUBLE, H DOUBLE, sy DOUBLE, sz DOUBLE")
    chi = [
        r[0]
        for r in df.orderBy("y").select(
            disp.ground_concentration_per_rate(F.col("u"), F.col("y"), F.col("H"), F.col("sy"), F.col("sz"))
        ).collect()
    ]
    assert chi[1] / chi[0] == pytest.approx(math.exp(-0.5), rel=1e-9)


def test_mass_balance(spark):
    """∫∫ u·χ/q dy dz po przekroju poprzecznym = 1: cała emisja przepływa przez przekrój.

    Liczymy całkę numerycznie w Sparku (siatka 400 × 400 punktów) — ten sam wzór,
    którego używa potok, bez żadnej kopii w Pythonie.
    """
    u, H, sy, sz = 5.0, 50.0, 76.3, 37.9
    n = 400
    y_min, y_max = -6 * sy, 6 * sy
    z_max = H + 8 * sz
    dy, dz = (y_max - y_min) / n, z_max / n
    grid = spark.range(n * n).select(
        (F.lit(y_min) + ((F.col("id") % n) + 0.5) * dy).alias("y"),
        ((F.floor(F.col("id") / n) + 0.5) * dz).alias("z"),
    )
    chi = disp.concentration_per_rate(F.lit(u), F.col("y"), F.col("z"), F.lit(H), F.lit(sy), F.lit(sz))
    total = grid.select(F.sum(chi * u * dy * dz)).first()[0]
    assert total == pytest.approx(1.0, rel=5e-3)


def test_column_integral_matches_vertical_integral(spark):
    """column_per_rate (używany do depozycji mokrej) = ∫ χ/q dz liczone numerycznie."""
    u, H, sy, sz, y = 5.0, 50.0, 76.3, 37.9, 30.0
    n, z_max = 4000, H + 10 * sz
    dz = z_max / n
    zs = spark.range(n).select(((F.col("id") + 0.5) * dz).alias("z"))
    num = zs.select(F.sum(disp.concentration_per_rate(F.lit(u), F.lit(y), F.col("z"), F.lit(H), F.lit(sy), F.lit(sz)) * dz)).first()[0]
    col = _one(spark, x=0).select(disp.column_per_rate(F.lit(u), F.lit(y), F.lit(sy))).first()[0]
    assert num == pytest.approx(col, rel=1e-3)


def test_wind_profile_is_clipped_to_minimum(spark):
    exps = {c: 0.15 for c in "ABCDEF"}
    r = _one(spark, u=0.1, h=10).select(disp.wind_at_height(F.col("u"), F.col("h"), F.lit(D), exps, 0.5)).first()[0]
    assert r == pytest.approx(0.5)


def test_plume_goes_downwind_only(spark, cfg):
    """Wiatr z zachodu (270°) → depozycja tylko na wschód od źródła (x_km > 0)."""
    import datetime as dt

    t0 = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    meteo = spark.createDataFrame(
        [("s", t0, 5.0, 270.0, 90.0, 0.0, D)],
        "site_id STRING, time_utc TIMESTAMP, wind_speed_ms DOUBLE, wind_from_deg DOUBLE, plume_to_deg DOUBLE, precip_mm_h DOUBLE, stability_idx INT",
    )
    scenarios = spark.createDataFrame(
        [("c", "s", 0, t0, 1, 0, 0, 1.0, 1.0, 50.0)],
        "scenario_set STRING, site_id STRING, episode_id INT, episode_start TIMESTAMP, episode_hours INT, variant_id INT, stability_shift INT, vd_mult DOUBLE, washout_mult DOUBLE, release_height_m DOUBLE",
    )
    grid = spark.createDataFrame(
        [("s", f"c{x}_{y}", float(x), float(y)) for x in (-10, 10) for y in (-10, 0, 10)],
        "site_id STRING, cell_id STRING, x_km DOUBLE, y_km DOUBLE",
    )
    out = disp.hourly_unit_deposition(meteo, scenarios, grid, disp.nuclides_df(spark, cfg["physics"]), cfg["physics"])
    cells = {r["cell_id"] for r in out.where("dep_hour > 0").select("cell_id").collect()}
    assert cells == {"c10_0"}  # na wschód i na osi smugi; ±10 km w poprzek to > 4σy przy 10 km


def test_decay_reduces_iodine_more_than_cesium(cfg):
    # I-131 (8 dni) rozpada się szybciej niż Cs-137 (30 lat) — sprawdzenie stałych z configu.
    n = cfg["physics"]["nuclides"]
    assert n["I-131"]["half_life_s"] < n["Cs-137"]["half_life_s"] / 1000
