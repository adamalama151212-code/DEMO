"""Model obłoków „puff” (plan rozszerzeń, etap 3)."""

import copy
import datetime as dt
import math

import pytest
from pyspark.sql import functions as F

from radplume.silver import dispersion as disp
from radplume.silver.grid import build_grid, sites_df
from radplume.silver.puff import norm_cdf, puff_unit_deposition, trajectories
from tests.unit.test_physics import METEO_SCHEMA, single_scenario

T0 = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
D = 3


def _meteo(spark, winds, speed=5.0, hours=24):
    """Pogoda godzinowa: ``winds`` = lista kierunków „skąd” dla kolejnych godzin (ostatni się powtarza)."""
    rows = [("s", T0 + dt.timedelta(hours=h), speed, float(winds[min(h, len(winds) - 1)]), 0.0, D) for h in range(hours)]
    return spark.createDataFrame(rows, METEO_SCHEMA)


def _grid(spark, size=41, cell_km=5.0):
    sites = sites_df(spark, {"s": {"site_id": "s", "name": "s", "country_code": "XX", "jurisdiction_code": "XX",
                                   "lat": 50.0, "lon": 18.0}})
    return build_grid(sites, size, cell_km)


def _per_cell(df, nuclide="Cs-137"):
    rows = df.where(F.col("nuclide") == nuclide).groupBy("cell_id").agg(F.sum("dep_hour").alias("d")).collect()
    return {r["cell_id"]: r["d"] for r in rows}


def test_norm_cdf_matches_math(spark):
    zs = [-4.0, -1.5, -0.3, 0.0, 0.7, 2.2, 5.0]
    df = spark.createDataFrame([(z,) for z in zs], "z DOUBLE")
    got = [r[0] for r in df.select(norm_cdf(F.col("z"))).collect()]
    for z, g in zip(zs, got, strict=True):
        assert g == pytest.approx(0.5 * (1 + math.erf(z / math.sqrt(2))), abs=2e-7)


def test_constant_wind_matches_straight_model(spark, cfg):
    """Przy stałym wietrze model obłoków musi dać to samo co prosta smuga.

    Suma ΔΦ po kolejnych odcinkach trajektorii „skleja” się w pełne przejście chmury,
    a σ liczone w punkcie odcinka najbliższym komórce odtwarza σ(x) prostej smugi.
    Zmierzona różnica < 1%; tolerancja 5% zostawia zapas na przybliżenie erf i kroki czasu.
    """
    meteo = _meteo(spark, [270.0])  # z zachodu → na wschód
    scenarios, schedule = single_scenario(spark, T0, hours=1)
    grid = _grid(spark)
    nucl = disp.nuclides_df(spark, cfg["physics"])
    straight = _per_cell(disp.hourly_unit_deposition(meteo, scenarios, schedule, grid, nucl, cfg["physics"]))
    puff = _per_cell(puff_unit_deposition(meteo, scenarios, schedule, grid, nucl, cfg["physics"]))
    # komórki na osi smugi, 5–100 km na wschód (ix = 21 … 40, iy = 20) i tuż obok osi
    for cid in [f"s_{ix}_20" for ix in range(21, 41)] + ["s_30_21"]:
        assert puff[cid] == pytest.approx(straight[cid], rel=0.05), cid


def test_turning_wind_reaches_where_straight_plume_cannot(spark, cfg):
    """Pierwsza godzina: wiatr z zachodu (chmura na wschód), potem z południa (chmura na północ).

    Chmura z pierwszej godziny skręca na północ ok. 9 km na wschód od źródła, więc dociera
    nad komórkę (10 km E, 20 km N). Prosta smuga z tej godziny leci tylko na wschód.
    """
    meteo = _meteo(spark, [270.0, 180.0])
    scenarios, schedule = single_scenario(spark, T0, hours=1)
    grid = _grid(spark)
    nucl = disp.nuclides_df(spark, cfg["physics"])
    target = "s_22_24"  # x = +10 km, y = +20 km
    straight = _per_cell(disp.hourly_unit_deposition(meteo, scenarios, schedule, grid, nucl, cfg["physics"]))
    puff = _per_cell(puff_unit_deposition(meteo, scenarios, schedule, grid, nucl, cfg["physics"]))
    assert straight.get(target, 0.0) == 0.0
    assert puff.get(target, 0.0) > 0.0
    # a daleko na wschodzie (60 km) chmura z pierwszej godziny już nie dociera
    assert puff.get("s_32_20", 0.0) < 0.01 * straight["s_32_20"]


def test_trajectory_stops_when_weather_is_missing(spark, cfg):
    meteo = _meteo(spark, [270.0], hours=2)  # pogoda tylko na 2 godziny
    scenarios, schedule = single_scenario(spark, T0, hours=1)
    seg = trajectories(meteo, scenarios, schedule, cfg["physics"])
    # start 00:30, pogoda do 02:00 → 1,5 h = 6 kroków po 15 min
    assert seg.count() == 6


def test_sigma_never_decreases(spark, cfg):
    # zmiana z klasy D na F w połowie drogi nie może „ścisnąć” chmury
    rows = [("s", T0 + dt.timedelta(hours=h), 5.0, 270.0, 0.0, 3 if h < 2 else 5) for h in range(12)]
    meteo = spark.createDataFrame(rows, METEO_SCHEMA)
    scenarios, schedule = single_scenario(spark, T0, hours=1)
    sig = [r["sigma_y"] for r in trajectories(meteo, scenarios, schedule, cfg["physics"]).orderBy("k").collect()]
    assert all(b >= a for a, b in zip(sig, sig[1:], strict=False))


def test_release_schedule_only_counts_release_hours(spark, cfg):
    """Dwa zrzuty (godzina 0 i 3) → obłoki tylko z tych godzin."""
    meteo = _meteo(spark, [270.0])
    scenarios, schedule = single_scenario(spark, T0, hours=4, release_hours=[0, 3])
    phys = copy.deepcopy(cfg["physics"])
    out = puff_unit_deposition(meteo, scenarios, schedule, _grid(spark), disp.nuclides_df(spark, phys), phys)
    assert {r["h"] for r in out.select("h").distinct().collect()} == {0, 3}
