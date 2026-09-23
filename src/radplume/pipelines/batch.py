"""Ścieżka A — BATCH: od danych meteo do odpowiedzi „czy miasto Y zostanie skażone”.

Każdy krok to funkcja ``(ctx) -> None``, która czyta tabele wejściowe
i zapisuje wyjściowe. Kroki są niezależnie uruchamialne i idempotentne,
więc na Databricks każdy może być osobnym taskiem Lakeflow Joba
(plan 4.5), a lokalnie CLI woła je po kolei.

Kolejność:
    ingest_meteo → ingest_cities → bronze → silver_base → dispersion
    → gold → validation (jeśli są pomiary) → expected_dose (dla czujników)
"""

from __future__ import annotations

import glob
import logging
import os

from pyspark.sql import functions as F

from radplume.bronze.cities import read_cities
from radplume.bronze.meteo import BRONZE_METEO_KEYS, read_landing_meteo
from radplume.core.config import active_sites
from radplume.core.dq_metrics import log_metrics
from radplume.gold import aggregates as agg
from radplume.ingest.cities import ingest_cities
from radplume.ingest.meteo import ingest_meteo
from radplume.pipelines.context import Context
from radplume.silver import dispersion as disp
from radplume.silver.grid import assign_points_to_cells, build_grid, sites_df
from radplume.silver.meteo import build_silver_meteo
from radplume.silver.scenarios import build_scenarios
from radplume.validation.metrics import read_measurements, validation_points, validation_summary

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------------- ingest
def step_ingest_meteo(ctx: Context) -> None:
    files = ingest_meteo(ctx.cfg, ctx.storage.landing())
    log.info("ingest_meteo: nowych plików %d", len(files))


def step_ingest_cities(ctx: Context) -> None:
    ingest_cities(ctx.cfg, ctx.storage.landing())


# ----------------------------------------------------------------------------- bronze
def step_bronze(ctx: Context) -> None:
    st = ctx.storage
    meteo = read_landing_meteo(ctx.spark, st.landing("meteo"))
    # MERGE po (site_id, time_utc): ponowne wczytanie tych samych plików nie dubluje godzin.
    st.merge(meteo, "bronze", "meteo", BRONZE_METEO_KEYS)

    city_files = glob.glob(st.landing("cities", "cities5000.txt")) if ctx.cfg["sources"]["cities"] == "geonames" \
        else glob.glob(st.landing("cities", "cities_fallback.csv"))
    if not city_files:
        raise FileNotFoundError("Brak pliku miast w landing — uruchom najpierw ingest-cities")
    cities = read_cities(ctx.spark, city_files[0])
    st.overwrite(cities, "bronze", "cities")


# ----------------------------------------------------------------------------- silver
def step_silver_base(ctx: Context) -> None:
    """Meteo po kontroli jakości, siatka, miasta w zasięgu, scenariusze MC."""
    st, cfg, run = ctx.storage, ctx.cfg, ctx.run
    sites = sites_df(ctx.spark, active_sites(cfg))

    meteo, metrics = build_silver_meteo(st.read("bronze", "meteo").where(F.col("site_id").isin(run["sites"])))
    st.merge(meteo, "silver", "meteo", ["site_id", "time_utc"])
    log_metrics(st, "silver_meteo", metrics)
    if metrics["rows_grid_elevation_le_0"]:
        # P21: lokalizacja lądowa z wysokością ≤ 0 = podejrzane współrzędne (linia brzegowa).
        # Ostrzeżenie (expect), nie odrzucenie — dane pogodowe są poprawne.
        log.warning("silver_meteo: %d godzin z grid_elevation ≤ 0 — sprawdź współrzędne lokalizacji", metrics["rows_grid_elevation_le_0"])

    grid = build_grid(sites, run["grid_size"], run["cell_km"])
    st.overwrite(grid, "silver", "grid")

    countries = sorted({cfg["sites"][s]["country_code"] for s in run["sites"]})
    cities = (
        st.read("bronze", "cities")
        .where(F.col("country_code").isin(countries) & (F.col("population") > 0))
        .select("city_id", F.col("name").alias("city_name"), "ascii_name", "country_code", "lat", "lon", "population",
                F.col("source").alias("city_source"))
    )
    city_cells = assign_points_to_cells(
        cities, sites.drop("country_code"), run["grid_size"], run["cell_km"], cfg["city_radius_km"]
    )
    st.overwrite(city_cells, "silver", "city_cells")

    scenarios, q_samples = build_scenarios(ctx.spark, cfg)
    st.overwrite(scenarios, "silver", "scenarios")
    st.overwrite(q_samples, "silver", "q_samples")


def _q_median(ctx: Context):
    rows = [
        (sid, nuc, float(st["median_bq"]))
        for sid in ctx.run["sites"]
        for nuc, st in ctx.cfg["sites"][sid]["source_term"].items()
    ]
    return ctx.spark.createDataFrame(rows, "site_id STRING, nuclide STRING, q_median_bq DOUBLE")


def step_dispersion(ctx: Context) -> None:
    """Liczy smugi dla wszystkich scenariuszy, lokalizacja po lokalizacji.

    Pętla po LOKALIZACJACH (kilka), nie po danych: każda lokalizacja to osobny,
    atomowy zapis ``replaceWhere site_id = …``. Awaria w połowie nie zostawia
    tabeli w stanie mieszanym, a ponowne uruchomienie nadpisuje dokładnie ten wycinek.
    """
    st, cfg = ctx.storage, ctx.cfg
    meteo = st.read("silver", "meteo")
    grid = st.read("silver", "grid")
    scenarios = st.read("silver", "scenarios")
    nuclides = disp.nuclides_df(ctx.spark, cfg["physics"])
    q_median = _q_median(ctx)

    # Kontrola DQ: ile godzin epizodów nie ma danych meteo (dziura w danych = mniej obliczeń).
    needed = disp.episode_hours(scenarios).select("site_id", "time_utc").distinct()
    missing = needed.join(meteo.select("site_id", "time_utc"), ["site_id", "time_utc"], "left_anti").count()
    log_metrics(st, "dispersion", {"episode_hours_missing_meteo": missing})

    for site_id in ctx.run["sites"]:
        sc = scenarios.where(F.col("site_id") == site_id)
        hourly = disp.hourly_unit_deposition(meteo, sc, grid, nuclides, cfg["physics"])
        episodes = disp.aggregate_episodes(hourly, q_median, cfg["physics"]["arrival_threshold_bq_m2"])
        st.overwrite(
            episodes, "silver", "dispersion_episode",
            partition_by=["site_id"], replace_where=f"site_id = '{site_id}'",
        )


# ----------------------------------------------------------------------------- gold
def step_gold(ctx: Context) -> None:
    st, cfg = ctx.storage, ctx.cfg
    sites = sites_df(ctx.spark, active_sites(cfg))
    episodes = st.read("silver", "dispersion_episode").where(F.col("site_id").isin(ctx.run["sites"]))
    scenarios = st.read("silver", "scenarios")
    q_samples = st.read("silver", "q_samples")
    counts = agg.scenario_counts(scenarios, q_samples)
    thresholds = agg.thresholds_df(ctx.spark, cfg)

    risk = agg.build_risk_map(episodes, q_samples, counts, thresholds, st.read("silver", "grid"), sites)
    st.overwrite(risk, "gold", "risk_map", partition_by=["site_id"])

    city = agg.build_city_exposure(episodes, q_samples, counts, thresholds, st.read("silver", "city_cells"), sites)
    st.overwrite(city, "gold", "city_exposure")

    st.overwrite(agg.build_site_ranking(st.read("gold", "city_exposure")), "gold", "site_ranking")


def step_validation(ctx: Context) -> None:
    st = ctx.storage
    pattern = st.landing("validation", "deposition", "*.csv")
    if st.mode == "path" and not glob.glob(pattern):
        log.warning(
            "validation: brak pomiarów w %s — pomijam. Format pliku opisany w "
            "docs/przygotowanie-danych.md (dane JAEA EMDB, plan 2.4).", os.path.dirname(pattern)
        )
        return
    points = validation_points(
        read_measurements(ctx.spark, pattern),
        st.read("gold", "risk_map"),
        st.read("silver", "grid"),
        ctx.run["grid_size"],
        ctx.run["cell_km"],
    )
    st.overwrite(points, "gold", "validation_points")
    st.overwrite(validation_summary(st.read("gold", "validation_points")), "gold", "validation")


def step_expected_dose(ctx: Context) -> None:
    """Eksport oczekiwanej mocy dawki (komórka × godzina) dla symulatora czujników (plan P3).

    Liczymy godzinowo tylko JEDEN scenariusz demo (epizod × wariant centralny),
    więc to tanie — dlatego wolno tu zapisać wynik godzinowy.
    """
    st, cfg = ctx.storage, ctx.cfg
    demo = cfg["demo"]
    if demo["site_id"] not in ctx.run["sites"]:
        log.warning("expected_dose: lokalizacja demo %s nie jest aktywna — pomijam", demo["site_id"])
        return
    sc = st.read("silver", "scenarios").where(
        (F.col("scenario_set") == demo["scenario_set"])
        & (F.col("site_id") == demo["site_id"])
        & (F.col("episode_id") == demo["episode_id"])
        & (F.col("variant_id") == demo["variant_id"])
    )
    grid = st.read("silver", "grid").where(F.col("site_id") == demo["site_id"])
    hourly = disp.hourly_unit_deposition(
        st.read("silver", "meteo"), sc, grid, disp.nuclides_df(ctx.spark, cfg["physics"]), cfg["physics"]
    )
    dose = disp.expected_dose_series(hourly, grid, _q_median(ctx))
    st.overwrite(dose, "gold", "expected_dose")


BATCH_STEPS = {
    "ingest-meteo": step_ingest_meteo,
    "ingest-cities": step_ingest_cities,
    "bronze": step_bronze,
    "silver": step_silver_base,
    "dispersion": step_dispersion,
    "gold": step_gold,
    "validation": step_validation,
    "expected-dose": step_expected_dose,
}
