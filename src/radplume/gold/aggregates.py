"""Agregacja Monte Carlo: (epizody × warianty × próbki Q) → prawdopodobieństwa i percentyle.

Kluczowy szczegół statystyczny: w silver zapisujemy tylko scenariusze, w których
smuga DOTARŁA do komórki (resztę odcięliśmy dla wydajności). Scenariusze
„nie dotarła” to depozycja 0 i MUSZĄ liczyć się do mianownika — inaczej
prawdopodobieństwo skażenia byłoby zawyżone. Dlatego:
- p_exceed = (liczba scenariuszy ≥ próg) / (liczba WSZYSTKICH scenariuszy),
- percentyle liczymy metodą „nearest rank” na posortowanej tablicy
  z dopisanymi w myślach zerami (bez fizycznego tworzenia zer).

Dlaczego dokładne percentyle zamiast ``percentile_approx``: są deterministyczne
(test idempotencji porównuje sumy kontrolne dwóch przebiegów) i poprawnie
uwzględniają niejawne zera. Tablice na grupę mają rozmiar ≤ epizody × warianty × Q
(lokalnie ~1 tys., w PROD ~100 tys. liczb) — mieści się w pamięci executora.
Przy dalszym skalowaniu: zamiana na percentile_approx z korektą o udział zer.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from radplume import MODEL_VERSION

SECTORS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def thresholds_df(spark: SparkSession, cfg: dict) -> DataFrame:
    """Progi przeliczone na depozycję [kBq/m²] — jedna jednostka porównań w gold.

    Próg dawkowy D [mSv/rok] → depozycja: D / (ground shine [µSv/h na kBq/m²] × 8760 h / 1000).
    """
    rows = []
    for tid, t in cfg["thresholds"].items():
        gs = cfg["physics"]["nuclides"][t["nuclide"]]["groundshine_usv_h_per_kbq_m2"]
        if t["metric"] == "deposition_kbq_m2":
            dep = float(t["value"])
        elif t["metric"] == "annual_dose_msv":
            dep = float(t["value"]) / (gs * 8760 / 1000)
        else:
            raise ValueError(f"Nieznana metryka progu {tid}: {t['metric']}")
        rows.append((tid, t["nuclide"], t["metric"], float(t["value"]), dep, t["label"]))
    return spark.createDataFrame(
        rows,
        "threshold_id STRING, nuclide STRING, metric STRING, threshold_value DOUBLE, threshold_kbq_m2 DOUBLE, threshold_label STRING",
    )


def scenario_counts(scenarios: DataFrame, q_samples: DataFrame) -> DataFrame:
    """Liczba wszystkich scenariuszy = (epizody × warianty) × próbki Q — mianownik prawdopodobieństw."""
    ev = scenarios.groupBy("scenario_set", "site_id").agg(F.count("*").alias("n_episode_variants"))
    nq = q_samples.groupBy("site_id", "nuclide").agg(F.count("*").alias("n_q"))
    return ev.join(nq, on="site_id").withColumn("n_total", F.col("n_episode_variants") * F.col("n_q"))


def nearest_rank(sorted_vals: Column, n_total: Column, q: float) -> Column:
    """Percentyl q (0..1) metodą nearest rank z niejawnymi zerami na początku rozkładu."""
    pos = F.greatest(F.ceil(F.lit(q) * n_total), F.lit(1))
    zeros = n_total - F.size(sorted_vals)
    # try_element_at: w Spark 4 (ANSI) zwykłe element_at poza zakresem rzuca wyjątek.
    return F.when(pos <= zeros, F.lit(0.0)).otherwise(F.try_element_at(sorted_vals, (pos - zeros).cast("int")))


def with_q(episodes: DataFrame, q_samples: DataFrame) -> DataFrame:
    """Nakłada próbki Q: depozycja [kBq/m²] = unit × Q / 1000 (liniowość modelu)."""
    return episodes.join(F.broadcast(q_samples), on=["site_id", "nuclide"]).withColumn(
        "dep_kbq_m2", F.col("unit_dep_per_bq") * F.col("q_bq") / 1000.0
    )


def exceedance_stats(grouped_vals: DataFrame, thresholds: DataFrame) -> DataFrame:
    """Z kolumny ``vals`` (posortowane depozycje) i ``n_total`` liczy p_exceed i percentyle."""
    d = grouped_vals.join(F.broadcast(thresholds), on="nuclide")
    return (
        d.withColumn(
            "p_exceed",
            F.size(F.filter("vals", lambda v: v >= F.col("threshold_kbq_m2"))) / F.col("n_total"),
        )
        .withColumn("dep_p05_kbq_m2", nearest_rank(F.col("vals"), F.col("n_total"), 0.05))
        .withColumn("dep_p50_kbq_m2", nearest_rank(F.col("vals"), F.col("n_total"), 0.50))
        .withColumn("dep_p95_kbq_m2", nearest_rank(F.col("vals"), F.col("n_total"), 0.95))
        .drop("vals")
    )


def build_risk_map(
    episodes: DataFrame, q_samples: DataFrame, counts: DataFrame, thresholds: DataFrame, grid: DataFrame, sites: DataFrame
) -> DataFrame:
    dep = with_q(episodes, q_samples)
    g = dep.groupBy("scenario_set", "site_id", "cell_id", "nuclide").agg(
        F.array_sort(F.collect_list("dep_kbq_m2")).alias("vals")
    )
    g = g.join(counts.select("scenario_set", "site_id", "nuclide", "n_total"), on=["scenario_set", "site_id", "nuclide"])
    stats = exceedance_stats(g, thresholds)
    return (
        stats.join(grid.select("site_id", "cell_id", "lat", "lon", "x_km", "y_km", "dist_km"), on=["site_id", "cell_id"])
        .join(F.broadcast(sites.select("site_id", "site_name", "jurisdiction_code")), on="site_id")
        .withColumn("model_version", F.lit(MODEL_VERSION))
    )


def wind_sector(deg: Column) -> Column:
    """Stopnie → jeden z 8 sektorów (N, NE, …). Sektor N obejmuje 337,5°–22,5°."""
    idx = F.floor(F.pmod(deg + F.lit(22.5), F.lit(360.0)) / 45).cast("int")
    return F.element_at(F.array(*[F.lit(s) for s in SECTORS]), idx + 1)


def build_city_exposure(
    episodes: DataFrame,
    q_samples: DataFrame,
    counts: DataFrame,
    thresholds: DataFrame,
    city_cells: DataFrame,
    sites: DataFrame,
) -> DataFrame:
    """PYTANIE GŁÓWNE (plan P12): czy uwolnienie w lokalizacji X skazi miasto Y.

    Dla każdej pary (lokalizacja, miasto w promieniu 100 km):
    - p_exceed           — odsetek scenariuszy z przekroczeniem progu,
    - dep_p05/p50/p95    — rozkład depozycji,
    - arrival_h_p50      — mediana czasu dotarcia smugi (w scenariuszach, gdzie dotarła),
    - worst_wind_sector  — z jakiego kierunku wiatr jest najgroźniejszy dla tego miasta.
    Miasto, do którego smuga nigdy nie dotarła, też ma wiersz (p_exceed = 0):
    odpowiedź „0%” to informacja, a nie brak danych.
    """
    cc = city_cells.select("site_id", "cell_id", "city_id", "city_name", "country_code", "population", "distance_km", "bearing_deg", "city_source")
    ep = episodes.join(F.broadcast(cc), on=["site_id", "cell_id"])

    dep = with_q(ep, q_samples)
    vals = dep.groupBy("scenario_set", "site_id", "city_id", "nuclide").agg(
        F.array_sort(F.collect_list("dep_kbq_m2")).alias("vals")
    )
    arrival = ep.groupBy("scenario_set", "site_id", "city_id", "nuclide").agg(
        F.array_sort(F.collect_list("arrival_h")).alias("arr")  # collect_list pomija NULL = „nie dotarła”
    ).select(
        "scenario_set", "site_id", "city_id", "nuclide",
        F.when(F.size("arr") > 0, F.try_element_at("arr", F.ceil(F.size("arr") * 0.5).cast("int"))).alias("arrival_h_p50"),
    )
    sector = (
        ep.withColumn("sector", wind_sector(F.col("dominant_wind_from_deg")))
        .groupBy("scenario_set", "site_id", "city_id", "nuclide", "sector")
        .agg(F.sum("unit_dep_per_bq").alias("s"))
        .groupBy("scenario_set", "site_id", "city_id", "nuclide")
        .agg(F.max_by("sector", "s").alias("worst_wind_sector"))
    )

    # Szkielet: każde miasto × zestaw scenariuszy × nuklid — także bez żadnej depozycji.
    base = (
        cc.join(counts.select("scenario_set", "site_id", "nuclide", "n_total"), on="site_id")
    )
    keys = ["scenario_set", "site_id", "city_id", "nuclide"]
    full = (
        base.join(vals, on=keys, how="left")
        .withColumn("vals", F.coalesce("vals", F.array().cast("array<double>")))
        .join(arrival, on=keys, how="left")
        .join(sector, on=keys, how="left")
    )
    stats = exceedance_stats(full, thresholds)
    return (
        stats.join(F.broadcast(sites.select("site_id", "site_name", "jurisdiction_code")), on="site_id")
        .withColumn("model_version", F.lit(MODEL_VERSION))
    )


def build_site_ranking(city_exposure: DataFrame) -> DataFrame:
    """Oczekiwana liczba mieszkańców w miastach z przekroczeniem progu = Σ populacja × p_exceed."""
    return city_exposure.groupBy(
        "scenario_set", "site_id", "site_name", "jurisdiction_code", "nuclide", "threshold_id", "threshold_label"
    ).agg(
        F.sum(F.col("population") * F.col("p_exceed")).alias("expected_exposed_population"),
        F.count_if(F.col("p_exceed") >= 0.1).alias("n_cities_p_ge_10pct"),
        F.max_by("city_name", "p_exceed").alias("most_exposed_city"),
        F.max("p_exceed").alias("max_city_p_exceed"),
        F.count("*").alias("n_cities_in_radius"),
    )
