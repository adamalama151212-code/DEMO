"""silver.dispersion — gaussowski model smugi policzony w Sparku.

Model (świadomie prosty — plan Część VI, „Uwaga do fizyki”):
- każda godzina uwolnienia to osobna, stacjonarna smuga z pogodą tej godziny
  („segmentacja godzinowa” — mityguje założenie jednorodnego wiatru),
- dyspersja: wzory Briggsa (1973) dla terenu wiejskiego, klasy Pasquilla A–F,
- odbicie od gruntu, wiatr na wysokości uwolnienia z profilu potęgowego,
- depozycja sucha (v_d · całka stężenia) + mokra (Λ · całka kolumny),
- rozpad promieniotwórczy w czasie transportu x/u.

LINIOWOŚĆ względem Q (P20): wszystko liczymy dla uwolnienia 1 Bq („unit”).
Prawdziwa depozycja = unit × Q, więc 100 próbek Q nie wymaga 100 przeliczeń
smugi — mnożymy dopiero w gold. To największa oszczędność obliczeń w projekcie.

Funkcje fizyczne przyjmują i zwracają obiekty ``Column`` — te same wzory
testujemy na kilku wierszach (tests/test_physics.py) i liczymy na milionach.
"""

from __future__ import annotations

import math

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from radplume.silver.meteo import STABILITY_CLASSES

# Współczynniki Briggsa (teren wiejski) dla σy = a·x·(1 + 0,0001x)^(-1/2), indeks = klasa A..F
_SIGMA_Y_A = [0.22, 0.16, 0.11, 0.08, 0.06, 0.04]


def _lit_array(values: list[float]) -> Column:
    return F.array(*[F.lit(float(v)) for v in values])


def briggs_sigmas(x_m: Column, stab_idx: Column) -> tuple[Column, Column]:
    """(σy, σz) w metrach dla odległości z wiatrem ``x_m`` i klasy 0..5."""
    a_y = F.element_at(_lit_array(_SIGMA_Y_A), stab_idx + 1)
    sigma_y = a_y * x_m * F.pow(F.lit(1.0) + F.lit(1e-4) * x_m, -0.5)
    sigma_z = (
        F.when(stab_idx == 0, 0.20 * x_m)
        .when(stab_idx == 1, 0.12 * x_m)
        .when(stab_idx == 2, 0.08 * x_m * F.pow(1 + 2e-4 * x_m, -0.5))
        .when(stab_idx == 3, 0.06 * x_m * F.pow(1 + 1.5e-3 * x_m, -0.5))
        .when(stab_idx == 4, 0.03 * x_m / (1 + 3e-4 * x_m))
        .otherwise(0.016 * x_m / (1 + 3e-4 * x_m))
    )
    return sigma_y, sigma_z


def wind_at_height(u10: Column, height_m: Column, stab_idx: Column, exponents: dict, u_min: float) -> Column:
    """u(H) = u10 · (H/10)^p, obcięte od dołu do ``u_min`` (cisza wiatrowa)."""
    p = F.element_at(_lit_array([exponents[c] for c in STABILITY_CLASSES]), stab_idx + 1)
    return F.greatest(u10 * F.pow(height_m / F.lit(10.0), p), F.lit(u_min))


def concentration_per_rate(u: Column, y: Column, z: Column, H: Column, sy: Column, sz: Column) -> Column:
    """χ/q [s/m³] w punkcie (x, y, z) — pełny wzór gaussowski z odbiciem od gruntu.

    χ/q = 1/(2π σy σz u) · exp(-y²/2σy²) · [exp(-(z-H)²/2σz²) + exp(-(z+H)²/2σz²)]
    Drugi człon to „źródło lustrzane” pod ziemią — grunt odbija, a nie pochłania.
    """
    lateral = F.exp(-(y * y) / (2 * sy * sy))
    vertical = F.exp(-((z - H) ** 2) / (2 * sz * sz)) + F.exp(-((z + H) ** 2) / (2 * sz * sz))
    return lateral * vertical / (F.lit(2 * math.pi) * sy * sz * u)


def ground_concentration_per_rate(u: Column, y: Column, H: Column, sy: Column, sz: Column) -> Column:
    """χ/q przy gruncie (z = 0) — to, co osiada na ziemi i czym oddychają ludzie."""
    return concentration_per_rate(u, y, F.lit(0.0), H, sy, sz)


def column_per_rate(u: Column, y: Column, sy: Column) -> Column:
    """∫χ/q dz od 0 do ∞ [s/m²] — ilość w słupie powietrza, którą wymywa deszcz.
    Z odbiciem od gruntu całka po z wynosi dokładnie 1/(√(2π) σy u) · exp(-y²/2σy²)."""
    return F.exp(-(y * y) / (2 * sy * sy)) / (F.lit(math.sqrt(2 * math.pi)) * sy * u)


def nuclides_df(spark: SparkSession, physics: dict) -> DataFrame:
    rows = [
        (
            name,
            math.log(2) / float(p["half_life_s"]),  # stała rozpadu λ [1/s]
            float(p["dry_deposition_velocity_ms"]),
            float(p["washout_a"]),
            float(p["washout_b"]),
            float(p["groundshine_usv_h_per_kbq_m2"]),
        )
        for name, p in physics["nuclides"].items()
    ]
    return spark.createDataFrame(
        rows, "nuclide STRING, decay_const DOUBLE, vd_ms DOUBLE, washout_a DOUBLE, washout_b DOUBLE, groundshine DOUBLE"
    )


def episode_hours(scenarios: DataFrame) -> DataFrame:
    """Rozwija epizody na godziny uwolnienia (h = 0..episode_hours-1) z czasem UTC."""
    eps = scenarios.select("scenario_set", "site_id", "episode_id", "episode_start", "episode_hours").distinct()
    return eps.withColumn("h", F.explode(F.sequence(F.lit(0), F.col("episode_hours") - 1))).withColumn(
        "time_utc", F.timestamp_seconds(F.unix_timestamp("episode_start") + F.col("h") * 3600)
    )


def hourly_unit_deposition(
    meteo: DataFrame,
    scenarios: DataFrame,
    grid: DataFrame,
    nuclides: DataFrame,
    physics: dict,
) -> DataFrame:
    """Depozycja [Bq/m² na 1 Bq uwolnienia] w każdej komórce od każdej godziny uwolnienia.

    Wymiary: scenariusze (epizod × wariant) × godziny × komórki × nuklidy.
    To jest „duży” iloczyn projektu — dlatego:
    - siatka i nuklidy idą przez ``broadcast`` (małe tabele kopiowane do
      każdego executora zamiast shuffle'a dużej strony),
    - wiersze pod wiatr i daleko od osi smugi odcinamy PRZED agregacją.
    """
    hours = episode_hours(scenarios).join(
        meteo.select(
            "site_id", "time_utc", "wind_speed_ms", "wind_from_deg", "plume_to_deg",
            "precip_mm_h", "stability_idx",
        ),
        on=["site_id", "time_utc"],
        how="inner",  # godzina bez meteo = brak obliczenia; liczone w metrykach DQ
    )
    s = scenarios.join(hours, on=["scenario_set", "site_id", "episode_id", "episode_start", "episode_hours"])

    stab = F.least(F.greatest(F.col("stability_idx") + F.col("stability_shift"), F.lit(0)), F.lit(5))
    s = s.withColumn("stab", stab).withColumn(
        "u_h",
        wind_at_height(
            F.col("wind_speed_ms"), F.col("release_height_m"), F.col("stab"),
            physics["wind_profile_exponent"], physics["min_wind_speed_ms"],
        ),
    )

    g = s.join(F.broadcast(grid.select("site_id", "cell_id", "x_km", "y_km")), on="site_id")
    phi = F.radians("plume_to_deg")
    dx, dy = F.col("x_km") * 1000.0, F.col("y_km") * 1000.0
    # Obrót do układu smugi: x_down wzdłuż kierunku lotu, y_cross w poprzek.
    g = g.withColumn("x_down", dx * F.sin(phi) + dy * F.cos(phi)).withColumn(
        "y_cross", dx * F.cos(phi) - dy * F.sin(phi)
    )
    g = g.where(F.col("x_down") >= physics["min_downwind_distance_m"])  # nic nie leci pod wiatr

    sy, sz = briggs_sigmas(F.col("x_down"), F.col("stab"))
    g = g.withColumn("sigma_y", sy).withColumn("sigma_z", sz)
    g = g.where(F.abs(F.col("y_cross")) <= physics["crosswind_cutoff_sigmas"] * F.col("sigma_y"))

    g = g.crossJoin(F.broadcast(nuclides))
    travel_s = F.col("x_down") / F.col("u_h")
    chi = ground_concentration_per_rate(
        F.col("u_h"), F.col("y_cross"), F.col("release_height_m"), F.col("sigma_y"), F.col("sigma_z")
    )
    column = column_per_rate(F.col("u_h"), F.col("y_cross"), F.col("sigma_y"))
    # Uwolnienie 1 Bq rozłożone równo na episode_hours godzin → każda godzina wypuszcza 1/N Bq.
    frac = F.lit(1.0) / F.col("episode_hours")
    washout = F.when(
        F.col("precip_mm_h") > 0,
        F.col("washout_a") * F.col("washout_mult") * F.pow(F.col("precip_mm_h"), F.col("washout_b")),
    ).otherwise(F.lit(0.0))
    dry = F.col("vd_ms") * F.col("vd_mult") * chi * frac
    wet = washout * column * frac
    decay = F.exp(-F.col("decay_const") * travel_s)

    return g.select(
        "scenario_set", "site_id", "episode_id", "variant_id", "episode_start", "h",
        "cell_id", "nuclide", "wind_from_deg", "groundshine",
        ((dry + wet) * decay).alias("dep_hour"),
        (F.col("h") + travel_s / 3600.0).alias("arrival_h"),
    )


def aggregate_episodes(hourly: DataFrame, q_median: DataFrame, arrival_threshold_bq_m2: float) -> DataFrame:
    """Suma po godzinach uwolnienia → jedna wartość na (scenariusz, komórka, nuklid).

    To utrwalamy w silver. Godzinowego wyniku NIE zapisujemy w trybie
    klimatologicznym (plan 3.4: „uwaga na eksplozję”) — byłby N razy większy.
    """
    h = hourly.join(F.broadcast(q_median), on=["site_id", "nuclide"])
    reached = F.col("dep_hour") * F.col("q_median_bq") >= arrival_threshold_bq_m2
    return h.groupBy("scenario_set", "site_id", "episode_id", "variant_id", "cell_id", "nuclide").agg(
        F.sum("dep_hour").alias("unit_dep_per_bq"),
        # czas dotarcia: pierwsza godzina, w której depozycja (przy medianie Q) jest znacząca
        F.min(F.when(reached, F.col("arrival_h"))).alias("arrival_h"),
        # kierunek wiatru w godzinie, która dała NAJWIĘCEJ depozycji — do „najgorszego sektora”
        F.max_by("wind_from_deg", "dep_hour").alias("dominant_wind_from_deg"),
    )


def expected_dose_series(hourly: DataFrame, grid: DataFrame, q_median: DataFrame, hours_after: int = 24) -> DataFrame:
    """Oczekiwana moc dawki [µSv/h] w każdej komórce co godzinę — wejście dla symulatora czujników.

    Depozyt z godziny uwolnienia h pojawia się w komórce w godzinie dotarcia
    floor(arrival_h), potem się kumuluje (suma narastająca). Moc dawki = depozyt
    × współczynnik ground shine, sumowany po nuklidach. Konserwatywnie pomijamy
    rozpad I-131 po osadzeniu — w skali kilku dni to kilkadziesiąt procent,
    ale dla demo potoku czujników wystarczy.
    """
    h = (
        hourly.join(F.broadcast(q_median), on=["site_id", "nuclide"])
        .withColumn("arr_idx", F.floor("arrival_h").cast("int"))
        .withColumn("dose_contrib", F.col("dep_hour") * F.col("q_median_bq") / 1000.0 * F.col("groundshine"))
        .groupBy("site_id", "episode_start", "cell_id", "arr_idx")
        .agg(F.sum("dose_contrib").alias("dose_contrib"))
    )
    meta = hourly.select("site_id", "episode_start").distinct()
    n_hours = hourly.agg(F.max("h")).first()[0] + 1 + hours_after
    # Gęsta siatka (komórka × godzina), żeby każda godzina miała wartość —
    # także te, w których nic nowego nie spadło (depozyt się utrzymuje).
    dense = (
        meta.join(grid.select("site_id", "cell_id"), on="site_id")
        .withColumn("arr_idx", F.explode(F.sequence(F.lit(0), F.lit(n_hours - 1))))
        .join(h, on=["site_id", "episode_start", "cell_id", "arr_idx"], how="left")
    )
    w = Window.partitionBy("site_id", "episode_start", "cell_id").orderBy("arr_idx").rowsBetween(
        Window.unboundedPreceding, 0
    )
    return dense.select(
        "site_id",
        "cell_id",
        F.timestamp_seconds(F.unix_timestamp("episode_start") + F.col("arr_idx") * 3600).alias("hour_ts"),
        F.sum(F.coalesce("dose_contrib", F.lit(0.0))).over(w).alias("model_dose_usv_h"),
    )
