"""silver.meteo — meteo gotowe dla modelu: wektory wiatru, kierunek smugi, klasa Pasquilla.

Najważniejsza pułapka (plan 2.4.1, P21): ``wind_direction_10m`` to kierunek,
Z KTÓREGO wieje wiatr (konwencja meteorologiczna). Smuga leci w przeciwną stronę.
Konwersja jest w JEDNYM miejscu (tutaj) i ma test jednostkowy.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Klasy Pasquilla jako indeksy 0..5 (A..F): łatwo przesuwać wariantem
# niepewności (±1 klasa) i ograniczać do zakresu.
STABILITY_CLASSES = ["A", "B", "C", "D", "E", "F"]


def plume_bearing(wind_from_deg: Column) -> Column:
    """Kierunek, w którym leci smuga = kierunek wiatru + 180° (mod 360).
    Przykład z testu API: wiatr 311° (z NW) → smuga na 131° (SE, na Wejherowo/Gdynię)."""
    return F.pmod(wind_from_deg + F.lit(180.0), F.lit(360.0))


def wind_components(speed: Column, wind_from_deg: Column) -> tuple[Column, Column]:
    """Składowe wektora ruchu powietrza: u (na wschód), v (na północ).

    Wiatr „z” kierunku θ porusza powietrze w stronę θ+180°, stąd minus:
    u = -s·sin(θ), v = -s·cos(θ). Sprawdzenie: θ=270° (z zachodu) → u = +s (na wschód).
    """
    rad = F.radians(wind_from_deg)
    return -speed * F.sin(rad), -speed * F.cos(rad)


def pasquill_class_idx(wind_ms: Column, radiation_wm2: Column, cloud_pct: Column) -> Column:
    """Klasa stabilności Pasquilla (0=A … 5=F) wg klasycznej tabeli Turnera.

    Dzień: im silniejsze nasłonecznienie i słabszy wiatr, tym bardziej
    niestabilnie (A). Noc: bezchmurnie + słaby wiatr = bardzo stabilnie (F).
    Duże zachmurzenie (≥ 90%) = neutralnie (D) o każdej porze.

    Klasy pośrednie tabeli (np. „A–B”) mapujemy na BARDZIEJ stabilną z pary:
    stabilniejsza atmosfera = węższa smuga = wyższe stężenia daleko od źródła,
    więc to wybór konserwatywny dla pytania „czy miasto zostanie skażone”.
    Promieniowanie krótkofalowe zastępuje liczenie wysokości Słońca — prościej
    i bez dodatkowej astronomii.
    """
    A, B, C, D, E, Fc = range(6)
    u = wind_ms
    is_day = radiation_wm2 > 25
    strong = radiation_wm2 > 600
    moderate = radiation_wm2 > 300

    day = (
        F.when(u < 2, F.when(strong, A).when(moderate, B).otherwise(B))
        .when(u < 3, F.when(strong, B).when(moderate, B).otherwise(C))
        .when(u < 5, F.when(strong, B).when(moderate, C).otherwise(C))
        .when(u < 6, F.when(strong, C).when(moderate, D).otherwise(D))
        .otherwise(F.when(strong, C).otherwise(D))
    )
    cloudy_night = cloud_pct >= 50  # ≥ 4/8 pokrycia nieba
    night = (
        F.when(u < 3, F.when(cloudy_night, E).otherwise(Fc))
        .when(u < 5, F.when(cloudy_night, D).otherwise(E))
        .otherwise(D)
    )
    return F.when(cloud_pct >= 90, D).when(is_day, day).otherwise(night)


def build_silver_meteo(bronze: DataFrame) -> tuple[DataFrame, dict]:
    """Zwraca (silver_meteo, metryki DQ)."""
    required = ["wind_speed_10m", "wind_direction_10m", "precipitation", "cloud_cover", "shortwave_radiation"]
    # Kontrola jakości: wiersz bez kluczowej zmiennej nie nadaje się do modelu.
    # Odrzucamy go jawnie i liczymy, ile takich było (metryka → ops.dq_metrics).
    complete = F.lit(True)
    for c in required:
        complete = complete & F.col(c).isNotNull()
    physical = (F.col("wind_speed_10m") >= 0) & (F.col("wind_speed_10m") < 75) & (
        F.col("precipitation") >= 0
    )
    flagged = bronze.withColumn("_ok", complete & physical)

    stats = flagged.agg(
        F.count("*").alias("rows_in"),
        F.count_if(~F.col("_ok")).alias("rows_rejected"),
        F.count_if(F.col("grid_elevation") <= 0).alias("rows_grid_elevation_le_0"),
    ).first()
    metrics = stats.asDict()

    df = flagged.where("_ok").drop("_ok")
    u10, v10 = wind_components(F.col("wind_speed_10m"), F.col("wind_direction_10m"))
    silver = df.select(
        "site_id",
        "time_utc",
        "source",
        "grid_lat",
        "grid_lon",
        "grid_elevation",
        F.col("wind_speed_10m").alias("wind_speed_ms"),
        F.col("wind_direction_10m").alias("wind_from_deg"),
        # Wiatr 100 m bywa pusty w starszych danych — wtedy przyjmujemy 10 m.
        F.coalesce("wind_speed_100m", "wind_speed_10m").alias("wind_speed_100m_ms"),
        plume_bearing(F.col("wind_direction_10m")).alias("plume_to_deg"),
        u10.alias("u_ms"),
        v10.alias("v_ms"),
        F.col("precipitation").alias("precip_mm_h"),
        F.col("cloud_cover").alias("cloud_pct"),
        F.col("shortwave_radiation").alias("radiation_wm2"),
        pasquill_class_idx(
            F.col("wind_speed_10m"), F.col("shortwave_radiation"), F.col("cloud_cover")
        ).alias("stability_idx"),
    ).withColumn(
        "stability_class", F.element_at(F.array(*[F.lit(c) for c in STABILITY_CLASSES]), F.col("stability_idx") + 1)
    )
    return silver, metrics
