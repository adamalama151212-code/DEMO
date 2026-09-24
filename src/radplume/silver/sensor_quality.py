"""silver.sensor_quality — reguły wymagające HISTORII urządzenia (plan 3.5.3 krok 3b, P2, P18).

Dryf, zamrożony odczyt i skok między odczytami potrzebują poprzednich wartości
tego samego urządzenia. Funkcje okna po wierszach (``lag``, ``rows between``)
nie działają w Structured Streaming, więc liczymy to wsadowo nad
``silver.sensor_clean`` — na Databricks jako materialized view, lokalnie jako
tabela nadpisywana przy każdym przebiegu (ta sama semantyka: wynik = funkcja
całego wejścia).

Dryf (P18): NIE z-score na surowej dawce (szum 35% + zmiany smugi → z ≈ 1,2,
nic by nie wykrył). Zamiast tego reszta względem modelu:
    r = ln(dawka / (tło + mediana_modelu))
Bez dryfu średnia r ≈ 0 niezależnie od tego, czy czujnik jest w smudze.
Odróżnienie usterki od sygnału: usterka jest LOKALNA (jeden czujnik),
realne odchylenie od modelu dotyczy OBSZARU (sąsiedzi też je widzą).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def with_model(clean: DataFrame, expected_dose: DataFrame, background_usv_h: float) -> DataFrame:
    """Dołącza oczekiwaną moc dawki z modelu dla komórki i godziny odczytu."""
    m = expected_dose.select("cell_id", "hour_ts", "model_dose_usv_h")
    j = clean.withColumn("hour_ts", F.date_trunc("hour", "event_time")).join(m, ["cell_id", "hour_ts"], "left")
    expected = F.lit(background_usv_h) + F.coalesce("model_dose_usv_h", F.lit(0.0))
    return j.withColumn("expected_usv_h", expected).withColumn(
        # ln(0) i ln(ujemnej) → NULL; dawka ≤ 0 i tak nie przeszłaby reguł twardych
        "residual", F.when(F.col("dose_rate_usv_h") > 0, F.log(F.col("dose_rate_usv_h") / expected))
    )


def device_history_flags(df: DataFrame, dq: dict) -> DataFrame:
    drift, frozen, wind = dq["drift_detection"], dq["frozen_reading"], dq["wind_speed_ms"]
    w = Window.partitionBy("device_id").orderBy("event_time")
    # rangeBetween na sekundach: okno „ostatnie 30 minut”, niezależnie od tego,
    # ile odczytów w nim jest (przerwy w łączności nie psują okna).
    w_res = Window.partitionBy("device_id").orderBy(F.col("event_time").cast("long")).rangeBetween(
        -drift["window_minutes"] * 60, 0
    )
    w_all = Window.partitionBy("device_id")

    df = (
        df.withColumn("residual_30m", F.avg("residual").over(w_res))
        .withColumn("prev_dose", F.lag("dose_rate_usv_h").over(w))
        .withColumn("prev_wind", F.lag("wind_speed_ms").over(w))
        .withColumn("same_as_prev", F.coalesce(F.col("dose_rate_usv_h") == F.col("prev_dose"), F.lit(False)))
        # Długość serii identycznych wartości: nowa grupa zaczyna się przy każdej zmianie wartości,
        # a pozycja w grupie = liczba powtórzeń.
        .withColumn("_new_run", F.when(F.col("same_as_prev"), 0).otherwise(1))
        .withColumn("_run_id", F.sum("_new_run").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    )
    w_run = Window.partitionBy("device_id", "_run_id").orderBy("event_time")
    return (
        df.withColumn("repeat_count", F.row_number().over(w_run))
        .withColumn("is_frozen", F.col("repeat_count") >= frozen["repeat_count_threshold"])
        .withColumn(
            "is_wind_jump",
            F.coalesce(F.abs(F.col("wind_speed_ms") - F.col("prev_wind")) > wind["max_delta_per_reading"], F.lit(False)),
        )
        .withColumn("first_seen", F.min("event_time").over(w_all))
        .withColumn(
            "is_warmup",
            F.col("event_time") < F.col("first_seen") + F.make_interval(mins=F.lit(drift["warmup_minutes"])),
        )
        .drop("_new_run", "_run_id", "prev_dose", "prev_wind")
    )


def neighbor_residuals(df: DataFrame, radius_km: float) -> DataFrame:
    """Mediana ``residual_30m`` innych urządzeń w promieniu R w tej samej minucie.

    Pary sąsiadów liczymy raz, z pozycji urządzeń (mała tabela), a potem łączymy
    po minucie — bez iloczynu wszystkich odczytów ze wszystkimi.
    """
    pos = df.select("device_id", "site_id", "x_km", "y_km").distinct()
    a, b = pos.alias("a"), pos.alias("b")
    pairs = a.join(
        b,
        (F.col("a.site_id") == F.col("b.site_id"))
        & (F.col("a.device_id") != F.col("b.device_id"))
        & (F.sqrt((F.col("a.x_km") - F.col("b.x_km")) ** 2 + (F.col("a.y_km") - F.col("b.y_km")) ** 2) <= radius_km),
    ).select(F.col("a.device_id").alias("device_id"), F.col("b.device_id").alias("nb_id")).distinct()

    minute = df.select("device_id", F.date_trunc("minute", "event_time").alias("minute"), "residual_30m")
    nb = minute.select(F.col("device_id").alias("nb_id"), "minute", F.col("residual_30m").alias("nb_res"))
    return (
        minute.select("device_id", "minute")
        .join(pairs, "device_id")
        .join(nb, ["nb_id", "minute"])
        .groupBy("device_id", "minute")
        .agg(F.percentile_approx("nb_res", 0.5).alias("neighbor_residual_30m"), F.count("*").alias("n_neighbors"))
    )


def build_sensor_quality(clean: DataFrame, expected_dose: DataFrame, dq: dict, background_usv_h: float) -> DataFrame:
    drift = dq["drift_detection"]
    thr = drift["residual_mean_threshold"]
    df = device_history_flags(with_model(clean, expected_dose, background_usv_h), dq)
    nb = neighbor_residuals(df, drift["neighbor_radius_km"])
    df = df.withColumn("minute", F.date_trunc("minute", "event_time")).join(nb, ["device_id", "minute"], "left")

    elevated = F.coalesce(F.col("residual_30m") > thr, F.lit(False))
    neighbors_elevated = F.coalesce(F.col("neighbor_residual_30m") > thr, F.lit(False))
    df = df.withColumn("is_drift_suspect", elevated & ~neighbors_elevated).withColumn(
        "is_plume_signal", elevated & neighbors_elevated
    )
    # Priorytet statusów: warm-up (brak historii) > frozen > dryf > skok wiatru > clean.
    status = (
        F.when(F.col("is_warmup"), "warmup")
        .when(F.col("is_frozen"), "frozen")
        .when(F.col("is_drift_suspect"), "drift_suspect")
        .when(F.col("is_wind_jump"), "wind_jump")
        .otherwise("clean")
    )
    return df.withColumn("quality_status", status).drop("minute", "first_seen")
