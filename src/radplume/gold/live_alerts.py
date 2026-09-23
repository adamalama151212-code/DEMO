"""gold.live_alerts — pomiar na żywo vs przewidywanie modelu (plan 4.4, 4.4a).

Dwa typy alertów, świadomie rozdzielone:
- ``outside_model_band`` — REALNY sygnał: odczyt ze statusem ``clean`` wypada
  poza pasmo P5–P95 modelu,
- ``sensor_fault`` — problem z JAKOŚCIĄ danych (dryf, zamrożenie, skok wiatru).
Alert ``outside_model_band`` NIE może powstać z odczytu o statusie innym niż clean.

Pasmo operacyjne: mediana modelu × niepewność pomiaru (σ z sensor_dq.yaml),
osobno dla tła i dla składnika ze smugi. Niepewność ilości uwolnienia Q jest
tu pominięta celowo: w trakcie realnego zdarzenia source term jest estymowany
na bieżąco, a szerokie pasmo ×6 z Q sprawiłoby, że żadne odchylenie nie byłoby
widoczne. To trade-off do omówienia w write-upie.

UWAGA (plan Część VIII): dawka w symulatorze pochodzi z tego samego modelu —
alerty weryfikują działanie POTOKU i logiki alertów, a nie trafność modelu.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

Z95 = 1.645  # kwantyl 95% rozkładu normalnego — pasmo P5–P95 w skali logarytmicznej


def build_live_alerts(quality: DataFrame, dq: dict, background_usv_h: float) -> DataFrame:
    s, sb = dq["measurement_sigma"], dq["background_sigma"]
    model = F.coalesce("model_dose_usv_h", F.lit(0.0))
    lo = F.lit(background_usv_h) * F.exp(F.lit(-Z95 * sb)) + model * F.exp(F.lit(-Z95 * s))
    hi = F.lit(background_usv_h) * F.exp(F.lit(Z95 * sb)) + model * F.exp(F.lit(Z95 * s))
    q = quality.withColumn("band_p05", lo).withColumn("band_p95", hi)

    alert_type = (
        F.when(
            (F.col("quality_status") == "clean")
            & ((F.col("dose_rate_usv_h") > F.col("band_p95")) | (F.col("dose_rate_usv_h") < F.col("band_p05"))),
            "outside_model_band",
        )
        .when(F.col("quality_status").isin("drift_suspect", "frozen", "wind_jump"), "sensor_fault")
    )
    a = q.withColumn("alert_type", alert_type).where(F.col("alert_type").isNotNull())
    # Pojedynczy odczyt poza pasmem zdarza się z definicji w ~10% przypadków (pasmo P5–P95).
    # Alert grupujemy per urządzenie × godzina i podajemy, JAKA CZĘŚĆ odczytów była poza pasmem.
    per_hour = q.groupBy("device_id", "hour_ts").agg(F.count("*").alias("n_readings_hour"))
    return (
        a.groupBy("device_id", "site_id", "cell_id", "jurisdiction_code", "hour_ts", "alert_type")
        .agg(
            F.count("*").alias("n_alert_readings"),
            F.min("event_time").alias("first_event_time"),
            F.max("dose_rate_usv_h").alias("max_dose_usv_h"),
            F.first("model_dose_usv_h").alias("model_dose_usv_h"),
            F.first("band_p05").alias("band_p05"),
            F.first("band_p95").alias("band_p95"),
            F.array_sort(F.collect_set("quality_status")).alias("statuses"),
        )
        .join(per_hour, ["device_id", "hour_ts"])
        .withColumn("alert_fraction", F.col("n_alert_readings") / F.col("n_readings_hour"))
        # Istotny alert = uporczywy, a nie pojedynczy odczyt z ogona rozkładu.
        # Dashboard i powiadomienia filtrują po tej kolumnie; reszta zostaje do analizy.
        .withColumn("is_significant", F.col("alert_fraction") >= F.lit(dq["alert_min_fraction"]))
    )


def build_dq_summary(bronze: DataFrame, late: DataFrame, quarantine: DataFrame, quality: DataFrame) -> DataFrame:
    """Metryki jakości danych dla dashboardu (plan 4.7 pkt 5): DQ jest MIERZONE, nie tylko zaimplementowane."""
    total = bronze.count()
    status = quality.groupBy("quality_status").count().collect()  # kilka wierszy — bezpieczne
    rows = [
        ("readings_received", float(total)),
        ("late_rejected", float(late.count())),
        ("quarantined_hard_rules", float(quarantine.count())),
        *[(f"status_{r['quality_status']}", float(r["count"])) for r in status],
    ]
    spark = bronze.sparkSession
    df = spark.createDataFrame(rows, "metric STRING, value DOUBLE")
    return df.withColumn("pct_of_received", F.round(F.col("value") / F.lit(max(total, 1)) * 100, 2))
