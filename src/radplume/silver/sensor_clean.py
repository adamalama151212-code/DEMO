"""Czyszczenie strumienia czujników — reguły BEZSTANOWE + deduplikacja (plan 3.5.3 krok 3a).

Wszystko jako czyste funkcje ``DataFrame -> DataFrame``: te same funkcje
testujemy w pytest na statycznych DataFrame'ach i wywołujemy na strumieniu
(lokalnie Structured Streaming, na Databricks flow w Lakeflow).

Dlaczego spóźnienie liczymy z ``sent_at``, a nie samym watermarkiem (plan P1):
watermark po cichu gubi spóźnione rekordy w operatorach stanowych — nie da się
ich zapisać do osobnej tabeli, a wynik zależy od podziału na mikro-batche
(niedeterministyczne testy). Jawny ``lag`` + filtr jest deterministyczny
i każdy odrzucony rekord ląduje w ``ops.sensor_late_rejected``.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SENSOR_SCHEMA = """
    device_id STRING, event_time TIMESTAMP, sent_at TIMESTAMP,
    dose_rate_usv_h DOUBLE, wind_speed_ms DOUBLE, battery DOUBLE,
    detector_temp_c DOUBLE
"""
# detector_temp_c: kolumna z firmware v2. Lokalnie jest w jawnym schemacie
# (file source Sparka nie ewoluuje schematu). Na Databricks Auto Loader
# z cloudFiles.schemaEvolutionMode = addNewColumns doda ją sam, gdy się pojawi —
# to jest pokaz schema evolution z planu (4.4).


def with_lag(df: DataFrame) -> DataFrame:
    lag_min = (F.unix_timestamp("sent_at") - F.unix_timestamp("event_time")) / 60.0
    return df.withColumn("lag_minutes", lag_min)


def is_late(max_lag_min: float) -> Column:
    return F.col("lag_minutes") > F.lit(max_lag_min)


def split_late(df: DataFrame, max_lag_min: float) -> tuple[DataFrame, DataFrame]:
    """(na czas, spóźnione). Rekord bez ``sent_at`` traktujemy jak spóźniony:
    nie da się udowodnić, że zmieścił się w oknie."""
    df = with_lag(df)
    late = is_late(max_lag_min) | F.col("lag_minutes").isNull()
    return df.where(~late), df.where(late)


def dedup(df: DataFrame, watermark_min: int, keys: list[str]) -> DataFrame:
    """Deduplikacja po (device_id, event_time) na strumieniu.

    Watermark TYLKO ogranicza stan (ile kluczy Spark pamięta) — strumień jest już
    przefiltrowany po lag, więc watermark tej samej długości niczego nie powinien
    gubić. Sprawdzamy to metryką numRowsDroppedByWatermark (≈ 0).
    """
    if not df.isStreaming:
        # Na danych statycznych (testy, ponowne przeliczenie z bronze) nie ma stanu
        # do ograniczania — zwykła deduplikacja daje ten sam wynik. Spark 4 nie pozwala
        # wywołać dropDuplicatesWithinWatermark na DataFrame wsadowym.
        return df.dropDuplicates(keys)
    return df.withWatermark("event_time", f"{watermark_min} minutes").dropDuplicatesWithinWatermark(keys)


def hard_rules(dq: dict) -> dict[str, str]:
    """Reguły twarde (expect_or_drop w Lakeflow) budowane z progów w sensor_dq.yaml."""
    w, d = dq["wind_speed_ms"], dq["dose_rate_usv_h"]
    return {
        "wind_physical": f"wind_speed_ms BETWEEN {w['hard_min']} AND {w['hard_max']}",
        "dose_in_detector_range": f"dose_rate_usv_h BETWEEN {d['hard_min']} AND {d['hard_max']}",
        "no_null_when_powered": "NOT (battery > 0 AND dose_rate_usv_h IS NULL)",
        "known_device": "cell_id IS NOT NULL",  # odczyt z urządzenia spoza rejestru / wycofanego
    }


def apply_hard_rules(df: DataFrame, rules: dict[str, str]) -> tuple[DataFrame, DataFrame]:
    """(czyste, kwarantanna z listą złamanych reguł).

    Wzorzec „quarantine table”: expectations w Lakeflow same nie zapisują
    odrzuconych wierszy, więc robimy drugi zapis z odwróconym warunkiem.
    ``coalesce(…, false)``: reguła z wynikiem NULL (brak danych) = złamana —
    nie przepuszczamy rekordu, o którym nic nie wiemy.
    """
    checks = {name: F.coalesce(F.expr(expr), F.lit(False)) for name, expr in rules.items()}
    failed = F.array_compact(F.array(*[F.when(~c, F.lit(name)) for name, c in checks.items()]))
    flagged = df.withColumn("failed_rules", failed)
    clean = flagged.where(F.size("failed_rules") == 0).drop("failed_rules")
    quarantine = flagged.where(F.size("failed_rules") > 0).withColumn(
        "quarantine_reason", F.array_join("failed_rules", ",")
    )
    return clean, quarantine


def join_device_version(readings: DataFrame, devices_scd2: DataFrame) -> DataFrame:
    """Dołącza wersję urządzenia obowiązującą W CHWILI ODCZYTU (join SCD2 po czasie).

    Dzięki temu odczyt z 10:30 dostaje firmware sprzed aktualizacji z 11:00,
    a odczyt urządzenia wycofanego nie dostanie żadnej wersji (→ kwarantanna).
    """
    d = devices_scd2.select(
        F.col("device_id").alias("_dev"), "site_id", "cell_id", "jurisdiction_code", "firmware",
        "x_km", "y_km", "lat", "lon", "valid_from", "valid_to",
    )
    cond = (
        (F.col("device_id") == F.col("_dev"))
        & (F.col("event_time") >= F.col("valid_from"))
        & (F.col("valid_to").isNull() | (F.col("event_time") < F.col("valid_to")))
    )
    return readings.join(F.broadcast(d), cond, "left").drop("_dev", "valid_from", "valid_to")
