"""bronze.source_term — przebieg uwolnienia w czasie z pliku (Katata 2015 / Terada 2020).

Format pliku i pułapki (czas JST → UTC, jednostki): docs/przygotowanie-danych.md, pkt E.
Bronze niczego nie przelicza — rozkład przedziałów na godziny robi
``silver/scenarios.schedule_from_intervals``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE_TERM_FILE_SCHEMA = """
    site_id STRING, nuclide STRING, time_start_utc TIMESTAMP, time_end_utc TIMESTAMP,
    release_rate_bq_h DOUBLE, release_height_m DOUBLE, source STRING
"""


def read_source_term(spark: SparkSession, landing_dir: str) -> DataFrame:
    return (
        spark.read.schema(SOURCE_TERM_FILE_SCHEMA)
        .option("header", True)
        # Tylko pliki bezpośrednio w folderze: oryginały w `_zrodlo/` są ignorowane.
        .option("pathGlobFilter", "*.csv")
        .csv(landing_dir)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
