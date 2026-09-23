"""bronze.meteo — godzinowe dane meteo z plików JSON w landing.

Jeden plik JSON (koperta + odpowiedź API z tablicami godzinowymi) zamieniamy
na wiersze „jedna godzina = jeden wiersz”.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from radplume.ingest.meteo import HOURLY_VARS

# Jawny schemat zamiast inferencji (plan 3.5.3 krok 2: „jawny schemat, nie inferSchema”):
# - inferencja czyta wszystkie pliki dwa razy (wolno przy tysiącach plików),
# - typ mógłby „pływać” (int vs double) zależnie od tego, jakie liczby trafiły do pliku,
# - schemat jest jednocześnie dokumentacją kontraktu z API.
_HOURLY_FIELDS = ", ".join(
    ["time: ARRAY<STRING>"] + [f"{v}: ARRAY<DOUBLE>" for v in HOURLY_VARS]
)
_UNIT_FIELDS = ", ".join(f"{v}: STRING" for v in ("time", *HOURLY_VARS))
METEO_FILE_SCHEMA = f"""
    site_id STRING,
    source STRING,
    requested_lat DOUBLE,
    requested_lon DOUBLE,
    fetched_at STRING,
    payload STRUCT<
        latitude: DOUBLE,
        longitude: DOUBLE,
        elevation: DOUBLE,
        utc_offset_seconds: INT,
        timezone: STRING,
        hourly_units: STRUCT<{_UNIT_FIELDS}>,
        hourly: STRUCT<{_HOURLY_FIELDS}>
    >
"""

BRONZE_METEO_KEYS = ["site_id", "time_utc"]


def read_landing_meteo(spark: SparkSession, landing_meteo_dir: str) -> DataFrame:
    raw = (
        spark.read.schema(METEO_FILE_SCHEMA)
        # multiLine: każdy plik to jeden obiekt JSON (a nie JSON Lines)
        .option("multiLine", True)
        # Katalog + filtr zamiast wzorca „*/*.json” w ścieżce: działa tak samo
        # lokalnie i na volume, a Spark nie loguje ostrzeżeń o nieistniejącej ścieżce.
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json")
        .json(landing_meteo_dir)
        # _metadata.file_path — wbudowana kolumna Sparka (3.3+), działa tak samo
        # lokalnie i na Databricks; zastępuje przestarzałe input_file_name().
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    return explode_hourly(raw)


def explode_hourly(raw: DataFrame) -> DataFrame:
    """Tablice godzinowe → wiersze.

    ``arrays_zip`` + ``inline`` zamiast osobnych ``explode`` na każdej kolumnie:
    kilka niezależnych explode dałoby iloczyn kartezjański (godziny × godziny),
    a arrays_zip łączy tablice „po indeksie”, tak jak są ułożone w JSON-ie.
    """
    h = "payload.hourly"
    flat = raw.select(
        "site_id",
        "source",
        "requested_lat",
        "requested_lon",
        F.col("payload.latitude").alias("grid_lat"),
        F.col("payload.longitude").alias("grid_lon"),
        F.col("payload.elevation").alias("grid_elevation"),
        F.col("payload.hourly_units.wind_speed_10m").alias("wind_unit"),
        "source_file",
        F.col(f"{h}.time").alias("time"),
        *[F.col(f"{h}.{v}").alias(v) for v in HOURLY_VARS],
    )
    zipped = flat.select(
        *[c for c in flat.columns if c not in ("time", *HOURLY_VARS)],
        F.inline(F.arrays_zip("time", *HOURLY_VARS)),
    )
    return (
        zipped.withColumn("time_utc", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
        .drop("time")
        .withColumn("ingest_ts", F.current_timestamp())
    )
