"""bronze.cities — miasta z GeoNames (TSV) albo z listy zapasowej (CSV).

Oba formaty sprowadzamy do jednego schematu, żeby silver nie musiał
wiedzieć, skąd przyszły dane.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Format pliku GeoNames: 19 kolumn rozdzielonych tabulatorem, bez nagłówka
# (https://download.geonames.org/export/dump/readme.txt).
GEONAMES_SCHEMA = """
    geonameid LONG, name STRING, asciiname STRING, alternatenames STRING,
    latitude DOUBLE, longitude DOUBLE, feature_class STRING, feature_code STRING,
    country_code STRING, cc2 STRING, admin1_code STRING, admin2_code STRING,
    admin3_code STRING, admin4_code STRING, population LONG, elevation INT,
    dem INT, timezone STRING, modification_date STRING
"""

FALLBACK_SCHEMA = """
    city_id STRING, name STRING, ascii_name STRING, country_code STRING,
    lat DOUBLE, lon DOUBLE, population LONG
"""


def read_cities(spark: SparkSession, path: str) -> DataFrame:
    if path.endswith(".txt"):
        raw = (
            spark.read.schema(GEONAMES_SCHEMA)
            .option("sep", "\t")
            # Nazwy miejscowości mogą zawierać cudzysłowy. Wyłączamy obsługę
            # cudzysłowów, inaczej jeden „"” rozkleiłby kolumny w całym wierszu.
            .option("quote", "\u0000")
            .csv(path)
        )
        df = raw.select(
            F.concat(F.lit("gn_"), F.col("geonameid").cast("string")).alias("city_id"),
            "name",
            F.col("asciiname").alias("ascii_name"),
            "country_code",
            F.col("latitude").alias("lat"),
            F.col("longitude").alias("lon"),
            "population",
            F.lit("geonames").alias("source"),
        )
    else:
        df = (
            spark.read.schema(FALLBACK_SCHEMA)
            .option("header", True)
            .option("encoding", "UTF-8")
            .csv(path)
            .withColumn("source", F.lit("fallback_approx"))
        )
    return df.withColumn("source_file", F.col("_metadata.file_path")).withColumn(
        "ingest_ts", F.current_timestamp()
    )
