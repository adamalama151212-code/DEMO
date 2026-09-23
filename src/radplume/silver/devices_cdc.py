"""silver.devices — rejestr urządzeń jako SCD typu 2 z feedu CDC (zdolność zaawansowana, plan 4.4).

Na Databricks to jest jedna deklaracja Lakeflow:

    dlt.create_auto_cdc_flow(target="silver_devices", source="bronze_device_cdc",
        keys=["device_id"], sequence_by="seq",
        apply_as_deletes=F.expr("op = 'DELETE'"), stored_as_scd_type=2)

Lokalnie (bez Lakeflow) implementujemy TĘ SAMĄ semantykę funkcjami okna:
- każda wersja urządzenia ma ``valid_from`` / ``valid_to`` (odpowiednik
  ``__START_AT`` / ``__END_AT`` w AUTO CDC),
- DELETE zamyka ostatnią wersję i sam nie tworzy nowej,
- powtórzone zdarzenie (ten sam ``seq``) jest ignorowane.
Przeliczamy całość z bronze (deterministycznie) — AUTO CDC robi to przyrostowo,
ale wynik musi być identyczny. Dzięki SCD2 wiemy, jaki firmware (i jaka
reguła DQ) obowiązywał w chwili KAŻDEGO odczytu.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CDC_SCHEMA = """
    device_id STRING, site_id STRING, cell_id STRING, jurisdiction_code STRING,
    firmware STRING, x_km DOUBLE, y_km DOUBLE, lat DOUBLE, lon DOUBLE, role STRING,
    op STRING, seq LONG, change_time TIMESTAMP
"""


def read_cdc(spark: SparkSession, landing_dir: str) -> DataFrame:
    return (
        spark.read.schema(CDC_SCHEMA)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json")
        .json(landing_dir)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingest_ts", F.current_timestamp())
    )


def apply_scd2(changes: DataFrame) -> DataFrame:
    # Idempotencja: to samo zdarzenie (device_id, seq) liczy się raz.
    dedup = changes.dropDuplicates(["device_id", "seq"])
    w = Window.partitionBy("device_id").orderBy("seq")
    versions = dedup.withColumn("valid_to", F.lead("change_time").over(w))
    return (
        versions.where(F.col("op") != "DELETE")
        .withColumnRenamed("change_time", "valid_from")
        .withColumn("is_current", F.col("valid_to").isNull())
        .select(
            "device_id", "site_id", "cell_id", "jurisdiction_code", "firmware",
            "x_km", "y_km", "lat", "lon", "role", "seq", "valid_from", "valid_to", "is_current",
        )
    )
