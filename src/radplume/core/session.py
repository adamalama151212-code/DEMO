"""Sesja Spark: lokalna z Delta Lake albo istniejąca sesja klastra Databricks.

To JEDYNE miejsce w kodzie, które wie, czy działamy lokalnie, czy w chmurze
(plan 3.1: „ingest odseparowany od transformacji”, „ścieżki z configu”).
Cała reszta dostaje gotowy obiekt ``SparkSession`` i nie musi tego rozróżniać.
"""

from __future__ import annotations

import logging
import os
import time

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


def is_databricks() -> bool:
    """Databricks ustawia tę zmienną na każdym klastrze i w serverless."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _force_process_utc() -> None:
    """Strefa czasowa procesu Pythona = UTC.

    PySpark przy ``collect()`` i ``createDataFrame`` zamienia znaczniki czasu
    według strefy PROCESU Pythona (nie sesji Sparka). Na laptopie z czasem
    polskim datetime z tabeli różniłby się o 1–2 h od tego w Dockerze/na
    klastrze — np. symulator szukałby dawki dla złej godziny.
    """
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):  # brak na Windows — tam i tak zalecamy Docker/WSL
        time.tzset()


def get_spark(cfg: dict) -> SparkSession:
    _force_process_utc()
    if is_databricks():
        # Na klastrze sesja już istnieje (z Delta, Unity Catalog, Photonem).
        # Budowanie własnej zignorowałoby konfigurację klastra.
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = _build_local_session(cfg["spark"])

    # UTC wszędzie (plan 2.4.1). Bez tego Spark interpretuje znaczniki czasu
    # w strefie systemu operacyjnego i ten sam kod dałby inne godziny
    # na laptopie w Polsce, w Dockerze i na klastrze w Azure.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def _build_local_session(spark_cfg: dict) -> SparkSession:
    # Import tutaj, a nie na górze pliku: na Databricks pakiet `delta`
    # (delta-spark z pip) nie jest potrzebny, bo Delta jest wbudowana w runtime.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master(spark_cfg.get("master", "local[*]"))
        .appName("radplume-local")
        # Dwie linijki, które zamieniają zwykłego Sparka w „Sparka z Delta Lake”:
        # rozszerzenia SQL (MERGE, DESCRIBE HISTORY) i katalog rozumiejący tabele Delta.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", str(spark_cfg.get("shuffle_partitions", 8)))
        .config(
            "spark.databricks.delta.snapshotPartitions",
            str(spark_cfg.get("delta_snapshot_partitions", 2)),
        )
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "4g"))
        # Adaptive Query Execution: Spark sam łączy małe partycje po shuffle.
        # Na Databricks jest domyślnie włączone — włączamy lokalnie, żeby plany były podobne.
        .config("spark.sql.adaptive.enabled", "true")
        # Pasek postępu zaśmieca logi CLI i testów.
        .config("spark.ui.showConsoleProgress", "false")
        # Metastore Derby tworzyłby pliki w bieżącym katalogu. Lokalnie używamy
        # tabel ścieżkowych (patrz storage.py), więc metastore nie jest potrzebny.
        .config("spark.sql.catalogImplementation", "in-memory")
    )
    # configure_spark_with_delta_pip dopisuje spark.jars.packages z właściwą
    # wersją jarów Delta. Przy pierwszym uruchomieniu pobiera je z Maven Central
    # (potem są w cache ~/.ivy2).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Lokalna sesja Spark %s gotowa", spark.version)
    return spark
