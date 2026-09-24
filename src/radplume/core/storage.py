"""Warstwa przechowywania: jedna klasa, dwa tryby — ścieżki lokalne albo Unity Catalog.

Dlaczego osobna warstwa (plan 3.1: „zakazane ścieżki literalne”):
- Lokalnie nie ma Unity Catalog, więc tabele Delta to katalogi na dysku
  (``data/delta/<warstwa>/<tabela>``).
- Na Databricks te same tabele to ``<katalog>.<warstwa>.<tabela>`` w UC,
  a pliki surowe leżą w volume ``/Volumes/...``.
- Kod transformacji woła ``storage.read("silver", "meteo")`` i nie wie,
  który tryb jest aktywny. Przejście do chmury = zmiana ``storage.mode``
  w YAML, zero zmian w logice.

Schematy (warstwy) odpowiadają planowi 2.3: raw, bronze, silver, gold, ops.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

LAYERS = ("bronze", "silver", "gold", "ops")


class Storage:
    def __init__(self, spark: SparkSession, cfg: dict):
        self.spark = spark
        st = cfg["storage"]
        self.mode = st["mode"]
        if self.mode == "path":
            # Zmienna środowiskowa wygrywa z YAML — testy i Docker mogą
            # przekierować dane gdzie indziej bez edycji konfiguracji.
            root = os.environ.get("RADPLUME_DATA_DIR", st.get("root", "data"))
            self.root = Path(root).resolve()
            self._landing_root = self.root / "landing"
            self._checkpoint_root = self.root / "checkpoints"
        elif self.mode == "catalog":
            self.catalog = st["catalog"]
            self._landing_root = Path(st["landing_root"])
            self._checkpoint_root = Path(st["checkpoint_root"])
        else:
            raise ValueError(f"Nieznany storage.mode: {self.mode!r}")

    # ------------------------------------------------------------------ pliki
    def landing(self, *parts: str) -> str:
        """Ścieżka w strefie landing (pliki surowe: JSON z API, CSV, strumień czujników)."""
        return str(self._landing_root.joinpath(*parts))

    def checkpoint(self, name: str) -> str:
        """Katalog checkpointu Structured Streaming — dzięki niemu strumień
        po restarcie wie, które pliki już przetworzył (exactly-once)."""
        return str(self._checkpoint_root / name)

    # ----------------------------------------------------------------- tabele
    def _check_layer(self, layer: str) -> None:
        if layer not in LAYERS:
            raise ValueError(f"Nieznana warstwa {layer!r}; dozwolone: {LAYERS}")

    def table_path(self, layer: str, name: str) -> str:
        return str(self.root / "delta" / layer / name)

    def table_name(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.{name}"

    def sql_ref(self, layer: str, name: str) -> str:
        """Referencja do tabeli w zapytaniu SQL.

        Lokalnie ``delta.`/ścieżka``` (tabela ścieżkowa), w UC pełna nazwa
        trzyczłonowa. Aplikacja AI buduje SQL przez tę metodę, więc zapytania
        działają w obu trybach.
        """
        self._check_layer(layer)
        if self.mode == "path":
            return f"delta.`{self.table_path(layer, name)}`"
        return self.table_name(layer, name)

    def exists(self, layer: str, name: str) -> bool:
        self._check_layer(layer)
        if self.mode == "path":
            return DeltaTable.isDeltaTable(self.spark, self.table_path(layer, name))
        return self.spark.catalog.tableExists(self.table_name(layer, name))

    def read(self, layer: str, name: str) -> DataFrame:
        self._check_layer(layer)
        if self.mode == "path":
            return self.spark.read.format("delta").load(self.table_path(layer, name))
        return self.spark.read.table(self.table_name(layer, name))

    def read_stream(self, layer: str, name: str) -> DataFrame:
        """Tabela Delta jako źródło strumieniowe (bronze → silver w ścieżce czujników)."""
        self._check_layer(layer)
        if self.mode == "path":
            return self.spark.readStream.format("delta").load(self.table_path(layer, name))
        return self.spark.readStream.table(self.table_name(layer, name))

    def delta_table(self, layer: str, name: str) -> DeltaTable:
        if self.mode == "path":
            return DeltaTable.forPath(self.spark, self.table_path(layer, name))
        return DeltaTable.forName(self.spark, self.table_name(layer, name))

    def _writer(self, df: DataFrame, mode: str, partition_by: list[str] | None):
        w = df.write.format("delta").mode(mode)
        if partition_by:
            w = w.partitionBy(*partition_by)
        return w

    def _save(self, writer, layer: str, name: str) -> None:
        if self.mode == "path":
            writer.save(self.table_path(layer, name))
        else:
            self._ensure_schema(layer)
            writer.saveAsTable(self.table_name(layer, name))

    def _ensure_schema(self, layer: str) -> None:
        # W PROD schematy tworzy bootstrap/governance (plan 4.1); tu tylko
        # zabezpieczenie dla DEV, gdzie deweloper może zacząć od pustego katalogu.
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{layer}")

    def overwrite(
        self,
        df: DataFrame,
        layer: str,
        name: str,
        partition_by: list[str] | None = None,
        replace_where: str | None = None,
    ) -> None:
        """Nadpisanie tabeli albo jej fragmentu.

        ``replace_where`` (np. ``"scenario_set = 'climatology'"``) podmienia
        atomowo tylko ten wycinek. Używamy go, gdy przeliczamy CAŁY zestaw
        scenariuszy od nowa: to tak samo idempotentne jak MERGE po kluczu,
        a dużo tańsze dla milionów wierszy (brak joinu ze starą wersją).
        """
        self._check_layer(layer)
        writer = self._writer(df, "overwrite", partition_by)
        if replace_where and self.exists(layer, name):
            new_cols = set(df.columns) - set(self.read(layer, name).columns)
            if new_cols:
                # Migracja schematu (np. dane z poprzedniej wersji bez kolumny scenario_set):
                # warunek replaceWhere na starych wierszach dałby NULL i zostawiłby je w tabeli.
                # Bezpieczniej przeliczyć całą tabelę od nowa.
                log.warning("%s.%s: nowe kolumny %s — nadpisuję całą tabelę (migracja schematu)",
                            layer, name, sorted(new_cols))
                replace_where = None
        if replace_where and self.exists(layer, name):
            # mergeSchema: nowa kolumna w danych (np. po rozszerzeniu schematu) zostanie
            # dopisana do tabeli zamiast przerwać zapis.
            writer = writer.option("replaceWhere", replace_where).option("mergeSchema", "true")
        else:
            # Pozwala zmienić schemat przy pełnym nadpisaniu (np. nowa kolumna w gold).
            writer = writer.option("overwriteSchema", "true")
        self._save(writer, layer, name)
        log.info("Zapisano %s.%s (overwrite%s)", layer, name, f", {replace_where}" if replace_where else "")

    def merge(self, df: DataFrame, layer: str, name: str, keys: list[str]) -> None:
        """Upsert po kluczu naturalnym — podstawowy mechanizm idempotencji (plan 4.5).

        Ponowne uruchomienie z tymi samymi danymi nie dubluje wierszy:
        istniejące klucze są aktualizowane, nowe — dopisywane.
        """
        self._check_layer(layer)
        # Źródło MERGE nie może mieć duplikatów klucza (Delta rzuci błąd
        # „multiple source rows matched”). Deduplikujemy świadomie tutaj.
        df = df.dropDuplicates(keys)
        if not self.exists(layer, name):
            self._save(self._writer(df, "overwrite", None), layer, name)
            log.info("Utworzono %s.%s", layer, name)
            return
        cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
        (
            self.delta_table(layer, name)
            .alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("MERGE do %s.%s po %s", layer, name, keys)

    def append_idempotent(
        self, df: DataFrame, layer: str, name: str, app_id: str, version: int
    ) -> None:
        """Dopisanie z identyfikatorem transakcji (txnAppId/txnVersion).

        Używane w ``foreachBatch`` strumienia: jeśli Spark powtórzy mikro-batch
        po awarii, Delta rozpozna parę (app_id, version) i nie zapisze go drugi raz.
        To daje exactly-once przy zapisie do kilku tabel z jednego strumienia.
        """
        self._check_layer(layer)
        writer = (
            df.write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", version)
            .option("mergeSchema", "true")
        )
        self._save(writer, layer, name)
