"""Kontekst przebiegu: konfiguracja + sesja Spark + warstwa przechowywania.

Każdy krok potoku dostaje ten jeden obiekt zamiast trzech argumentów.
Kroki nie tworzą sesji ani nie czytają konfiguracji same — dzięki temu
ten sam krok uruchamia CLI lokalnie, task Joba na Databricks i test pytest
(który podstawia małą konfigurację i tymczasowy katalog danych).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from radplume.core.storage import Storage


@dataclass
class Context:
    cfg: dict
    spark: SparkSession
    storage: Storage

    @property
    def run(self) -> dict:
        return self.cfg["run"]
