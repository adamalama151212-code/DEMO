"""Metryki jakości danych zapisywane do ``ops.dq_metrics``.

Dlaczego tabela, a nie tylko logi: dashboard (plan 4.7, pkt 5) ma pokazywać,
że jakość danych jest MIERZONA — ile rekordów odrzucono, ile spóźniło się itd.
Logi znikają, tabela Delta zostaje i da się ją odpytać SQL-em.
"""

from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import types as T

from radplume.core.storage import Storage

log = logging.getLogger(__name__)

_SCHEMA = T.StructType(
    [
        T.StructField("run_ts", T.TimestampType()),
        T.StructField("step", T.StringType()),
        T.StructField("metric", T.StringType()),
        T.StructField("value", T.DoubleType()),
    ]
)


def log_metrics(storage: Storage, step: str, metrics: dict[str, float]) -> None:
    # Datetime ze strefą UTC — naiwny PySpark przeliczyłby wg strefy systemu.
    now = dt.datetime.now(dt.timezone.utc)
    rows = [(now, step, k, float(v)) for k, v in metrics.items()]
    for _, _, k, v in rows:
        log.info("[DQ] %s: %s = %s", step, k, v)
    df = storage.spark.createDataFrame(rows, _SCHEMA)
    # Zwykły append (a nie MERGE): to dziennik zdarzeń, każdy przebieg ma swój wpis.
    storage._save(df.write.format("delta").mode("append"), "ops", "dq_metrics")
