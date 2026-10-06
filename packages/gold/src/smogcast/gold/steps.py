"""Gold steps: each is ``(Context) -> None``.

Tomorrow's forecast (gold.forecast_tomorrow) is written by ``smogcast-model --step forecast``:
it needs the saved model and the model's feature preparation, and step wheels never import
each other (PLAN_SMOG.md, section 5a).
"""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from smogcast.core.context import Context
from smogcast.gold.dq_summary import build_dq_summary

log = logging.getLogger(__name__)

STREAM_SOURCES = ["live", "fault_replay"]
EMPTY = {
    ("bronze", "pm_stream"): "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP",
    ("ops", "pm_late_rejected"): "source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP",
    ("ops", "pm_quarantine"): ("source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, "
                               "quarantine_reason STRING"),
    ("silver", "pm_stream"): ("source STRING, station_code STRING, pollutant STRING, time_utc TIMESTAMP, "
                              "dq_flag STRING, is_valid BOOLEAN"),
}


def step_dq_summary(ctx: Context) -> None:
    """Stream tables → gold.dq_summary (full overwrite: a few dozen rows recomputed from the sources)."""
    st = ctx.storage

    def stream_table(layer: str, name: str):
        # A table can be missing legitimately, e.g. no reading was ever late.
        df = st.read(layer, name) if st.exists(layer, name) else ctx.spark.createDataFrame([], EMPTY[(layer, name)])
        return df.where(F.col("source").isin(STREAM_SOURCES))

    summary = build_dq_summary(*(stream_table(layer, name) for layer, name in EMPTY))
    st.overwrite(summary, "gold", "dq_summary")
    for r in st.read("gold", "dq_summary").orderBy("source", "metric").collect():
        log.info("dq-summary %s %s = %s (%s%%)", r["source"], r["metric"], r["value"], r["pct_of_unique"])
