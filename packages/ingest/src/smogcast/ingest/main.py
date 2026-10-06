"""Entry point of the smogcast-ingest wheel: ``smogcast-ingest --step <name> --env <env>``.

Each step is a function ``(Context) -> None``; one Lakeflow Job task runs one step.
Steps not implemented yet point to their stage in PLAN_SMOG.md.
"""

from __future__ import annotations

from smogcast.core.runner import make_main, run
from smogcast.ingest.steps import (
    step_fault_replay,
    step_gios_archive,
    step_gios_live,
    step_gios_registry,
    step_rag_documents,
    step_weather_forecast_history,
    step_weather_forecast_live,
)

STEPS = {
    "gios-archive": step_gios_archive,
    "weather-forecast-history": step_weather_forecast_history,
    "weather-forecast-live": step_weather_forecast_live,
    "gios-live": step_gios_live,
    "gios-registry": step_gios_registry,
    "fault-replay": step_fault_replay,
    "rag-documents": step_rag_documents,
}

main = make_main("ingest", STEPS, "Raw data into the landing zone")

if __name__ == "__main__":
    run(main)
