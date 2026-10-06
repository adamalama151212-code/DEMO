"""Entry point of the smogcast-gold wheel: ``smogcast-gold --step <name> --env <env>``.

Each step is a function ``(Context) -> None``; one Lakeflow Job task runs one step.
Tomorrow's forecast is a step of the smogcast-model wheel (``--step forecast``), see steps.py.
"""

from __future__ import annotations

from smogcast.core.runner import make_main, run
from smogcast.gold.steps import step_dq_summary

STEPS = {
    "dq-summary": step_dq_summary,
}

main = make_main("gold", STEPS, "Business-level gold tables")

if __name__ == "__main__":
    run(main)
