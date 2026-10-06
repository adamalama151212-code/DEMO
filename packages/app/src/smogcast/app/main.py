"""Entry point of the smogcast-app wheel.

    smogcast-app ask --city Kraków [--pollutant PM10]   tomorrow's forecast in words (no language model needed)
    smogcast-app chat "What is the PM10 alarm level?"   the assistant: forecast, data (text-to-SQL) or documents
    smogcast-app build-index                             (re)build the RAG index from docs/rag + landing/rag
    smogcast-app ui                                      the web interface (Streamlit, http://localhost:8501)

Unlike pipeline steps these commands take a question, not ``--step``.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from smogcast.core.config import ENVIRONMENTS
from smogcast.core.runner import configure_logging, run


def _print_answer(answer) -> None:
    print(answer.text)
    if answer.sql:
        print(f"\n[SQL] {answer.sql}")
    for i, (chunk, score) in enumerate(answer.sources, 1):
        print(f"[{i}] {chunk.title} — {chunk.location} (score {score:.2f})")
    for note in answer.notes:
        print(f"[note] {note}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="smogcast-app", description="Will the PM limit be exceeded tomorrow?")
    parser.add_argument("--env", default=None, choices=ENVIRONMENTS)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    ask = sub.add_parser("ask", help="tomorrow's exceedance probability for a city")
    ask.add_argument("--city", required=True, help="city name, with or without Polish diacritics")
    ask.add_argument("--pollutant", default=None, choices=["PM10", "PM2.5"], help="default: both")
    chat = sub.add_parser("chat", help="ask the assistant anything (forecast, data, documents)")
    chat.add_argument("question")
    sub.add_parser("build-index", help="build the RAG index from the knowledge documents")
    ui = sub.add_parser("ui", help="start the web interface")
    ui.add_argument("--port", type=int, default=8501)
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    if args.command == "ui":
        # Streamlit runs its own server process; the environment travels in SMOGCAST_ENV.
        from smogcast.app.ui.theme import STREAMLIT_THEME

        script = Path(__file__).with_name("ui") / "streamlit_app.py"
        theme = [f"--{k}={v}" for k, v in STREAMLIT_THEME.items()]
        cmd = [sys.executable, "-m", "streamlit", "run", str(script), "--server.port", str(args.port),
               "--server.address", "0.0.0.0", "--browser.gatherUsageStats", "false", *theme,
               "--", *(["--env", args.env] if args.env else [])]
        return subprocess.call(cmd)

    from smogcast.app.text import resolve_city

    # Spark starts only after argument parsing, so `--help` and typos fail instantly.
    from smogcast.core.config import load_config

    if args.command == "ask" and resolve_city(load_config(args.env)["cities"], args.city) is None:
        cities = ", ".join(c["name"] for c in load_config(args.env)["cities"].values())
        print(f"There is no forecast for “{args.city}”. Available cities: {cities}.")
        return 2

    from smogcast.app import services
    from smogcast.core.context import build_context

    ctx = build_context(args.env, offline=args.offline)
    if args.command == "build-index":
        services.build_index(ctx)
        return 0
    if args.command == "ask":
        city = ctx.cfg["cities"][resolve_city(ctx.cfg["cities"], args.city)]["name"]
        question = f"Will {args.pollutant or 'PM10 or PM2.5'} exceed the limit in {city} tomorrow?"
        _print_answer(services.build_assistant(ctx).forecast_answer(question))
        return 0
    _print_answer(services.build_assistant(ctx).ask(args.question))
    return 0


if __name__ == "__main__":
    run(main)
