"""Punkt wejścia: ``radplume <krok>``.

Ten sam entry point wywołujemy lokalnie z terminala i na Databricks
z ``python_wheel_task`` (databricks.yml). Na Databricks każdy krok to
osobny task Joba — dlatego kroki są osobnymi komendami, a nie jedną funkcją.

Przykłady:
    radplume run-all --offline          # całość bez internetu (dane syntetyczne)
    radplume run-batch                  # ścieżka batch na prawdziwych danych
    radplume ask --site lubiatowo_kopalino --city Lębork
    radplume event --site lubiatowo_kopalino --date 2020-01-15 --release 13:00=1e15 --release 16:00=5e15
"""

from __future__ import annotations

import argparse
import logging
import sys

from radplume.core.config import load_config

# Tryb offline pisze do OSOBNEGO katalogu: pliki landing działają jak cache, więc
# syntetyczne meteo w `data/` zostałoby potem po cichu użyte zamiast prawdziwego.
OFFLINE_OVERRIDES = {
    "sources": {"meteo": "synthetic", "cities": "fallback"},
    "storage": {"root": "data-offline"},
}


def _context(args):
    # Import Sparka dopiero tutaj: `radplume --help` działa natychmiast,
    # bez uruchamiania JVM.
    from radplume.core.session import get_spark
    from radplume.core.storage import Storage
    from radplume.pipelines.context import Context

    cfg = load_config(args.env, OFFLINE_OVERRIDES if args.offline else None)
    spark = get_spark(cfg)
    return Context(cfg, spark, Storage(spark, cfg))


def main(argv: list[str] | None = None) -> int:
    from radplume.pipelines.batch import BATCH_STEPS
    from radplume.pipelines.stream import STREAM_STEPS

    all_steps = {**BATCH_STEPS, **STREAM_STEPS}
    parser = argparse.ArgumentParser(prog="radplume", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", default=None, help="local | dev | prod (domyślnie RADPLUME_ENV albo local)")
    parser.add_argument(
        "--offline", action="store_true",
        help="dane syntetyczne meteo + wbudowana lista miast (bez internetu; wyniki NIE są realne)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    for name, fn in all_steps.items():
        sub.add_parser(name, help=(fn.__doc__ or "").strip().split("\n")[0])
    sub.add_parser("run-batch", help="wszystkie kroki ścieżki batch po kolei")
    sub.add_parser("run-stream", help="wszystkie kroki ścieżki czujników po kolei")
    sub.add_parser("run-all", help="batch + czujniki")

    ask = sub.add_parser("ask", help="czy uwolnienie w lokalizacji X skazi miasto Y?")
    ask.add_argument("--site", required=True, help="site_id z sites.yaml, np. lubiatowo_kopalino")
    ask.add_argument("--city", required=True, help="nazwa miasta (z polskimi znakami lub bez)")
    ask.add_argument("--scenario-set", default="climatology")

    ev = sub.add_parser(
        "event",
        help="konkretne zdarzenie: znane godziny i ilości uwolnienia, pogoda danego dnia zaburzona (zespół)",
        description=(
            "Przykład: radplume event --site lubiatowo_kopalino --date 2020-01-15 "
            "--release 13:00=1e15 --release 16:00-18:00=5e15 --timezone Europe/Warsaw"
        ),
    )
    ev.add_argument("--site", required=True)
    ev.add_argument("--date", required=True, help="dzień zdarzenia, RRRR-MM-DD")
    ev.add_argument(
        "--release", action="append", required=True,
        help="HH:MM=Bq (godzina od HH:MM) albo HH:MM-HH:MM=Bq; ilość Cs-137; można podać wiele razy",
    )
    ev.add_argument("--timezone", default="UTC", help="strefa godzin z --release, np. Europe/Warsaw (domyślnie UTC)")
    ev.add_argument("--name", default=None, help="nazwa zdarzenia (domyślnie z daty i godziny)")
    ev.add_argument("--members", type=int, default=None, help="liczba członków zespołu (domyślnie z configu)")

    sub.add_parser("list-events", help="zapisane zdarzenia (zestawy scenariuszy event_…)")

    cities = sub.add_parser("list-cities", help="miasta w zasięgu danej lokalizacji")
    cities.add_argument("--site", required=True)

    show = sub.add_parser("show", help="podgląd tabeli, np. show gold city_exposure")
    show.add_argument("layer")
    show.add_argument("table")
    show.add_argument("-n", type=int, default=20)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # py4j loguje każde wywołanie JVM na poziomie INFO — wyciszamy.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    spec = None
    if args.command == "event":
        # Parsowanie PRZED startem Sparka: literówka w --release kończy się od razu czytelnym błędem.
        import datetime as dt

        from radplume.silver.events import EventSpec, event_name, parse_release

        date = dt.date.fromisoformat(args.date)
        releases = [parse_release(r, date, args.timezone) for r in args.release]
        spec = EventSpec(event_name(date, releases, args.name), args.site, releases)

    ctx = _context(args)
    from radplume.pipelines.stream import STREAM_ORDER

    batch_order = list(BATCH_STEPS)
    if args.command in all_steps:
        all_steps[args.command](ctx)
    elif args.command in ("run-batch", "run-stream", "run-all"):
        order = {"run-batch": batch_order, "run-stream": STREAM_ORDER, "run-all": batch_order + STREAM_ORDER}[args.command]
        for step in order:
            logging.getLogger("radplume").info("===== krok: %s =====", step)
            all_steps[step](ctx)
    elif args.command == "ask":
        from radplume.app.city_query import answer_city_question

        print(answer_city_question(ctx, args.site, args.city, args.scenario_set))
    elif args.command == "event":
        from radplume.pipelines.event import event_summary, run_event

        run_event(ctx, spec, args.members or ctx.run["event"]["n_members"])
        print(event_summary(ctx, spec))
    elif args.command == "list-events":
        (
            ctx.storage.read("silver", "source_terms")
            .where("scenario_set LIKE 'event_%'")
            .select("scenario_set", "site_id", "nuclide", "median_bq")
            .orderBy("scenario_set", "nuclide")
            .show(100, truncate=False)
        )
    elif args.command == "list-cities":
        from radplume.app.city_query import list_cities

        list_cities(ctx, args.site).show(200, truncate=False)
    elif args.command == "show":
        ctx.storage.read(args.layer, args.table).show(args.n, truncate=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
