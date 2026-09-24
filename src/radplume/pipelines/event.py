"""Orkiestracja trybu zdarzenia: ``radplume event``.

Kolejność:
    1. pogoda dla dnia zdarzenia (+ czas śledzenia obłoku) — pobierana, jeśli jej brak,
    2. scenariusze zdarzenia (zespół członków) → zapis TYLKO wycinka ``scenario_set = event_…``,
    3. model transportu dla tego zestawu,
    4. gold (przeliczany w całości — tabele gold są małe),
    5. podsumowanie w terminalu.

Wymaga wcześniejszego ``radplume run-batch`` (siatka i miasta w zasięgu dla lokalizacji).
"""

from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import functions as F

from radplume.ingest.meteo import ingest_meteo
from radplume.pipelines.batch import (
    compute_dispersion,
    refresh_bronze_meteo,
    refresh_silver_meteo,
    step_gold,
    write_scenario_tables,
)
from radplume.pipelines.context import Context
from radplume.silver.events import EventSpec, build_event_tables

log = logging.getLogger(__name__)


def _ensure_meteo(ctx: Context, spec: EventSpec) -> None:
    """Pogoda musi pokrywać okno uwolnienia + horyzont śledzenia obłoku."""
    horizon = int(ctx.cfg["physics"]["transport"]["horizon_hours"])
    t0 = spec.window_start
    t1 = t0 + dt.timedelta(hours=spec.window_hours + horizon)
    needed_hours = int((t1 - t0).total_seconds() // 3600)

    st = ctx.storage
    have = 0
    if st.exists("silver", "meteo"):
        have = (
            st.read("silver", "meteo")
            .where((F.col("site_id") == spec.site_id) & (F.col("time_utc") >= t0) & (F.col("time_utc") < t1))
            .count()
        )
    if have >= needed_hours:
        return
    log.info("event: brak pełnej pogody dla %s %s..%s — pobieram", spec.site_id, t0, t1)
    ingest_meteo(ctx.cfg, st.landing(), [(spec.site_id, t0.date().isoformat(), t1.date().isoformat())])
    refresh_bronze_meteo(ctx)
    refresh_silver_meteo(ctx, [spec.site_id])


def _check_prerequisites(ctx: Context, site_id: str) -> None:
    st = ctx.storage
    if site_id not in ctx.run["sites"]:
        raise ValueError(f"Lokalizacja {site_id} nie jest aktywna (run.sites w pliku środowiska)")
    for table in ("grid", "city_cells"):
        if not st.exists("silver", table) or st.read("silver", table).where(F.col("site_id") == site_id).isEmpty():
            raise RuntimeError(f"Brak silver.{table} dla {site_id} — uruchom najpierw `radplume run-batch`")


def run_event(ctx: Context, spec: EventSpec, n_members: int) -> None:
    _check_prerequisites(ctx, spec.site_id)
    _ensure_meteo(ctx, spec)

    only_this = f"scenario_set = '{spec.name}'"
    write_scenario_tables(ctx, build_event_tables(ctx.cfg, spec, n_members).to_frames(ctx.spark), only_this)
    compute_dispersion(ctx, only_this)
    step_gold(ctx)


def event_summary(ctx: Context, spec: EventSpec, threshold_id: str = "cs137_contaminated", top: int = 10) -> str:
    """Krótkie podsumowanie: miasta z największym prawdopodobieństwem przekroczenia progu."""
    ce = ctx.storage.read("gold", "city_exposure").where(
        (F.col("scenario_set") == spec.name) & (F.col("threshold_id") == threshold_id)
    )
    rows = ce.orderBy(F.desc("p_exceed"), "distance_km").limit(top).collect()
    lines = [
        f"Zdarzenie {spec.name} — {ctx.cfg['sites'][spec.site_id]['name']}",
        "Uwolnienia (UTC, Cs-137):",
        *[f"  {r.start_utc:%Y-%m-%d %H:%M} – {r.end_utc:%H:%M}: {r.amount_bq:.2e} Bq" for r in spec.releases],
        "",
        f"Miasta wg prawdopodobieństwa przekroczenia progu „{rows[0]['threshold_label'] if rows else threshold_id}”:",
    ]
    for r in rows:
        arrival = f"{r['arrival_h_p50']:.1f} h" if r["arrival_h_p50"] is not None else "—"
        lines.append(
            f"  {r['city_name']:<16} {r['distance_km']:5.1f} km  p = {r['p_exceed'] * 100:5.1f}%  "
            f"mediana dotarcia od {spec.window_start:%H:%M} UTC: {arrival}"
        )
    lines += ["", f"Szczegóły: radplume ask --site {spec.site_id} --city <miasto> --scenario-set {spec.name}"]
    return "\n".join(lines)
