"""Scenariusze Monte Carlo: kiedy, jak i ile się uwalnia.

Każdy zestaw scenariuszy (``scenario_set``) opisują cztery małe tabele:

- ``silver.scenarios``        — epizod (kiedy) × wariant fizyczny (jak się rozproszy),
- ``silver.source_terms``     — całkowita ilość uwolnienia Q: mediana i niepewność (gsd),
- ``silver.release_schedule`` — rozkład uwolnienia w czasie: jaki UŁAMEK Q wychodzi w godzinie h,
- ``silver.q_samples``        — próbki Q losowane z ``source_terms``.

Zestawy:
- ``climatology``      — losowe momenty awarii z wieloletniej pogody, emisja równomierna,
- ``validation_2011``  — prawdziwe daty Fukushimy; harmonogram z pliku (Katata 2015),
                         a gdy go brak — równomierny z ostrzeżeniem,
- ``event_…``          — konkretne zdarzenie podane przez użytkownika (``silver/events.py``).

Dlaczego harmonogram jako UŁAMKI, a nie Bq na godzinę: model zostaje liniowy
względem Q. Smugę liczymy dla 1 Bq rozłożonego w czasie według harmonogramu,
a ilość (i jej niepewność) mnożymy dopiero w gold — jak dotąd (plan P20).

Tabele parametrów są małe, więc generujemy je w Pythonie z seedowanym RNG.
To NIE łamie zakazu pętli z plan 3.1 — zakaz dotyczy danych dużych.
Dlaczego nie ``F.rand(seed)``: wynik zależy od podziału na partycje, więc zmiana
liczby rdzeni zmieniłaby scenariusze. ``random.Random(seed)`` daje zawsze ten sam ciąg.
"""

from __future__ import annotations

import datetime as dt
import logging
import math
import random
import zlib
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

CLIMATOLOGY = "climatology"
EVENT_PREFIX = "event_"

SCENARIO_SCHEMA = """
    scenario_set STRING, site_id STRING, episode_id INT, episode_start TIMESTAMP,
    episode_hours INT, variant_id INT, stability_shift INT, vd_mult DOUBLE,
    washout_mult DOUBLE, release_height_m DOUBLE, wind_dir_offset_deg DOUBLE,
    wind_speed_mult DOUBLE
"""
SOURCE_TERM_SCHEMA = "scenario_set STRING, site_id STRING, nuclide STRING, median_bq DOUBLE, gsd DOUBLE, source STRING"
SCHEDULE_SCHEMA = "scenario_set STRING, site_id STRING, nuclide STRING, h INT, release_fraction DOUBLE"
Q_SCHEMA = "scenario_set STRING, site_id STRING, nuclide STRING, q_sample_id INT, q_bq DOUBLE"

# Warunek wycinka tabel zapisywanego przez krok `silver` — wszystko POZA zdarzeniami.
# Dzięki temu ponowne przeliczenie klimatologii nie kasuje zapisanych zdarzeń.
NON_EVENT_SETS = f"scenario_set NOT LIKE '{EVENT_PREFIX}%'"


def utc(t: dt.datetime) -> dt.datetime:
    """Datetime ze strefą UTC. PySpark zamienia naiwny datetime według strefy
    systemu operacyjnego — na laptopie w Polsce przesunąłby czas o 1–2 h."""
    return t.replace(tzinfo=dt.timezone.utc) if t.tzinfo is None else t.astimezone(dt.timezone.utc)


def seeded_rng(seed: int, key: str) -> random.Random:
    """Osobny RNG na klucz (crc32 — stabilny między uruchomieniami, w przeciwieństwie
    do hash()). Dodanie nowej lokalizacji nie zmienia scenariuszy istniejących."""
    return random.Random(seed ^ zlib.crc32(key.encode()))


# ----------------------------------------------------------------------------- harmonogram
def uniform_schedule(hours: int) -> list[tuple[int, float]]:
    """Emisja równomierna: każda godzina wypuszcza 1/N całości."""
    return [(h, 1.0 / hours) for h in range(hours)]


def schedule_from_intervals(
    intervals: list[tuple[dt.datetime, dt.datetime, float]], window_start: dt.datetime, window_hours: int
) -> tuple[list[tuple[int, float]], float, float]:
    """Przedziały (start, koniec, tempo Bq/h) → ułamki godzinowe w oknie epizodu.

    Przedział może mieć dowolną długość (np. 30 min albo 3 h) i dowolnie zaczynać się
    względem pełnych godzin — ilość dzielimy proporcjonalnie do czasu nakładania się
    przedziału z każdą godziną okna.

    Zwraca (harmonogram, całkowita ilość w oknie [Bq], ilość POZA oknem [Bq]).
    """
    amounts = [0.0] * window_hours
    outside = 0.0
    w0 = utc(window_start)
    for start, end, rate_bq_h in intervals:
        start, end = utc(start), utc(end)
        if end <= start:
            raise ValueError(f"Przedział uwolnienia kończy się przed początkiem: {start} → {end}")
        total = rate_bq_h * (end - start).total_seconds() / 3600.0
        inside = 0.0
        for h in range(window_hours):
            h0 = w0 + dt.timedelta(hours=h)
            h1 = h0 + dt.timedelta(hours=1)
            overlap = (min(end, h1) - max(start, h0)).total_seconds()
            if overlap > 0:
                amount = rate_bq_h * overlap / 3600.0
                amounts[h] += amount
                inside += amount
        outside += total - inside
    total_in = sum(amounts)
    if total_in <= 0:
        raise ValueError("Harmonogram nie ma żadnego uwolnienia wewnątrz okna epizodu")
    return [(h, a / total_in) for h, a in enumerate(amounts) if a > 0], total_in, outside


# ----------------------------------------------------------------------------- warianty
def climatology_starts(
    meteo_range: list[str], n_episodes: int, episode_hours: int, rng: random.Random, tail_hours: int = 0
) -> list[dt.datetime]:
    """Losowe pełne godziny startu. Epizod + czas śledzenia obłoku po ostatnim uwolnieniu
    (``tail_hours``) musi zmieścić się w zakresie pobranej pogody."""
    start = dt.datetime.fromisoformat(meteo_range[0])
    end = dt.datetime.fromisoformat(meteo_range[1]) + dt.timedelta(days=1)
    max_offset_h = int((end - start).total_seconds() // 3600) - episode_hours - tail_hours
    if max_offset_h <= 0:
        raise ValueError("Zakres meteo krótszy niż jeden epizod")
    return sorted(start + dt.timedelta(hours=rng.randrange(max_offset_h)) for _ in range(n_episodes))


def physical_variants(n: int, heights: list[float], cfg_variants: dict, rng: random.Random) -> list[dict]:
    """Warianty parametrów fizycznych (bez zaburzania pogody).

    Wariant 0 = centralny (bez przesunięć) — używany w demo i jako „najlepsze oszacowanie”.
    Pogody tu nie zaburzamy: w klimatologii niepewność pogody pokrywają RÓŻNE dni.
    """
    out = [make_variant(0, 0, 1.0, 1.0, float(heights[1]))]
    for i in range(1, n):
        out.append(
            make_variant(
                i,
                rng.choices(cfg_variants["stability_shift_choices"], weights=cfg_variants["stability_shift_weights"])[0],
                rng.lognormvariate(0, cfg_variants["deposition_multiplier_sigma"]),
                rng.lognormvariate(0, cfg_variants["washout_multiplier_sigma"]),
                rng.uniform(heights[0], heights[2]),
            )
        )
    return out


def make_variant(i, shift, vd, washout, height, dir_offset=0.0, speed_mult=1.0) -> dict:
    return {
        "variant_id": i, "stability_shift": shift, "vd_mult": vd, "washout_mult": washout,
        "release_height_m": height, "wind_dir_offset_deg": dir_offset, "wind_speed_mult": speed_mult,
    }


# ----------------------------------------------------------------------------- budowa zestawu
@dataclass
class ScenarioTables:
    """Wiersze czterech tabel opisujących zestawy scenariuszy (przed zamianą na DataFrame)."""

    scenarios: list[tuple] = field(default_factory=list)
    source_terms: list[tuple] = field(default_factory=list)
    schedule: list[tuple] = field(default_factory=list)
    q_samples: list[tuple] = field(default_factory=list)

    def add_set(
        self, scenario_set: str, site_id: str, starts: list[dt.datetime], hours: int, variants: list[dict],
        source_terms: dict[str, tuple[float, float, str]], schedules: dict[str, list[tuple[int, float]]],
        n_q: int, seed: int,
    ) -> None:
        """Dodaje zestaw: epizody × warianty, ilości per nuklid i ich harmonogramy."""
        for ep_id, t0 in enumerate(starts):
            for v in variants:
                self.scenarios.append(
                    (scenario_set, site_id, ep_id, utc(t0), hours, v["variant_id"], v["stability_shift"],
                     v["vd_mult"], v["washout_mult"], v["release_height_m"], v["wind_dir_offset_deg"],
                     v["wind_speed_mult"])
                )
        for nuclide, (median, gsd, source) in source_terms.items():
            self.source_terms.append((scenario_set, site_id, nuclide, float(median), float(gsd), source))
            for h, frac in schedules[nuclide]:
                self.schedule.append((scenario_set, site_id, nuclide, int(h), float(frac)))
            rng = seeded_rng(seed, f"Q:{scenario_set}:{site_id}:{nuclide}")
            sigma = math.log(float(gsd))
            for i in range(n_q):
                # Q ~ lognormal(ln(mediana), ln(gsd)) — ilość uwolnienia jest dodatnia
                # i niepewna „mnożnikowo” (×/÷), a nie „addytywnie”.
                self.q_samples.append((scenario_set, site_id, nuclide, i, float(median) * math.exp(rng.gauss(0, sigma))))

    def to_frames(self, spark: SparkSession) -> dict[str, DataFrame]:
        return {
            "scenarios": spark.createDataFrame(self.scenarios, SCENARIO_SCHEMA),
            "source_terms": spark.createDataFrame(self.source_terms, SOURCE_TERM_SCHEMA),
            "release_schedule": spark.createDataFrame(self.schedule, SCHEDULE_SCHEMA),
            "q_samples": spark.createDataFrame(self.q_samples, Q_SCHEMA),
        }


def validation_source(
    site_id: str, site: dict, file_rows: list[dict] | None
) -> tuple[dict[str, tuple[float, float, str]], dict[str, list[tuple[int, float]]]]:
    """Ilości i harmonogram epizodu walidacyjnego.

    Z pliku (Katata 2015 / Terada 2020): harmonogram i całkowita ilość z danych,
    niepewność ``validation.source_term_gsd``. Nuklid, którego nie ma w pliku,
    dostaje ilość z ``sites.yaml`` i profil czasowy pierwszego nuklidu z pliku.
    Bez pliku: emisja równomierna i ilości z ``sites.yaml`` — z ostrzeżeniem, bo
    walidacja testuje wtedy głównie założenie o równomiernej emisji.
    """
    val = site["validation"]
    t0 = dt.datetime.fromisoformat(val["episode_start"])
    hours = val["episode_hours"]
    rows = [r for r in (file_rows or []) if r["site_id"] == site_id]
    terms, schedules = {}, {}
    if not rows:
        log.warning(
            "validation: brak pliku z przebiegiem uwolnienia dla %s w landing/source_term/ — "
            "używam emisji równomiernej (docs/przygotowanie-danych.md, pkt E)", site_id,
        )
        for nuclide, st in site["source_term"].items():
            terms[nuclide] = (st["median_bq"], st["gsd"], "config_uniform")
            schedules[nuclide] = uniform_schedule(hours)
        return terms, schedules

    gsd = float(val.get("source_term_gsd", 2.0))
    by_nuclide: dict[str, list] = {}
    for r in rows:
        by_nuclide.setdefault(r["nuclide"], []).append((r["time_start_utc"], r["time_end_utc"], r["release_rate_bq_h"]))
    for nuclide, intervals in sorted(by_nuclide.items()):
        sched, total, outside = schedule_from_intervals(intervals, t0, hours)
        if outside > 0.01 * total:
            log.warning("validation: %.1f%% uwolnienia %s leży poza oknem epizodu — pominięte",
                        100 * outside / (total + outside), nuclide)
        terms[nuclide] = (total, gsd, "file")
        schedules[nuclide] = sched
    reference = next(iter(schedules.values()))
    for nuclide, st in site["source_term"].items():
        if nuclide not in terms:
            log.warning("validation: brak %s w pliku — ilość z sites.yaml, profil czasowy z pliku", nuclide)
            terms[nuclide] = (st["median_bq"], st["gsd"], "config_file_profile")
            schedules[nuclide] = reference
    return terms, schedules


def build_scenarios(spark: SparkSession, cfg: dict, source_term_rows: list[dict] | None = None) -> dict[str, DataFrame]:
    """Zestawy ``climatology`` i walidacyjne dla wszystkich aktywnych lokalizacji."""
    run = cfg["run"]
    clim = run["climatology"]
    tail = int(cfg["physics"]["transport"]["horizon_hours"])
    tables = ScenarioTables()
    for site_id in run["sites"]:
        site = cfg["sites"][site_id]
        rng = seeded_rng(run["seed"], site_id)
        variants = physical_variants(run["n_param_scenarios"], site["release_height_m"], cfg["physics"]["variants"], rng)

        starts = climatology_starts(clim["meteo_range"], clim["n_episodes"], clim["episode_hours"], rng, tail)
        terms = {n: (st["median_bq"], st["gsd"], "config") for n, st in site["source_term"].items()}
        schedules = {n: uniform_schedule(clim["episode_hours"]) for n in terms}
        tables.add_set(CLIMATOLOGY, site_id, starts, clim["episode_hours"], variants, terms, schedules,
                       run["n_source_samples"], run["seed"])

        if site.get("validation"):
            val = site["validation"]
            v_terms, v_sched = validation_source(site_id, site, source_term_rows)
            tables.add_set(val["scenario_set"], site_id, [dt.datetime.fromisoformat(val["episode_start"])],
                           val["episode_hours"], variants, v_terms, v_sched, run["n_source_samples"], run["seed"])
    return tables.to_frames(spark)
