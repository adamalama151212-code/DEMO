"""Tryb zdarzenia: „wyciek o 13:00 i drugi o 16:00, o znanych ilościach — gdzie pójdzie chmura?”.

Różnica względem klimatologii:
- klimatologia — NIE wiadomo, kiedy dojdzie do awarii → losujemy RÓŻNE dni z historii,
- zdarzenie    — dzień, godziny i ilości są ZNANE → bierzemy pogodę tego dnia i zaburzamy
                 ją o małe wartości (zespół „członków”), bo sama pogoda też jest niepewna:
                 reanaliza ERA5 ma oczka ~25 km, a prognoza — błąd rosnący z czasem.

Każdy członek zespołu to jeden wariant:
- przesunięcie kierunku wiatru ~ N(0, σ_kierunku) — stałe dla całego zdarzenia,
- mnożnik prędkości wiatru ~ lognormal(0, σ_prędkości),
- przesunięcie klasy stabilności, mnożniki depozycji suchej i mokrej, wysokość uwolnienia.
Członek 0 jest niezaburzony („przebieg kontrolny”, jak w prognozach zespołowych).

Zdarzenie zapisuje się jako osobny ``scenario_set`` (``event_<nazwa>``) w tych samych
tabelach co klimatologia — gold, ``radplume ask`` i dashboard działają bez zmian.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from radplume.silver.scenarios import (
    EVENT_PREFIX,
    ScenarioTables,
    make_variant,
    physical_variants,
    schedule_from_intervals,
    seeded_rng,
)

_RELEASE_RE = re.compile(
    r"^\s*(?P<h1>\d{1,2}):(?P<m1>\d{2})(?:\s*-\s*(?P<h2>\d{1,2}):(?P<m2>\d{2}))?\s*=\s*(?P<amount>[0-9.eE+]+)\s*$"
)
_NAME_RE = re.compile(r"^[a-z0-9_]+$")


@dataclass
class Release:
    start_utc: dt.datetime
    end_utc: dt.datetime
    amount_bq: float  # Cs-137


@dataclass
class EventSpec:
    name: str              # pełna nazwa zestawu, np. event_20200115_1300
    site_id: str
    releases: list[Release]

    @property
    def window_start(self) -> dt.datetime:
        """Początek okna = pełna godzina pierwszego uwolnienia."""
        return min(r.start_utc for r in self.releases).replace(minute=0, second=0, microsecond=0)

    @property
    def window_hours(self) -> int:
        """Liczba godzin od początku okna do końca ostatniego uwolnienia (w górę)."""
        end = max(r.end_utc for r in self.releases)
        return max(1, int(-(-(end - self.window_start).total_seconds() // 3600)))


def parse_release(spec: str, date: dt.date, tz: str = "UTC") -> Release:
    """``"13:00=1e15"`` (godzina od 13:00) albo ``"13:00-15:30=2e15"`` (równomiernie w przedziale).

    Godziny podaje się w strefie ``tz`` (np. ``Europe/Warsaw``) i przeliczamy je na UTC —
    tak jak cały projekt. Koniec wcześniejszy niż początek = przejście przez północ.
    Ilość to Cs-137 w Bq; I-131 dobieramy proporcjonalnie (patrz ``build_event_tables``).
    """
    m = _RELEASE_RE.match(spec)
    if not m:
        raise ValueError(f"Niepoprawny zapis wycieku {spec!r}; oczekiwano np. 13:00=1e15 albo 13:00-15:00=2e15")
    zone = ZoneInfo(tz)
    start = dt.datetime.combine(date, dt.time(int(m["h1"]), int(m["m1"])), tzinfo=zone)
    if m["h2"] is not None:
        end = dt.datetime.combine(date, dt.time(int(m["h2"]), int(m["m2"])), tzinfo=zone)
        if end <= start:
            end += dt.timedelta(days=1)
    else:
        end = start + dt.timedelta(hours=1)
    amount = float(m["amount"])
    if amount <= 0:
        raise ValueError(f"Ilość uwolnienia musi być dodatnia: {spec!r}")
    return Release(start.astimezone(dt.timezone.utc), end.astimezone(dt.timezone.utc), amount)


def event_name(date: dt.date, releases: list[Release], name: str | None) -> str:
    if name:
        name = name.lower()
        if not _NAME_RE.match(name):
            raise ValueError("Nazwa zdarzenia może zawierać tylko małe litery, cyfry i _")
        return name if name.startswith(EVENT_PREFIX) else EVENT_PREFIX + name
    first = min(r.start_utc for r in releases)
    return f"{EVENT_PREFIX}{date:%Y%m%d}_{first:%H%M}utc"


def ensemble_members(n: int, site: dict, physics: dict, rng) -> list[dict]:
    """Członkowie zespołu: warianty fizyczne + zaburzenie pogody. Członek 0 = kontrolny."""
    pert = physics["event_perturbation"]
    physical = physical_variants(n, site["release_height_m"], physics["variants"], rng)
    members = [physical[0]]  # kontrolny: bez zaburzeń
    for v in physical[1:]:
        members.append(
            make_variant(
                v["variant_id"], v["stability_shift"], v["vd_mult"], v["washout_mult"], v["release_height_m"],
                dir_offset=rng.gauss(0, pert["wind_dir_sigma_deg"]),
                speed_mult=rng.lognormvariate(0, pert["wind_speed_sigma"]),
            )
        )
    return members


def build_event_tables(cfg: dict, spec: EventSpec, n_members: int) -> ScenarioTables:
    """Cztery tabele scenariuszy dla jednego zdarzenia (jeden epizod × n członków)."""
    site = cfg["sites"][spec.site_id]
    run = cfg["run"]
    rng = seeded_rng(run["seed"], f"{spec.name}:{spec.site_id}")
    members = ensemble_members(n_members, site, cfg["physics"], rng)

    intervals = [(r.start_utc, r.end_utc, r.amount_bq / ((r.end_utc - r.start_utc).total_seconds() / 3600))
                 for r in spec.releases]
    schedule, total_cs, _ = schedule_from_intervals(intervals, spec.window_start, spec.window_hours)

    # Użytkownik podaje ilość Cs-137. Pozostałe nuklidy w tej samej proporcji do Cs-137
    # co w sites.yaml (np. I-131 ≈ 10 × Cs-137 dla awarii typu Fukushima).
    st = site["source_term"]
    cs_median = float(st["Cs-137"]["median_bq"])
    gsd = cfg["physics"]["event_perturbation"]["source_gsd"]
    terms = {n: (total_cs * float(v["median_bq"]) / cs_median, gsd, "event_cli") for n, v in st.items()}
    schedules = {n: schedule for n in terms}

    tables = ScenarioTables()
    tables.add_set(spec.name, spec.site_id, [spec.window_start], spec.window_hours, members, terms, schedules,
                   run["n_source_samples"], run["seed"])
    return tables
