"""Scenariusze Monte Carlo: epizody pogodowe × warianty fizyczne × próbki ilości uwolnienia.

Dwa wymiary niepewności (plan 3.4):
- meteorologiczna — nie wiadomo, KIEDY dojdzie do awarii → losujemy moment
  startu z wieloletniej historii pogody (epizody),
- parametryczna — nie wiadomo, JAK się rozproszy i ILE się uwolni →
  warianty fizyczne (stabilność, depozycja, wysokość) i próbki Q.

Tabele parametrów są małe (dziesiątki–setki wierszy), więc generujemy je
w Pythonie z jednym seedowanym RNG. To NIE łamie zakazu pętli z plan 3.1 —
zakaz dotyczy danych dużych (siatka × godziny × scenariusze), a te liczy Spark.
Dlaczego nie ``F.rand(seed)``: wynik ``rand`` w Sparku zależy od podziału
na partycje, więc zmiana liczby rdzeni zmieniłaby scenariusze i zepsuła
idempotencję. ``random.Random(seed)`` daje zawsze ten sam ciąg.
"""

from __future__ import annotations

import datetime as dt
import math
import random
import zlib

from pyspark.sql import DataFrame, SparkSession

CLIMATOLOGY = "climatology"

SCENARIO_SCHEMA = """
    scenario_set STRING, site_id STRING, episode_id INT, episode_start TIMESTAMP,
    episode_hours INT, variant_id INT, stability_shift INT, vd_mult DOUBLE,
    washout_mult DOUBLE, release_height_m DOUBLE
"""
Q_SCHEMA = "site_id STRING, nuclide STRING, q_sample_id INT, q_bq DOUBLE"


def climatology_starts(meteo_range: list[str], n_episodes: int, episode_hours: int, rng: random.Random) -> list[dt.datetime]:
    """Losowe, pełne godziny startu tak, by cały epizod mieścił się w zakresie meteo."""
    start = dt.datetime.fromisoformat(meteo_range[0])
    end = dt.datetime.fromisoformat(meteo_range[1]) + dt.timedelta(days=1)
    max_offset_h = int((end - start).total_seconds() // 3600) - episode_hours
    if max_offset_h <= 0:
        raise ValueError("Zakres meteo krótszy niż jeden epizod")
    return sorted(start + dt.timedelta(hours=rng.randrange(max_offset_h)) for _ in range(n_episodes))


def physical_variants(n: int, heights: list[float], cfg_variants: dict, rng: random.Random) -> list[dict]:
    """Wariant 0 = centralny (bez przesunięć) — używany w demo i jako „najlepsze oszacowanie”."""
    out = [{"variant_id": 0, "stability_shift": 0, "vd_mult": 1.0, "washout_mult": 1.0, "release_height_m": float(heights[1])}]
    for i in range(1, n):
        out.append(
            {
                "variant_id": i,
                "stability_shift": rng.choices(
                    cfg_variants["stability_shift_choices"], weights=cfg_variants["stability_shift_weights"]
                )[0],
                "vd_mult": rng.lognormvariate(0, cfg_variants["deposition_multiplier_sigma"]),
                "washout_mult": rng.lognormvariate(0, cfg_variants["washout_multiplier_sigma"]),
                "release_height_m": rng.uniform(heights[0], heights[2]),
            }
        )
    return out


def build_scenarios(spark: SparkSession, cfg: dict) -> tuple[DataFrame, DataFrame]:
    """Zwraca (scenarios, q_samples) dla wszystkich aktywnych lokalizacji."""
    run = cfg["run"]
    clim = run["climatology"]
    variants_cfg = cfg["physics"]["variants"]
    rows = []
    for site_id in run["sites"]:
        site = cfg["sites"][site_id]
        # Osobny RNG na lokalizację z seedem zależnym od nazwy (crc32 — stabilny),
        # żeby dodanie nowej lokalizacji nie zmieniało scenariuszy istniejących.
        rng = random.Random(run["seed"] ^ zlib.crc32(site_id.encode()))
        variants = physical_variants(run["n_param_scenarios"], site["release_height_m"], variants_cfg, rng)

        sets = [(CLIMATOLOGY, climatology_starts(clim["meteo_range"], clim["n_episodes"], clim["episode_hours"], rng), clim["episode_hours"])]
        val = site.get("validation")
        if val:
            sets.append((val["scenario_set"], [dt.datetime.fromisoformat(val["episode_start"])], val["episode_hours"]))

        for scenario_set, starts, hours in sets:
            for ep_id, t0 in enumerate(starts):
                # Datetime ze strefą UTC, a nie „naiwny”: PySpark zamienia naiwny datetime
                # według strefy systemu operacyjnego — na laptopie w Polsce przesunąłby
                # epizod o 1–2 h względem Dockera/klastra.
                t0 = t0.replace(tzinfo=dt.timezone.utc)
                for v in variants:
                    rows.append(
                        (scenario_set, site_id, ep_id, t0, hours, v["variant_id"], v["stability_shift"],
                         v["vd_mult"], v["washout_mult"], v["release_height_m"])
                    )

    q_rows = []
    for site_id in run["sites"]:
        rng = random.Random(run["seed"] ^ zlib.crc32(f"Q:{site_id}".encode()))
        for nuclide, st in cfg["sites"][site_id]["source_term"].items():
            sigma = math.log(float(st["gsd"]))
            for i in range(run["n_source_samples"]):
                # Q ~ lognormal(ln(mediana), ln(gsd)) — ilość uwolnienia jest dodatnia
                # i niepewna „mnożnikowo” (×/÷), a nie „addytywnie”.
                q_rows.append((site_id, nuclide, i, float(st["median_bq"]) * math.exp(rng.gauss(0, sigma))))

    return spark.createDataFrame(rows, SCENARIO_SCHEMA), spark.createDataFrame(q_rows, Q_SCHEMA)
