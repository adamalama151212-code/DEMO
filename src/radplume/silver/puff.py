"""MODEL „puff”: obłok z każdej godziny uwolnienia, przesuwany wiatrem zmieniającym się w czasie.

Dlaczego (plan rozszerzeń, etap 3): w modelu „straight” każda godzina uwolnienia to prosta
smuga lecąca w kierunku wiatru z TEJ godziny przez cały zasięg. Przy 5 m/s chmura potrzebuje
~5 h na 100 km, a wiatr w tym czasie się zmienia — prawdziwa chmura skręca, prosta smuga nie.

Jak liczymy:
1. Masa uwolniona w godzinie h (ułamek z harmonogramu) to jeden obłok, startujący w połowie
   tej godziny.
2. Obłok przesuwa się krokami ``step_minutes`` (domyślnie 15 min). W każdym kroku bierzemy
   wiatr z godziny, w której krok się zaczyna (z zaburzeniem członka zespołu). Położenie
   = suma narastająca przesunięć — trajektoria łamana, która skręca razem z wiatrem.
3. Rozmycie σy, σz liczymy wzorami Briggsa od PRZEBYTEJ DROGI (a nie od odległości od źródła),
   w punkcie odcinka najbliższym komórce, i pilnujemy, żeby nie malało wzdłuż trajektorii
   (zmiana klasy stabilności nie „ściska” chmury).
4. Depozycję w komórce od jednego odcinka liczymy ANALITYCZNIE: obłok gaussowski przesuwający
   się ze stałą prędkością wzdłuż odcinka daje w punkcie dawkę całkowaną po czasie równą
   wzorowi smugi (te same, przetestowane funkcje z ``dispersion.py``) × ΔΦ, gdzie
   ΔΦ = Φ(a/σ) − Φ((a−L)/σ) to część obłoku, która „przeszła” obok punktu na tym odcinku
   (a — położenie punktu wzdłuż odcinka, L — długość odcinka).
   Dzięki temu nie ma „dziur” między kolejnymi pozycjami obłoku, a przy stałym wietrze
   suma po odcinkach daje DOKŁADNIE model „straight” (test w tests/unit/test_puff.py).
5. Dla każdego odcinka bierzemy tylko komórki z prostokąta wokół niego (±4σy) — indeksy
   siatki liczymy wzorem, bez joinu z całą siatką. To klucz do skalowania.

Ograniczenia (zapisane w dokumentacji): wiatr jednorodny w przestrzeni (zmienny tylko w czasie),
jedna wysokość uwolnienia, brak zubożenia obłoku przez depozycję (jak w modelu straight).
"""

from __future__ import annotations

import math

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from radplume.silver.dispersion import (
    briggs_sigmas,
    column_per_rate,
    episode_hours,
    ground_concentration_per_rate,
    perturbed_wind,
    washout_coeff,
)

# Współczynniki przybliżenia erf (Abramowitz & Stegun 7.1.26), błąd bezwzględny < 1,5e-7.
# Spark SQL nie ma funkcji erf, a Python UDF jest zakazany w silver (plan 3.1).
_ERF_P = 0.3275911
_ERF_A = (0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429)


def erf(x: Column) -> Column:
    ax = F.abs(x)
    t = F.lit(1.0) / (F.lit(1.0) + F.lit(_ERF_P) * ax)
    poly = F.lit(0.0)
    for a in reversed(_ERF_A):  # schemat Hornera: t·(a1 + t·(a2 + …))
        poly = (poly + F.lit(a)) * t
    y = F.lit(1.0) - poly * F.exp(-ax * ax)
    return F.when(x >= 0, y).otherwise(-y)


def norm_cdf(z: Column) -> Column:
    """Φ(z) — dystrybuanta rozkładu normalnego."""
    return F.lit(0.5) * (F.lit(1.0) + erf(z / F.lit(math.sqrt(2.0))))


_PUFF_KEYS = ["scenario_set", "site_id", "episode_id", "variant_id", "h"]


def trajectories(meteo: DataFrame, scenarios: DataFrame, schedule: DataFrame, physics: dict) -> DataFrame:
    """Odcinki trajektorii wszystkich obłoków: początek (x0, y0), kierunek, długość, σ.

    Jeden wiersz = jeden krok jednego obłoku. Trajektoria nie zależy od nuklidu, więc liczymy
    ją raz (nuklidy dochodzą dopiero przy depozycji).
    """
    tr = physics["transport"]
    step_s = int(tr["step_minutes"]) * 60
    n_steps = int(tr["horizon_hours"]) * 3600 // step_s

    rel_hours = schedule.select("scenario_set", "site_id", "h").distinct()
    puffs = scenarios.join(
        episode_hours(scenarios).join(rel_hours, ["scenario_set", "site_id", "h"]),
        on=["scenario_set", "site_id", "episode_id", "episode_start", "episode_hours"],
    ).drop("time_utc")

    # Obłok startuje w połowie godziny uwolnienia (środek masy uwolnionej w tej godzinie).
    steps = (
        puffs.withColumn("k", F.explode(F.sequence(F.lit(0), F.lit(n_steps - 1))))
        .withColumn("age_s", F.col("k") * step_s)  # wiek obłoku na początku kroku
        .withColumn("t_s", F.unix_timestamp("episode_start") + F.col("h") * 3600 + 1800 + F.col("age_s"))
        .withColumn("time_utc", F.timestamp_seconds(F.floor(F.col("t_s") / 3600) * 3600))
    )
    steps = steps.join(
        meteo.select("site_id", "time_utc", "wind_speed_ms", "wind_from_deg", "precip_mm_h", "stability_idx"),
        on=["site_id", "time_utc"],
        how="left",
    )
    w = Window.partitionBy(*_PUFF_KEYS).orderBy("k")
    upto = w.rowsBetween(Window.unboundedPreceding, 0)
    before = w.rowsBetween(Window.unboundedPreceding, -1)

    # Brak pogody w którejś godzinie = koniec śledzenia obłoku (nie zgadujemy wiatru).
    steps = steps.withColumn("_gap", F.max(F.col("wind_speed_ms").isNull().cast("int")).over(upto))
    steps = perturbed_wind(steps.where(F.col("_gap") == 0), physics)

    phi = F.radians("plume_to_p")
    seg = (
        steps.withColumn("ux", F.sin(phi))
        .withColumn("uy", F.cos(phi))
        .withColumn("seg_len", F.col("u_h") * F.lit(float(step_s)))
        .withColumn("x0", F.coalesce(F.sum(F.col("ux") * F.col("seg_len")).over(before), F.lit(0.0)))
        .withColumn("y0", F.coalesce(F.sum(F.col("uy") * F.col("seg_len")).over(before), F.lit(0.0)))
        .withColumn("s0", F.coalesce(F.sum("seg_len").over(before), F.lit(0.0)))
    )
    # σ na KOŃCU odcinka — górne ograniczenie rozmycia na tym odcinku (do wyboru komórek).
    # Rozmycie nie maleje wzdłuż trajektorii (zmiana klasy stabilności nie „ściska” chmury),
    # dlatego bierzemy maksimum narastające; `sigma_*_prev` to wartość z końca poprzedniego odcinka.
    sy, sz = briggs_sigmas(F.col("s0") + F.col("seg_len"), F.col("stab"))
    return (
        seg.withColumn("_sy", sy)
        .withColumn("_sz", sz)
        .withColumn("sigma_y", F.max("_sy").over(upto))
        .withColumn("sigma_z", F.max("_sz").over(upto))
        .withColumn("sigma_y_prev", F.coalesce(F.max("_sy").over(before), F.lit(0.0)))
        .withColumn("sigma_z_prev", F.coalesce(F.max("_sz").over(before), F.lit(0.0)))
        .drop("_sy", "_sz", "_gap")
    )


def puff_unit_deposition(
    meteo: DataFrame,
    scenarios: DataFrame,
    schedule: DataFrame,
    grid: DataFrame,
    nuclides: DataFrame,
    physics: dict,
) -> DataFrame:
    """Depozycja [Bq/m² na 1 Bq całkowitego uwolnienia] — ten sam schemat co model „straight”."""
    cutoff = float(physics["crosswind_cutoff_sigmas"])
    seg = trajectories(meteo, scenarios, schedule, physics)

    # Parametry siatki per lokalizacja (mała tabela): połowa szerokości i rozmiar komórki.
    gp = grid.groupBy("site_id").agg(
        ((F.max("ix") - F.min("ix")) / 2).alias("half"),
        F.first("cell_km").alias("cell_km"),
        F.max("ix").alias("n_max"),
    )
    seg = seg.join(F.broadcast(gp), "site_id")

    # Prostokąt komórek wokół odcinka (±cutoff·σy), w indeksach siatki.
    margin = F.lit(cutoff) * F.col("sigma_y")
    x1, y1 = F.col("x0") + F.col("ux") * F.col("seg_len"), F.col("y0") + F.col("uy") * F.col("seg_len")
    cell_m = F.col("cell_km") * 1000.0

    def idx_lo(lo: Column) -> Column:
        return F.greatest(F.ceil((lo - margin) / cell_m + F.col("half")), F.lit(0)).cast("int")

    def idx_hi(hi: Column) -> Column:
        return F.least(F.floor((hi + margin) / cell_m + F.col("half")), F.col("n_max")).cast("int")

    seg = (
        seg.withColumn("ix_lo", idx_lo(F.least(F.col("x0"), x1)))
        .withColumn("ix_hi", idx_hi(F.greatest(F.col("x0"), x1)))
        .withColumn("iy_lo", idx_lo(F.least(F.col("y0"), y1)))
        .withColumn("iy_hi", idx_hi(F.greatest(F.col("y0"), y1)))
        # Odcinek całkowicie poza domeną (obłok już wyleciał) → brak komórek.
        .where((F.col("ix_lo") <= F.col("ix_hi")) & (F.col("iy_lo") <= F.col("iy_hi")))
    )
    cells = (
        seg.withColumn("ix", F.explode(F.sequence("ix_lo", "ix_hi")))
        .withColumn("iy", F.explode(F.sequence("iy_lo", "iy_hi")))
        .withColumn("cx", (F.col("ix") - F.col("half")) * cell_m)
        .withColumn("cy", (F.col("iy") - F.col("half")) * cell_m)
    )
    # Komórka ze źródłem: wzór gaussowski nie ma sensu w bliskim polu (jak w modelu straight).
    cells = cells.where(F.sqrt(F.col("cx") ** 2 + F.col("cy") ** 2) >= physics["min_downwind_distance_m"])

    # Położenie komórki względem odcinka: a — wzdłuż, c — w poprzek.
    px, py = F.col("cx") - F.col("x0"), F.col("cy") - F.col("y0")
    cells = cells.withColumn("a", px * F.col("ux") + py * F.col("uy")).withColumn(
        "c", px * F.col("uy") - py * F.col("ux")
    )
    cells = cells.where(F.abs(F.col("c")) <= F.lit(cutoff) * F.col("sigma_y"))
    # σ w punkcie odcinka najbliższym komórce (przebyta droga s0 + a), a nie w środku odcinka:
    # przy odcinkach po kilka km różnica w σ dawała ~20% błędu stężenia blisko źródła.
    a_on_seg = F.least(F.greatest(F.col("a"), F.lit(0.0)), F.col("seg_len"))
    sy_pt, sz_pt = briggs_sigmas(F.col("s0") + a_on_seg, F.col("stab"))
    cells = cells.withColumn("sy", F.greatest(sy_pt, F.col("sigma_y_prev"))).withColumn(
        "sz", F.greatest(sz_pt, F.col("sigma_z_prev"))
    )
    dphi = norm_cdf(F.col("a") / F.col("sy")) - norm_cdf((F.col("a") - F.col("seg_len")) / F.col("sy"))
    cells = cells.withColumn("dphi", dphi).where(F.col("dphi") > 1e-9)

    d = cells.crossJoin(F.broadcast(nuclides)).join(
        F.broadcast(schedule), on=["scenario_set", "site_id", "nuclide", "h"]
    )
    # Wiek obłoku w chwili przejścia obok komórki (do rozpadu i czasu dotarcia).
    pass_s = F.col("age_s") + F.least(F.greatest(F.col("a"), F.lit(0.0)), F.col("seg_len")) / F.col("u_h")
    chi = ground_concentration_per_rate(F.col("u_h"), F.col("c"), F.col("release_height_m"), F.col("sy"), F.col("sz"))
    column = column_per_rate(F.col("u_h"), F.col("c"), F.col("sy"))
    frac = F.col("release_fraction") * F.col("dphi")
    dry = F.col("vd_ms") * F.col("vd_mult") * chi * frac
    wet = washout_coeff() * column * frac
    decay = F.exp(-F.col("decay_const") * pass_s)

    return d.select(
        "scenario_set", "site_id", "episode_id", "variant_id", "episode_start", "h",
        F.concat_ws("_", "site_id", F.col("ix").cast("string"), F.col("iy").cast("string")).alias("cell_id"),
        "nuclide", F.col("wind_from_p").alias("wind_from_deg"), "groundshine",
        ((dry + wet) * decay).alias("dep_hour"),
        (F.col("h") + (F.lit(1800.0) + pass_s) / 3600.0).alias("arrival_h"),
    )
