"""Odpowiedź na pytanie: „czy uwolnienie w lokalizacji X skazi miasto Y?” (plan Część 0, 4.8).

Lokalna, deterministyczna wersja ścieżki liczbowej aplikacji AI:
1. rozpoznanie miasta (z polskimi znakami lub bez),
2. zapytanie SQL do ``gold.city_exposure`` — przepuszczone przez guardrails,
3. odpowiedź po polsku z liczbami WYŁĄCZNIE z wyniku zapytania,
4. zawsze: pokazany SQL i zdanie o ograniczeniach modelu.
Na Databricks krok 2 wykona LLM (text-to-SQL), a kroki 3–4 zostają te same.
"""

from __future__ import annotations

import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from radplume.app.guardrails import validate_select

LIMITATION = (
    "Zastrzeżenie: to wynik prostego modelu gaussowskiego (płaski teren, jednorodny wiatr, "
    "hipotetyczny source term) — narzędzie do porównywania scenariuszy, nie prognoza "
    "ani system wspomagania decyzji kryzysowych."
)


def normalize(name: str) -> str:
    """„Lębork” → „lebork”. Litery ł/Ł nie mają rozkładu Unicode, więc zamieniamy je ręcznie."""
    name = name.replace("ł", "l").replace("Ł", "L")
    decomposed = unicodedata.normalize("NFKD", name)
    return "".join(c for c in decomposed if not unicodedata.combining(c)).strip().lower()


def list_cities(ctx, site_id: str) -> DataFrame:
    return (
        ctx.storage.read("gold", "city_exposure")
        .where(F.col("site_id") == site_id)
        .select("city_name", "country_code", "population", F.round("distance_km", 1).alias("distance_km"))
        .distinct()
        .orderBy("distance_km")
    )


def _resolve_city(ctx, site_id: str, city: str) -> str | None:
    wanted = normalize(city)
    names = [r["city_name"] for r in list_cities(ctx, site_id).collect()]  # kilkadziesiąt wierszy
    for n in names:
        if normalize(n) == wanted:
            return n
    return None


def _fmt_pct(p: float) -> str:
    return f"{p * 100:.1f}%".replace(".", ",")


def _fmt(x: float | None, digits: int = 1) -> str:
    """Format polski: spacja jako separator tysięcy, przecinek dziesiętny (4 576,9)."""
    if x is None:
        return "—"
    return f"{x:,.{digits}f}".replace(",", "\u00a0").replace(".", ",").replace("\u00a0", " ")


def answer_city_question(ctx, site_id: str, city: str, scenario_set: str = "climatology") -> str:
    cfg = ctx.cfg
    if site_id not in cfg["sites"]:
        return f"Nie znam lokalizacji {site_id!r}. Dostępne: {', '.join(cfg['sites'])}."
    if site_id not in cfg["run"]["sites"]:
        # Odmowa zamiast zgadywania (plan P13): tej lokalizacji nie modelowano.
        return f"Lokalizacja {site_id} nie była modelowana w środowisku {cfg['env']} — nie podam liczb."
    city_name = _resolve_city(ctx, site_id, city)
    if city_name is None:
        return (
            f"Miasta {city!r} nie ma w wynikach dla {site_id} (poza promieniem "
            f"{cfg['city_radius_km']} km albo spoza listy miast). Nie podam liczb, których nie policzył model. "
            f"Lista miast: radplume list-cities --site {site_id}"
        )

    table = ctx.storage.sql_ref("gold", "city_exposure")
    # Parametry wstawiamy przez :nazwa (parametryzowane zapytanie Sparka), NIE przez
    # f-string — nazwa miasta od użytkownika nie może wstrzyknąć SQL.
    sql = (
        f"SELECT site_name, city_name, distance_km, bearing_deg, population, threshold_id, threshold_label, "
        f"p_exceed, dep_p05_kbq_m2, dep_p50_kbq_m2, dep_p95_kbq_m2, arrival_h_p50, worst_wind_sector, n_total "
        f"FROM {table} WHERE site_id = :site AND city_name = :city AND scenario_set = :sset "
        f"ORDER BY threshold_id"
    )
    safe_sql = validate_select(sql)
    rows = ctx.spark.sql(safe_sql, args={"site": site_id, "city": city_name, "sset": scenario_set}).collect()
    if not rows:
        return f"Brak wyników dla zestawu scenariuszy {scenario_set!r}."

    r0 = rows[0]
    meteo_src = {
        r["source"]
        for r in ctx.storage.read("silver", "meteo").where(F.col("site_id") == site_id).select("source").distinct().collect()
    }
    lines = [
        f"{r0['site_name']} → {r0['city_name']}: {_fmt(r0['distance_km'])} km, azymut {r0['bearing_deg']:.0f}°, "
        f"ok. {_fmt(r0['population'], 0)} mieszkańców.",
        f"Zestaw scenariuszy: {scenario_set} — {r0['n_total']} kombinacji (pogoda × warianty fizyczne × ilość uwolnienia).",
        "",
    ]
    for r in rows:
        lines.append(f"• {r['threshold_label']}: przekroczony w {_fmt_pct(r['p_exceed'])} scenariuszy")
    lines += [
        "",
        f"Depozycja Cs-137 [kBq/m²]: P5 = {_fmt(r0['dep_p05_kbq_m2'])}, mediana = {_fmt(r0['dep_p50_kbq_m2'])}, "
        f"P95 = {_fmt(r0['dep_p95_kbq_m2'])}",
    ]
    if r0["arrival_h_p50"] is not None:
        lines.append(
            f"Mediana czasu dotarcia smugi: {_fmt(r0['arrival_h_p50'])} h od początku uwolnienia; "
            f"najgroźniejszy wiatr: z sektora {r0['worst_wind_sector']}."
        )
    else:
        lines.append("W żadnym scenariuszu smuga nie dotarła do miasta w znaczącej ilości.")
    if meteo_src != {"open_meteo"}:
        lines.append("UWAGA: dane meteo SYNTETYCZNE (tryb --offline) — liczby nie opisują prawdziwej pogody.")
    lines += ["", LIMITATION, "", "SQL:", safe_sql]
    return "\n".join(lines)
