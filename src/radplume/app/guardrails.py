"""Guardrails dla SQL generowanego przez model językowy (plan 4.8).

Wymuszane PARSEREM (sqlglot), a nie promptem — prompt można obejść,
drzewo składniowe nie:
- dokładnie jedno zapytanie (brak „; DROP TABLE …”),
- wyłącznie SELECT (także w podzapytaniach i CTE),
- tylko tabele z białej listy (warstwa gold),
- wymuszony LIMIT (brak przypadkowego zrzutu milionów wierszy).
Dostęp do wierszy i tak ogranicza RLS/CLS w Unity Catalog (zapytania idą
z tożsamością użytkownika) — guardrails to druga, niezależna linia obrony.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

DEFAULT_ALLOWED_TABLES = frozenset(
    {"city_exposure", "risk_map", "site_ranking", "validation", "live_alerts", "sensor_dq_summary"}
)
# Węzły, które coś ZMIENIAJĄ lub wykonują polecenia — niedozwolone nigdzie w drzewie.
_FORBIDDEN = (
    exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter,
    exp.Merge, exp.Command, exp.Grant, exp.TruncateTable,
)


class UnsafeQueryError(ValueError):
    """Zapytanie odrzucone przez guardrails — pokazujemy powód użytkownikowi."""


def _table_name(t: exp.Table) -> str:
    """Nazwa tabeli do porównania z białą listą + kontrola, że to warstwa gold.

    Katalog pomijamy (ten sam SQL działa na radplume_dev i radplume_prod),
    ale schemat, jeśli podany, MUSI być ``gold`` — inaczej ``silver.city_exposure``
    przeszłoby po samej nazwie. Tabele ścieżkowe (lokalnie ``delta.`/…/gold/x```)
    sprawdzamy po dwóch ostatnich elementach ścieżki.
    """
    name = t.name
    if "/" in name:
        parts = name.rstrip("/").split("/")
        if len(parts) < 2 or parts[-2].lower() != "gold":
            raise UnsafeQueryError(f"Dozwolone są tylko tabele warstwy gold: {name}")
        return parts[-1].lower()
    if t.db and t.db.lower() != "gold":
        raise UnsafeQueryError(f"Dozwolone są tylko tabele warstwy gold: {t.db}.{name}")
    return name.lower()


def validate_select(sql: str, allowed_tables: frozenset[str] = DEFAULT_ALLOWED_TABLES, max_limit: int = 1000) -> str:
    """Zwraca bezpieczną wersję zapytania albo rzuca ``UnsafeQueryError``."""
    try:
        statements = [s for s in sqlglot.parse(sql, read="databricks") if s is not None]
    except sqlglot.errors.ParseError as e:
        raise UnsafeQueryError(f"Niepoprawna składnia SQL: {e}") from e
    if len(statements) != 1:
        raise UnsafeQueryError("Dozwolone jest dokładnie jedno zapytanie")
    stmt = statements[0]

    if not isinstance(stmt, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        raise UnsafeQueryError(f"Dozwolony jest tylko SELECT (otrzymano {type(stmt).__name__})")
    for node in stmt.walk():
        if isinstance(node, _FORBIDDEN):
            raise UnsafeQueryError(f"Niedozwolona operacja: {type(node).__name__}")

    cte_names = {c.alias_or_name.lower() for c in stmt.find_all(exp.CTE)}
    for t in stmt.find_all(exp.Table):
        name = _table_name(t)
        if name not in allowed_tables and name not in cte_names:
            raise UnsafeQueryError(f"Tabela spoza białej listy: {name}")

    limit = stmt.args.get("limit")
    if limit is None:
        stmt = stmt.limit(max_limit)
    else:
        try:
            value = int(limit.expression.name)
        except (AttributeError, ValueError):
            raise UnsafeQueryError("LIMIT musi być liczbą") from None
        if value > max_limit:
            stmt = stmt.limit(max_limit)
    return stmt.sql(dialect="databricks")
