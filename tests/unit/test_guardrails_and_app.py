"""Guardrails SQL (plan 4.8) i rozpoznawanie nazw miast."""

import pytest

from radplume.app.city_query import normalize
from radplume.app.guardrails import UnsafeQueryError, validate_select


def test_select_gets_limit():
    out = validate_select("SELECT city_name FROM radplume_prod.gold.city_exposure")
    assert out.endswith("LIMIT 1000")


def test_large_limit_is_capped():
    assert validate_select("SELECT * FROM gold.city_exposure LIMIT 999999").endswith("LIMIT 1000")


def test_local_path_table_in_gold_is_allowed():
    validate_select("SELECT * FROM delta.`/data/delta/gold/city_exposure`")


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE gold.city_exposure",
        "DELETE FROM gold.city_exposure",
        "SELECT 1; DROP TABLE gold.city_exposure",
        "SELECT * FROM silver.meteo",                         # spoza gold
        "SELECT * FROM silver.city_exposure",                 # ta sama nazwa, zła warstwa
        "SELECT * FROM delta.`/data/delta/silver/meteo`",
        "SELECT * FROM gold.city_exposure UNION SELECT * FROM secrets",
        "SELECT * FROM (SELECT * FROM bronze.meteo) t",       # podzapytanie
        "INSERT INTO gold.city_exposure SELECT * FROM gold.city_exposure",
    ],
)
def test_unsafe_queries_are_blocked(sql):
    with pytest.raises(UnsafeQueryError):
        validate_select(sql)


@pytest.mark.parametrize("raw, norm", [("Lębork", "lebork"), ("ŁEBA", "leba"), ("Władysławowo", "wladyslawowo"), (" Gdańsk ", "gdansk")])
def test_city_name_normalization(raw, norm):
    assert normalize(raw) == norm
