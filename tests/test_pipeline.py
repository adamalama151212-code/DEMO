"""Potok batch end-to-end w małej skali: poprawność, statystyka MC i idempotencja (plan 4.5)."""

import hashlib

import pytest
from pyspark.sql import functions as F

from radplume.gold.aggregates import nearest_rank
from radplume.pipeline_batch import BATCH_STEPS

STEPS = ["ingest-meteo", "ingest-cities", "bronze", "silver", "dispersion", "gold"]


def _run(ctx):
    for s in STEPS:
        BATCH_STEPS[s](ctx)


def _checksum(df):
    """Suma kontrolna tabeli niezależna od kolejności wierszy i partycji."""
    rows = sorted(str(sorted(r.asDict().items())) for r in df.collect())
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


@pytest.fixture(scope="module")
def pipeline_ctx(spark, tmp_path_factory):
    import os

    from radplume.config import load_config
    from radplume.pipeline_batch import Context
    from radplume.storage import Storage
    from tests.conftest import TEST_OVERRIDES

    os.environ["RADPLUME_DATA_DIR"] = str(tmp_path_factory.mktemp("pipe") / "data")
    cfg = load_config("local", TEST_OVERRIDES)
    ctx = Context(cfg, spark, Storage(spark, cfg))
    _run(ctx)
    yield ctx
    os.environ.pop("RADPLUME_DATA_DIR", None)


def test_rerun_is_idempotent(pipeline_ctx):
    """Dwa przebiegi → identyczne sumy kontrolne gold i brak zdublowanych godzin w bronze."""
    st = pipeline_ctx.storage
    before = {t: _checksum(st.read("gold", t)) for t in ("risk_map", "city_exposure", "site_ranking")}
    n_bronze = st.read("bronze", "meteo").count()
    _run(pipeline_ctx)
    after = {t: _checksum(st.read("gold", t)) for t in ("risk_map", "city_exposure", "site_ranking")}
    assert before == after
    assert st.read("bronze", "meteo").count() == n_bronze


def test_every_city_in_radius_has_an_answer(pipeline_ctx):
    """Także miasto, do którego smuga nie dotarła, ma wiersz (p = 0) — „0%” to odpowiedź."""
    st, cfg = pipeline_ctx.storage, pipeline_ctx.cfg
    cities = st.read("silver", "city_cells").select("city_id").distinct().count()
    per_threshold = (
        st.read("gold", "city_exposure")
        .where("scenario_set = 'climatology'")
        .groupBy("threshold_id").agg(F.countDistinct("city_id").alias("n"))
        .collect()
    )
    assert cities > 0
    assert {r["n"] for r in per_threshold} == {cities}
    assert len(per_threshold) == len(cfg["thresholds"])


def test_probabilities_are_valid(pipeline_ctx):
    df = pipeline_ctx.storage.read("gold", "risk_map")
    bad = df.where((F.col("p_exceed") < 0) | (F.col("p_exceed") > 1) | (F.col("dep_p05_kbq_m2") > F.col("dep_p95_kbq_m2")))
    assert bad.count() == 0


def test_stricter_threshold_never_more_likely(pipeline_ctx):
    """P(depozycja ≥ 555) ≤ P(depozycja ≥ 37) dla każdego miasta — spójność progów."""
    ce = pipeline_ctx.storage.read("gold", "city_exposure")
    a = ce.where("threshold_id = 'cs137_contaminated'").select("scenario_set", "city_id", F.col("p_exceed").alias("p37"))
    b = ce.where("threshold_id = 'cs137_strict_control'").select("scenario_set", "city_id", F.col("p_exceed").alias("p555"))
    assert a.join(b, ["scenario_set", "city_id"]).where("p555 > p37").count() == 0


def test_nearest_rank_counts_implicit_zeros(spark):
    """Scenariusze „nie dotarła” (brak wiersza) to zera — muszą przesunąć percentyle w dół."""
    df = spark.createDataFrame([([5.0, 10.0], 10)], "vals ARRAY<DOUBLE>, n_total INT")
    r = df.select(
        nearest_rank(F.col("vals"), F.col("n_total"), 0.5).alias("p50"),
        nearest_rank(F.col("vals"), F.col("n_total"), 0.95).alias("p95"),
    ).first()
    assert r["p50"] == 0.0     # 8 z 10 scenariuszy to zera
    assert r["p95"] == 10.0
