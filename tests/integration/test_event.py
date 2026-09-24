"""Tryb zdarzenia end-to-end: pobranie pogody, zespół, gold, bez niszczenia klimatologii."""

import datetime as dt
import os

import pytest
from pyspark.sql import functions as F

from radplume.pipelines.batch import BATCH_STEPS
from radplume.pipelines.event import event_summary, run_event
from radplume.silver.events import EventSpec, parse_release


@pytest.fixture(scope="module")
def event_ctx(spark, tmp_path_factory):
    from radplume.core.config import load_config
    from radplume.core.storage import Storage
    from radplume.pipelines.context import Context
    from tests.conftest import TEST_OVERRIDES

    os.environ["RADPLUME_DATA_DIR"] = str(tmp_path_factory.mktemp("event") / "data")
    cfg = load_config("local", TEST_OVERRIDES)
    ctx = Context(cfg, spark, Storage(spark, cfg))
    for s in ("ingest-meteo", "ingest-cities", "bronze", "silver", "dispersion", "gold"):
        BATCH_STEPS[s](ctx)
    yield ctx
    os.environ.pop("RADPLUME_DATA_DIR", None)


def test_event_end_to_end(event_ctx):
    st = event_ctx.storage
    clim_before = st.read("gold", "city_exposure").where("scenario_set = 'climatology'").count()
    # Dzień POZA zakresem klimatologii (2020-01..02) → pogoda musi zostać dociągnięta.
    day = dt.date(2021, 6, 1)
    spec = EventSpec("event_test", "lubiatowo_kopalino",
                     [parse_release("13:00=1e15", day), parse_release("16:00=5e15", day)])
    run_event(event_ctx, spec, n_members=6)

    meteo_days = st.read("silver", "meteo").where(F.to_date("time_utc") == F.lit(day)).count()
    assert meteo_days == 24
    ce = st.read("gold", "city_exposure")
    assert ce.where("scenario_set = 'event_test'").count() > 0
    # klimatologia nietknięta
    assert ce.where("scenario_set = 'climatology'").count() == clim_before
    # mianownik: 6 członków × 5 próbek Q
    assert {r["n_total"] for r in ce.where("scenario_set = 'event_test'").select("n_total").distinct().collect()} == {30}
    assert "event_test" in event_summary(event_ctx, spec)


def test_event_rerun_is_idempotent(event_ctx):
    st = event_ctx.storage
    day = dt.date(2021, 6, 1)
    spec = EventSpec("event_test", "lubiatowo_kopalino",
                     [parse_release("13:00=1e15", day), parse_release("16:00=5e15", day)])
    before = st.read("silver", "dispersion_episode").where("scenario_set = 'event_test'").count()
    run_event(event_ctx, spec, n_members=6)
    assert st.read("silver", "dispersion_episode").where("scenario_set = 'event_test'").count() == before
    assert st.read("silver", "scenarios").where("scenario_set = 'event_test'").count() == 6
