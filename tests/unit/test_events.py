"""Tryb zdarzenia: parsowanie wycieków, strefy czasowe, zespół członków (plan rozszerzeń, etap 2)."""

import datetime as dt

import pytest

from radplume.silver.events import EventSpec, build_event_tables, event_name, parse_release

DAY = dt.date(2020, 1, 15)
UTC = dt.timezone.utc


def test_single_hour_release_in_utc():
    r = parse_release("13:00=1e15", DAY)
    assert r.start_utc == dt.datetime(2020, 1, 15, 13, tzinfo=UTC)
    assert r.end_utc == dt.datetime(2020, 1, 15, 14, tzinfo=UTC)
    assert r.amount_bq == 1e15


def test_polish_winter_time_is_converted_to_utc():
    # 13:00 czasu polskiego w styczniu (CET, UTC+1) = 12:00 UTC
    r = parse_release("13:00=1e15", DAY, "Europe/Warsaw")
    assert r.start_utc == dt.datetime(2020, 1, 15, 12, tzinfo=UTC)


def test_polish_summer_time_is_converted_to_utc():
    # w lipcu CEST = UTC+2
    r = parse_release("13:00=1e15", dt.date(2020, 7, 15), "Europe/Warsaw")
    assert r.start_utc.hour == 11


def test_interval_across_midnight():
    r = parse_release("23:00-02:00=3e15", DAY)
    assert r.end_utc - r.start_utc == dt.timedelta(hours=3)


@pytest.mark.parametrize("bad", ["13=1e15", "13:00", "13:00=abc", "25:00=1e15", "13:00=-1e15"])
def test_bad_release_spec_is_rejected(bad):
    with pytest.raises(ValueError):
        parse_release(bad, DAY)


def test_event_name():
    rel = [parse_release("16:00=1e15", DAY), parse_release("13:00=1e15", DAY)]
    assert event_name(DAY, rel, None) == "event_20200115_1300utc"
    assert event_name(DAY, rel, "Test_Gdansk") == "event_test_gdansk"
    with pytest.raises(ValueError):
        event_name(DAY, rel, "zła nazwa!")


def test_window_covers_both_releases():
    spec = EventSpec("event_x", "lubiatowo_kopalino",
                     [parse_release("13:30=1e15", DAY), parse_release("16:00-18:00=5e15", DAY)])
    assert spec.window_start == dt.datetime(2020, 1, 15, 13, tzinfo=UTC)
    assert spec.window_hours == 5  # 13:00 … 18:00


def test_event_tables(cfg):
    spec = EventSpec("event_x", "lubiatowo_kopalino",
                     [parse_release("13:00=1e15", DAY), parse_release("16:00=3e15", DAY)])
    t = build_event_tables(cfg, spec, n_members=20)
    assert len(t.scenarios) == 20  # jeden epizod × 20 członków
    # członek 0 = kontrolny: bez zaburzenia pogody
    assert t.scenarios[0][10:12] == (0.0, 1.0)
    offsets = [row[10] for row in t.scenarios[1:]]
    assert any(abs(o) > 1 for o in offsets)
    # harmonogram: 25% w godzinie 0 (13:00), 75% w godzinie 3 (16:00), tak samo dla każdego nuklidu
    sched = {(n, h): f for _, _, n, h, f in t.schedule}
    assert sched[("Cs-137", 0)] == pytest.approx(0.25) and sched[("Cs-137", 3)] == pytest.approx(0.75)
    # ilość: Cs-137 = suma podana; I-131 w proporcji z sites.yaml (×10)
    terms = {n: m for _, _, n, m, _, _ in t.source_terms}
    assert terms["Cs-137"] == pytest.approx(4e15)
    assert terms["I-131"] == pytest.approx(4e16)
    # deterministycznie
    assert build_event_tables(cfg, spec, n_members=20).scenarios == t.scenarios
