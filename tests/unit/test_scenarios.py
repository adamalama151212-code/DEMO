"""Scenariusze i harmonogram uwolnienia (plan rozszerzeń, etap 1)."""

import datetime as dt

import pytest

from radplume.silver.scenarios import (
    ScenarioTables,
    schedule_from_intervals,
    uniform_schedule,
    validation_source,
)

T0 = dt.datetime(2011, 3, 12, 6, 0, tzinfo=dt.timezone.utc)


def _h(hours, minutes=0):
    return T0 + dt.timedelta(hours=hours, minutes=minutes)


def test_uniform_schedule_sums_to_one():
    s = uniform_schedule(24)
    assert len(s) == 24
    assert sum(f for _, f in s) == pytest.approx(1.0)


def test_interval_split_across_hours():
    # 1e12 Bq/h od 06:30 do 08:00 → 0,5e12 w godzinie 0 i 1e12 w godzinie 1
    sched, total, outside = schedule_from_intervals([(_h(0, 30), _h(2), 1e12)], T0, 4)
    assert total == pytest.approx(1.5e12)
    assert outside == 0
    assert dict(sched) == pytest.approx({0: 1 / 3, 1: 2 / 3})


def test_two_separate_releases():
    # „wyciek o 13 i drugi o 16”: dwa jednogodzinne zrzuty o różnej wielkości
    sched, total, _ = schedule_from_intervals([(_h(1), _h(2), 1e15), (_h(4), _h(5), 3e15)], T0, 6)
    assert dict(sched) == pytest.approx({1: 0.25, 4: 0.75})
    assert total == pytest.approx(4e15)


def test_release_outside_window_is_reported():
    _, total, outside = schedule_from_intervals([(_h(-2), _h(2), 1e12)], T0, 4)
    assert total == pytest.approx(2e12)
    assert outside == pytest.approx(2e12)


def test_empty_window_is_an_error():
    with pytest.raises(ValueError):
        schedule_from_intervals([(_h(10), _h(11), 1e12)], T0, 4)


def test_validation_without_file_falls_back_to_uniform(cfg):
    site = cfg["sites"]["fukushima_daiichi"]
    terms, sched = validation_source("fukushima_daiichi", site, None)
    assert terms["Cs-137"][2] == "config_uniform"
    assert len(sched["Cs-137"]) == site["validation"]["episode_hours"]


def test_validation_with_file_uses_file_amounts(cfg):
    site = cfg["sites"]["fukushima_daiichi"]
    rows = [
        {"site_id": "fukushima_daiichi", "nuclide": "Cs-137", "time_start_utc": _h(2).replace(tzinfo=None),
         "time_end_utc": _h(3).replace(tzinfo=None), "release_rate_bq_h": 4e15},
    ]
    terms, sched = validation_source("fukushima_daiichi", site, rows)
    assert terms["Cs-137"][0] == pytest.approx(4e15)
    assert terms["Cs-137"][2] == "file"
    assert dict(sched["Cs-137"]) == pytest.approx({2: 1.0})
    # I-131 nie ma w pliku → ilość z konfiguracji, profil czasowy z pliku
    assert terms["I-131"][2] == "config_file_profile"
    assert dict(sched["I-131"]) == pytest.approx({2: 1.0})


def test_q_samples_are_per_set_and_deterministic():
    def build():
        t = ScenarioTables()
        v = [{"variant_id": 0, "stability_shift": 0, "vd_mult": 1.0, "washout_mult": 1.0,
              "release_height_m": 50.0, "wind_dir_offset_deg": 0.0, "wind_speed_mult": 1.0}]
        for name, median in (("climatology", 1e16), ("event_x", 1e14)):
            t.add_set(name, "s", [T0], 2, v, {"Cs-137": (median, 2.0, "x")}, {"Cs-137": uniform_schedule(2)}, 50, 42)
        return t

    a, b = build(), build()
    assert a.q_samples == b.q_samples
    clim = sorted(q for s, _, _, _, q in a.q_samples if s == "climatology")
    event = sorted(q for s, _, _, _, q in a.q_samples if s == "event_x")
    # mediany próbek odpowiadają medianom zestawów (różnica 100×)
    assert clim[25] / event[25] == pytest.approx(100, rel=0.5)


def test_source_term_file_is_read_in_utc(spark, tmp_path):
    """Plik z przebiegiem uwolnienia (format z przygotowanie-danych.md, pkt E) → wiersze dla silver."""
    from radplume.bronze.source_term import read_source_term

    (tmp_path / "_zrodlo").mkdir()
    (tmp_path / "_zrodlo" / "oryginal.csv").write_text("to nie jest czytane\n")
    (tmp_path / "katata.csv").write_text(
        "site_id,nuclide,time_start_utc,time_end_utc,release_rate_bq_h,release_height_m,source\n"
        "fukushima_daiichi,Cs-137,2011-03-12T06:00:00,2011-03-12T07:30:00,2.0e+13,,test\n"
    )
    rows = [r.asDict() for r in read_source_term(spark, str(tmp_path)).collect()]
    assert len(rows) == 1  # oryginał w _zrodlo/ pominięty
    r = rows[0]
    assert r["time_start_utc"] == dt.datetime(2011, 3, 12, 6, 0)  # proces w UTC
    sched, total, _ = schedule_from_intervals(
        [(r["time_start_utc"], r["time_end_utc"], r["release_rate_bq_h"])], T0, 4
    )
    assert total == pytest.approx(3e13)
    assert dict(sched) == pytest.approx({0: 2 / 3, 1: 1 / 3})
