"""Generator czujników (determinizm, zgodność z modelem) i rejestr CDC → SCD2."""

import datetime as dt
import math

from radplume.ingest.device_registry import cdc_events, plan_devices
from radplume.ingest.sensor_sim import SensorSimulator
from radplume.silver.devices_cdc import CDC_SCHEMA, apply_scd2

START = dt.datetime(2011, 3, 12, 12, 0)
SITE = {"site_id": "s", "lat": 37.42, "lon": 141.03, "jurisdiction_code": "JP-07"}


def _devices():
    plume = [{"cell_id": f"p{i}", "x_km": 5.0 * i, "y_km": 0.0} for i in range(4)]
    bg = [{"cell_id": f"b{i}", "x_km": -20.0, "y_km": 5.0 * i} for i in range(2)]
    return plan_devices(plume, bg, SITE, seed=1)


def _sim(cfg, expected_dose, seed=42):
    devices = _devices()
    return SensorSimulator(devices, {}, {}, expected_dose, {}, cfg["sensor_dq"], 0.05, seed, demo=None)


def test_same_seed_same_files(cfg, tmp_path):
    dose = {(f"p{i}", START.replace(minute=0) + dt.timedelta(hours=h)): 10.0 for i in range(4) for h in range(4)}
    _sim(cfg, dose).run(START, 60, str(tmp_path / "a"))
    _sim(cfg, dose).run(START, 60, str(tmp_path / "b"))
    files_a = sorted(p.name for p in (tmp_path / "a").iterdir())
    assert files_a == sorted(p.name for p in (tmp_path / "b").iterdir())
    for name in files_a:
        assert (tmp_path / "a" / name).read_text() == (tmp_path / "b" / name).read_text()


def test_dose_follows_model_band(cfg, tmp_path):
    """Bez wstrzykniętych odchyleń ~90% odczytów mieści się w paśmie P5–P95 modelu (plan P3)."""
    import json

    model = 10.0
    dose = {(f"p{i}", START.replace(minute=0) + dt.timedelta(hours=h)): model for i in range(4) for h in range(4)}
    _sim(cfg, dose, seed=7).run(START, 180, str(tmp_path))
    s, sb, z = cfg["sensor_dq"]["measurement_sigma"], cfg["sensor_dq"]["background_sigma"], 1.645
    lo = 0.05 * math.exp(-z * sb) + model * math.exp(-z * s)
    hi = 0.05 * math.exp(z * sb) + model * math.exp(z * s)
    vals = [
        r["dose_rate_usv_h"]
        for f in tmp_path.iterdir()
        for r in map(json.loads, f.read_text().splitlines())
        if r["device_id"].endswith(("00", "01", "02", "03")) and r["dose_rate_usv_h"] is not None
    ]
    inside = sum(lo <= v <= hi for v in vals) / len(vals)
    assert 0.8 < inside < 0.97


def test_cdc_to_scd2(spark):
    devices = _devices()
    events = cdc_events(devices, START)
    rows = [
        (e["device_id"], e["site_id"], e["cell_id"], e["jurisdiction_code"], e["firmware"], e["x_km"], e["y_km"],
         e["lat"], e["lon"], e["role"], e["op"], e["seq"], dt.datetime.fromisoformat(e["change_time"]).replace(tzinfo=dt.timezone.utc))
        for e in events
    ]
    scd = apply_scd2(spark.createDataFrame(rows, CDC_SCHEMA)).collect()

    upgraded = sorted((r for r in scd if r["device_id"] == devices[3]["device_id"]), key=lambda r: r["seq"])
    # UPDATE firmware → dwie wersje; powtórzone zdarzenie NIE tworzy trzeciej
    assert [r["firmware"] for r in upgraded] == ["v1", "v2"]
    assert upgraded[0]["valid_to"] == upgraded[1]["valid_from"]
    assert [r["is_current"] for r in upgraded] == [False, True]

    retired = [r for r in scd if r["device_id"] == devices[-1]["device_id"]]
    # DELETE zamyka ostatnią wersję i nie tworzy nowej
    assert len(retired) == 1 and not retired[0]["is_current"] and retired[0]["valid_to"] is not None

    assert len({r["device_id"] for r in scd}) == len(devices)


def test_first_three_devices_are_neighbors():
    d = _devices()
    for a in d[:3]:
        for b in d[:3]:
            assert math.dist((a["x_km"], a["y_km"]), (b["x_km"], b["y_km"])) < 5
