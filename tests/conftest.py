"""Wspólne fikstury testów.

Jedna sesja Spark na cały przebieg testów (``scope="session"``): start JVM
trwa kilka sekund, więc tworzenie jej w każdym teście wydłużyłoby CI kilkukrotnie.
Każdy test, który zapisuje tabele, dostaje własny katalog danych (``tmp_path``),
więc testy nie wpływają na siebie nawzajem.
"""

from __future__ import annotations

import pytest

from radplume.config import load_config

# Mała skala testowa: sekundy zamiast minut, ale ta sama logika co w PROD.
TEST_OVERRIDES = {
    "spark": {"master": "local[2]", "shuffle_partitions": 2, "driver_memory": "2g"},
    "sources": {"meteo": "synthetic", "cities": "fallback"},
    "run": {
        "sites": ["lubiatowo_kopalino"],
        "grid_size": 11,
        "cell_km": 10.0,
        "climatology": {"meteo_range": ["2020-01-01", "2020-02-29"], "n_episodes": 3, "episode_hours": 6},
        "n_param_scenarios": 2,
        "n_source_samples": 5,
    },
}


@pytest.fixture(scope="session")
def spark():
    from radplume.session import get_spark

    cfg = load_config("local", TEST_OVERRIDES)
    s = get_spark(cfg)
    yield s
    s.stop()


@pytest.fixture
def cfg():
    return load_config("local", TEST_OVERRIDES)


@pytest.fixture
def ctx(spark, cfg, tmp_path, monkeypatch):
    """Kontekst potoku z izolowanym katalogiem danych."""
    from radplume.pipeline_batch import Context
    from radplume.storage import Storage

    monkeypatch.setenv("RADPLUME_DATA_DIR", str(tmp_path / "data"))
    return Context(cfg, spark, Storage(spark, cfg))
