"""Konfiguracja: środowiska różnią się tylko plikiem env, domena jest wspólna."""

import pytest

from radplume.config import deep_merge, load_config


def test_environments_share_physics_but_differ_in_storage():
    local, dev, prod = (load_config(e) for e in ("local", "dev", "prod"))
    assert local["physics"] == dev["physics"] == prod["physics"]
    assert local["thresholds"] == prod["thresholds"]
    assert local["storage"]["mode"] == "path"
    assert dev["storage"]["catalog"] == "radplume_dev"
    assert prod["storage"]["catalog"] == "radplume_prod"


def test_deep_merge_overrides_single_key_only():
    merged = deep_merge({"run": {"a": 1, "b": 2}}, {"run": {"b": 3}})
    assert merged == {"run": {"a": 1, "b": 3}}


def test_even_grid_is_rejected():
    # Parzysta siatka = źródło na styku komórek → błąd konfiguracji, nie cichy wynik.
    with pytest.raises(ValueError, match="nieparzysty"):
        load_config("local", {"run": {"grid_size": 40}})


def test_unknown_site_is_rejected():
    with pytest.raises(ValueError, match="nieznane lokalizacje"):
        load_config("local", {"run": {"sites": ["nie_istnieje"]}})


def test_yaml_numbers_are_numbers():
    # Regresja: PyYAML czyta „1.5e16” jako TEKST (wymaga „1.5e+16”).
    cfg = load_config("local")
    for site in cfg["sites"].values():
        for st in site["source_term"].values():
            assert isinstance(st["median_bq"], float)
    for nuc in cfg["physics"]["nuclides"].values():
        assert isinstance(nuc["half_life_s"], float)
