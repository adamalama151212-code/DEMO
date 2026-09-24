"""Ładowanie konfiguracji: pliki domenowe + plik środowiska (local/dev/prod).

Dlaczego tak:
- Pliki domenowe (fizyka, lokalizacje, progi, DQ) są WSPÓLNE dla wszystkich
  środowisk — ten sam model fizyczny lokalnie i w PROD.
- Plik środowiska nadpisuje tylko to, co naprawdę się różni: gdzie leżą dane
  i jaka jest skala. To realizuje zasadę „DEV/PROD różnią się tylko zmiennymi”.
- YAML leży w paczce (``radplume/conf``), więc wheel wdrożony na Databricks
  niesie dokładnie tę samą konfigurację, którą testowaliśmy lokalnie.
"""

from __future__ import annotations

import copy
import os
from importlib import resources
from typing import Any

import yaml

# Kolejność ma znaczenie: późniejsze pliki nadpisują wcześniejsze.
DOMAIN_FILES = ("physics.yaml", "sites.yaml", "thresholds.yaml", "sensor_dq.yaml")
ENVIRONMENTS = ("local", "dev", "prod")


def deep_merge(base: dict, override: dict) -> dict:
    """Scala słowniki rekurencyjnie; wartości z ``override`` wygrywają.

    Zwykłe ``dict.update`` zastąpiłoby całą sekcję (np. całe ``run``),
    a my chcemy nadpisać pojedynczy klucz (np. tylko ``run.grid_size``).
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _read_packaged_yaml(name: str) -> dict:
    # importlib.resources zamiast ścieżki względnej do pliku: działa tak samo
    # przy instalacji edytowalnej (-e), z wheela i na klastrze Databricks.
    text = (resources.files("radplume") / "conf" / name).read_text(encoding="utf-8")
    return yaml.safe_load(text) or {}


def load_config(env: str | None = None, overrides: dict[str, Any] | None = None) -> dict:
    """Zwraca pełną konfigurację jako zwykły słownik.

    Środowisko wybieramy (w kolejności): argument ``env`` → zmienna
    ``RADPLUME_ENV`` → ``local``. ``overrides`` służy testom i flagom CLI
    (np. ``--offline``) — bez edytowania plików.
    """
    env = env or os.environ.get("RADPLUME_ENV", "local")
    if env not in ENVIRONMENTS:
        raise ValueError(f"Nieznane środowisko {env!r}; dozwolone: {ENVIRONMENTS}")

    cfg: dict = {}
    for name in DOMAIN_FILES:
        cfg = deep_merge(cfg, _read_packaged_yaml(name))
    cfg = deep_merge(cfg, _read_packaged_yaml(f"{env}.yaml"))
    if overrides:
        cfg = deep_merge(cfg, overrides)
    cfg["env"] = env

    _validate(cfg)
    return cfg


def _validate(cfg: dict) -> None:
    """Szybkie sprawdzenie spójności — lepiej paść od razu niż po godzinie liczenia."""
    run = cfg["run"]
    if run["grid_size"] % 2 == 0:
        # Przy parzystej siatce źródło leżałoby na styku 4 komórek, a nie w środku jednej.
        raise ValueError("run.grid_size musi być nieparzysty (źródło w środku komórki)")
    unknown = set(run["sites"]) - set(cfg["sites"])
    if unknown:
        raise ValueError(f"run.sites zawiera nieznane lokalizacje: {sorted(unknown)}")
    for tid, t in cfg["thresholds"].items():
        if t["nuclide"] not in cfg["physics"]["nuclides"]:
            raise ValueError(f"Próg {tid} odwołuje się do nieznanego nuklidu {t['nuclide']}")


def active_sites(cfg: dict) -> dict[str, dict]:
    """Lokalizacje liczone w bieżącym środowisku, z dopisanym ``site_id``."""
    return {sid: {**cfg["sites"][sid], "site_id": sid} for sid in cfg["run"]["sites"]}
