"""Ingest miast z populacją: GeoNames (prawdziwe dane) albo lista zapasowa.

GeoNames ``cities5000.zip`` = wszystkie miejscowości świata z ≥5000 mieszkańców
(licencja CC BY 4.0). Plik jest mały (kilka MB), więc pobieramy całość i filtrujemy
w Sparku do krajów z aktywnych lokalizacji. Dlaczego cities5000, a nie ``JP.zip``:
jeden plik dla wszystkich krajów zamiast osobnego pobierania dla każdego.

Lista zapasowa (``conf/cities_fallback.csv``) to kilkadziesiąt miast wokół
lokalizacji, ze współrzędnymi i populacją PRZYBLIŻONĄ — tylko do trybu
offline i testów. Kolumna ``source`` mówi, skąd pochodzi wiersz.
"""

from __future__ import annotations

import logging
import shutil
import zipfile
from importlib import resources
from pathlib import Path

import requests

log = logging.getLogger(__name__)

GEONAMES_URL = "https://download.geonames.org/export/dump/cities5000.zip"


def ingest_cities(cfg: dict, landing_dir: str) -> str:
    """Zwraca ścieżkę do pliku z miastami w landing (TSV GeoNames albo CSV fallback)."""
    out_dir = Path(landing_dir) / "cities"
    out_dir.mkdir(parents=True, exist_ok=True)
    source = cfg["sources"]["cities"]

    if source == "geonames":
        txt = out_dir / "cities5000.txt"
        if not txt.exists():  # cache: pobieramy raz
            zip_path = out_dir / "cities5000.zip"
            log.info("Pobieram %s", GEONAMES_URL)
            with requests.get(GEONAMES_URL, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(zip_path, "wb") as fh:
                    shutil.copyfileobj(resp.raw, fh)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extract("cities5000.txt", out_dir)
        return str(txt)

    if source == "fallback":
        dst = out_dir / "cities_fallback.csv"
        src = resources.files("radplume") / "conf" / "cities_fallback.csv"
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        return str(dst)

    raise ValueError(f"Nieznane źródło miast: {source!r}")
