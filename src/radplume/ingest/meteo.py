"""Ingest meteo z Open-Meteo Historical API (ERA5) do strefy landing.

Wnioski z ręcznego testu API (plan 2.4.1, P21) są tu zakodowane jako zabezpieczenia:
1. jednostka wiatru wymuszona (``wind_speed_unit=ms``) i SPRAWDZANA w odpowiedzi —
   bez tego API zwraca km/h, a model po cichu liczyłby 3,6× za silny wiatr;
2. czas w UTC (``timezone=GMT``);
3. zapisujemy współrzędne żądane I zwrócone (API przyciąga punkt do swojej siatki).

Tryb ``synthetic`` generuje deterministyczne dane o tym samym kształcie JSON —
do testów, CI i pracy offline. Dane syntetyczne są oznaczone (``source``)
i nigdy nie udają prawdziwych.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
import random
import time
import zlib
from pathlib import Path

import requests

log = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Zmienne potrzebne modelowi (plan 2.4.1):
#  - wiatr 10 m: transport przy gruncie i klasa Pasquilla,
#  - wiatr 100 m: uwolnienie z wysokości,
#  - zachmurzenie + promieniowanie: klasa stabilności noc/dzień,
#  - opad: depozycja mokra.
HOURLY_VARS = (
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_speed_100m",
    "wind_direction_100m",
    "precipitation",
    "cloud_cover",
    "shortwave_radiation",
)
WIND_SPEED_VARS = ("wind_speed_10m", "wind_speed_100m")


class MeteoValidationError(ValueError):
    """Odpowiedź API nie spełnia kontraktu — plik NIE trafia do landing."""


def build_params(lat: float, lon: float, start: str, end: str) -> dict:
    return {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(HOURLY_VARS),
        "wind_speed_unit": "ms",   # P21: domyślnie km/h!
        "timezone": "GMT",         # wszystko w UTC
        "cell_selection": "land",  # preferuj komórkę lądową (lokalizacje nadmorskie)
    }


def validate_payload(payload: dict) -> None:
    """Kontrakt danych meteo. Lepiej odrzucić plik niż go „naprawić” po cichu.

    Konwersja km/h → m/s w locie wyglądałaby na wygodną, ale ukryłaby fakt,
    że zapytanie zostało źle zbudowane. Wolimy głośny błąd.
    """
    units = payload.get("hourly_units", {})
    for var in WIND_SPEED_VARS:
        if units.get(var) != "m/s":
            raise MeteoValidationError(
                f"{var}: jednostka {units.get(var)!r}, oczekiwano 'm/s' "
                "(czy zapytanie zawiera wind_speed_unit=ms?)"
            )
    if payload.get("utc_offset_seconds", 0) != 0:
        raise MeteoValidationError("Oczekiwano czasu UTC (utc_offset_seconds = 0)")
    hourly = payload.get("hourly", {})
    n = len(hourly.get("time", []))
    if n == 0:
        raise MeteoValidationError("Pusta odpowiedź (brak godzin)")
    for var in HOURLY_VARS:
        if len(hourly.get(var, [])) != n:
            raise MeteoValidationError(f"{var}: długość {len(hourly.get(var, []))} ≠ {n}")


def fetch_open_meteo(lat: float, lon: float, start: str, end: str, retries: int = 4) -> dict:
    """Pobranie z ponawianiem (exponential backoff) — API bywa chwilowo przeciążone."""
    params = build_params(lat, lon, start, end)
    delay = 2.0
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(ARCHIVE_URL, params=params, timeout=120)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            # 4xx inne niż 429 to błąd zapytania — ponawianie nic nie da.
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise
            if attempt == retries:
                raise
            log.warning("Open-Meteo: próba %d nieudana (%s), ponawiam za %.0fs", attempt, exc, delay)
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("nieosiągalne")


def synthetic_payload(lat: float, lon: float, start: str, end: str, seed: int) -> dict:
    """Deterministyczne, ale fizycznie sensowne dane meteo (tryb offline/testy).

    - wiatr: dobowy cykl + błądzenie losowe kierunku (fronty),
    - promieniowanie: sinusoida dnia zależna od szerokości geograficznej,
    - opad: rzadkie epizody.
    Seed zależy od współrzędnych (crc32, a nie hash() — hash() Pythona
    jest losowany przy każdym starcie interpretera i zniszczyłby determinizm).
    """
    rng = random.Random(seed ^ zlib.crc32(f"{lat:.4f},{lon:.4f}".encode()))
    t0 = dt.datetime.fromisoformat(start)
    t1 = dt.datetime.fromisoformat(end) + dt.timedelta(days=1)
    n_hours = int((t1 - t0).total_seconds() // 3600)

    times, ws10, wd10, ws100, wd100, precip, cloud, rad = ([] for _ in range(8))
    direction = rng.uniform(0, 360)
    base_speed = rng.uniform(3, 7)
    rain_left = 0
    for i in range(n_hours):
        t = t0 + dt.timedelta(hours=i)
        # godzina słoneczna ≈ UTC + długość/15 — wystarczy do cyklu dobowego
        solar_hour = (t.hour + lon / 15.0) % 24
        day_factor = max(0.0, math.sin(math.pi * (solar_hour - 6) / 12))
        direction = (direction + rng.gauss(0, 8)) % 360
        if rng.random() < 0.01:  # przejście frontu — skok kierunku
            direction = (direction + rng.uniform(60, 180)) % 360
        speed = max(0.3, base_speed * (0.7 + 0.6 * day_factor) + rng.gauss(0, 1.2))
        if rain_left == 0 and rng.random() < 0.02:
            rain_left = rng.randint(2, 10)
        p = round(rng.uniform(0.2, 4.0), 1) if rain_left > 0 else 0.0
        rain_left = max(0, rain_left - 1)
        cc = min(100, max(0, int(rng.gauss(85 if p > 0 else 50, 25))))
        season = 0.6 + 0.4 * math.cos(2 * math.pi * (t.timetuple().tm_yday - 172) / 365)
        sw = round(900 * day_factor * season * (1 - 0.7 * cc / 100), 1)

        times.append(t.strftime("%Y-%m-%dT%H:%M"))
        ws10.append(round(speed, 2))
        wd10.append(int(direction))
        ws100.append(round(speed * 1.4, 2))
        wd100.append(int((direction + 10) % 360))
        precip.append(p)
        cloud.append(cc)
        rad.append(sw)

    units = {v: "" for v in HOURLY_VARS}
    units.update({"time": "iso8601", "wind_speed_10m": "m/s", "wind_speed_100m": "m/s"})
    return {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "elevation": 10.0,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "hourly_units": units,
        "hourly": {
            "time": times,
            "wind_speed_10m": ws10,
            "wind_direction_10m": wd10,
            "wind_speed_100m": ws100,
            "wind_direction_100m": wd100,
            "precipitation": precip,
            "cloud_cover": cloud,
            "shortwave_radiation": rad,
        },
    }


def year_chunks(start: str, end: str) -> list[tuple[str, str]]:
    """Dzielimy zakres na lata: mniejsze odpowiedzi, a po awarii sieci
    ponownie pobieramy tylko brakujący rok (pliki działają jak cache)."""
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    chunks = []
    cur = s
    while cur <= e:
        chunk_end = min(dt.date(cur.year, 12, 31), e)
        chunks.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + dt.timedelta(days=1)
    return chunks


def meteo_requests(cfg: dict) -> list[tuple[str, str, str]]:
    """Lista (site_id, start, end) do pobrania: klimatologia + epizody walidacyjne."""
    out = []
    clim_start, clim_end = cfg["run"]["climatology"]["meteo_range"]
    for sid in cfg["run"]["sites"]:
        for s, e in year_chunks(clim_start, clim_end):
            out.append((sid, s, e))
        val = cfg["sites"][sid].get("validation")
        if val:
            out.append((sid, *val["meteo_range"]))
    return out


def ingest_meteo(cfg: dict, landing_dir: str, requests_list: list[tuple[str, str, str]] | None = None) -> list[str]:
    """Pobiera brakujące pliki do ``<landing>/meteo/<site_id>/<start>_<end>.json``.

    Idempotentne: istniejący plik jest pomijany (nie odpytujemy API ponownie
    i nie tworzymy duplikatów). Chcesz pobrać od nowa → usuń plik.
    ``requests_list`` pozwala pobrać konkretny zakres (np. dzień zdarzenia);
    domyślnie: klimatologia + epizody walidacyjne z konfiguracji. Nakładające się
    zakresy nie szkodzą — bronze scala godziny MERGE-em po (site_id, time_utc).
    """
    source = cfg["sources"]["meteo"]
    seed = cfg["run"]["seed"]
    written = []
    for site_id, start, end in requests_list if requests_list is not None else meteo_requests(cfg):
        site = cfg["sites"][site_id]
        out = Path(landing_dir) / "meteo" / site_id / f"{start}_{end}.json"
        if out.exists():
            continue
        if source == "open_meteo":
            payload = fetch_open_meteo(site["lat"], site["lon"], start, end)
        elif source == "synthetic":
            payload = synthetic_payload(site["lat"], site["lon"], start, end, seed)
        else:
            raise ValueError(f"Nieznane źródło meteo: {source!r}")
        validate_payload(payload)  # przed zapisem — zły plik nie trafi do landing

        record = {
            # Koperta z metadanymi pochodzenia: skąd, kiedy, o co pytaliśmy.
            "site_id": site_id,
            "source": source,
            "requested_lat": site["lat"],
            "requested_lon": site["lon"],
            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "payload": payload,
        }
        out.parent.mkdir(parents=True, exist_ok=True)
        # Zapis przez plik tymczasowy + rename: przerwany zapis nie zostawi
        # połówki JSON-a, którą bronze próbowałby potem wczytać.
        tmp = out.with_suffix(".tmp")
        tmp.write_text(json.dumps(record), encoding="utf-8")
        tmp.replace(out)
        written.append(str(out))
        log.info("meteo %s %s..%s (%s) → %s", site_id, start, end, source, out)
    return written
