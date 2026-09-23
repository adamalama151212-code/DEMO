"""Rejestr urządzeń sieci monitoringu jako strumień zmian CDC (plan 4.4, P8).

Generujemy feed zmian z kolumnami ``op`` (INSERT/UPDATE/DELETE) i ``seq``
(numer sekwencyjny zmiany) — dokładnie taki, jaki przyszedłby z bazy
operacyjnej przez Debezium albo eksport CDC. Pokazuje:
- UPDATE firmware v1 → v2 (od tego momentu urządzenie wysyła nową kolumnę),
- DELETE = wycofanie urządzenia,
- powtórzone zdarzenie (ten sam ``seq``) — CDC musi być idempotentne.

Rozmieszczenie czujników jest deterministyczne: część w komórkach z najwyższą
oczekiwaną dawką (żeby „widziały” smugę), część w tle. Dane urządzeń
(dokładne współrzędne, device_id) są wrażliwe → maskowanie CLS w UC (plan 4.6).
"""

from __future__ import annotations

import datetime as dt
import json
import math
import random
from pathlib import Path

from radplume.silver.grid import KM_PER_DEG_LAT


def plan_devices(plume_cells: list[dict], background_cells: list[dict], site: dict, seed: int) -> list[dict]:
    """Wybiera pozycje urządzeń.

    Pierwsze TRZY urządzenia stoją w tej samej (najbardziej skażonej) komórce,
    w odległości < 5 km od siebie — to grupa „sąsiadów”, na której demo pokazuje
    odchylenie obszarowe ×5 (realny sygnał, nie usterka — plan P18).
    """
    rng = random.Random(seed)
    cells = [plume_cells[0]] * 3 + plume_cells[1:] + background_cells
    devices = []
    for i, c in enumerate(cells):
        # Losowe przesunięcie ±1 km wewnątrz komórki — urządzenia nie stoją w jednym punkcie.
        jx, jy = rng.uniform(-1, 1), rng.uniform(-1, 1)
        x_km, y_km = c["x_km"] + jx, c["y_km"] + jy
        devices.append(
            {
                "device_id": f"{site['site_id']}-dev-{i:02d}",
                "site_id": site["site_id"],
                "cell_id": c["cell_id"],
                "jurisdiction_code": site["jurisdiction_code"],
                "firmware": "v1",
                "x_km": round(x_km, 3),
                "y_km": round(y_km, 3),
                "lat": round(site["lat"] + y_km / KM_PER_DEG_LAT, 5),
                "lon": round(site["lon"] + x_km / (KM_PER_DEG_LAT * math.cos(math.radians(site["lat"]))), 5),
                "role": "plume" if i < len(cells) - len(background_cells) else "background",
            }
        )
    return devices


def cdc_events(devices: list[dict], sim_start: dt.datetime) -> list[dict]:
    """Feed zmian. ``seq`` rośnie monotonicznie — AUTO CDC porządkuje po nim (sequence_by)."""
    events, seq = [], 0
    installed = sim_start - dt.timedelta(days=30)
    for d in devices:
        seq += 1
        events.append({**d, "op": "INSERT", "seq": seq, "change_time": installed.isoformat()})

    # Aktualizacja firmware urządzenia #3 w trakcie symulacji → nowa kolumna (schema evolution).
    seq += 1
    fw = {**devices[3], "firmware": "v2", "op": "UPDATE", "seq": seq,
          "change_time": (sim_start + dt.timedelta(minutes=60)).isoformat()}
    events.append(fw)
    # To samo zdarzenie dostarczone drugi raz (typowe przy retry po stronie źródła).
    events.append(dict(fw))

    # Wycofanie ostatniego urządzenia tła.
    seq += 1
    events.append({**devices[-1], "op": "DELETE", "seq": seq,
                   "change_time": (sim_start + dt.timedelta(minutes=120)).isoformat()})
    return events


def write_cdc(events: list[dict], landing_dir: str) -> str:
    out = Path(landing_dir) / "device_cdc" / "changes_0001.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    # Stała nazwa pliku: ponowne uruchomienie nadpisuje ten sam plik tą samą
    # treścią (seed) → idempotentne, bez mnożenia zdarzeń.
    out.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")
    return str(out)
