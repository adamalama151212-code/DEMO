"""Symulator sieci stacji meteo-radiacyjnych z realistycznie BRUDNYMI danymi (plan 3.5).

Czujnik co minutę mierzy moc dawki i wiatr. Dane psują się tak, jak w prawdziwych
sieciach (Safecast, EURDEP):
- utrata łączności → buforowanie → wysyłka zaległej paczki z jednym ``sent_at``,
- spike fizycznie niemożliwy (wiatr 80 m/s, dawka poza zakresem detektora),
- dryf detektora (powolny wzrost odczytu),
- zamrożony odczyt (zawieszony firmware),
- brak wartości przy działającym urządzeniu,
- duplikaty (ponowna wysyłka tej samej paczki).

Dawka NIE jest losowym tłem — pochodzi z wyniku modelu dla komórki czujnika
(plan P3), inaczej porównanie „pomiar vs model” nie miałoby sensu.

Symulator jest odseparowany od logiki czyszczenia (plan 3.1). To czysty Python
bez Sparka: pisze pliki JSON Lines do katalogu, z którego strumień czyta dane —
lokalnie zwykły katalog, na Databricks volume czytany przez Auto Loader.

Scenariusz demo (``DemoSchedule``) wymusza konkretne zdarzenia w konkretnych
minutach, żeby na prezentacji każdy przypadek pojawił się przewidywalnie.
"""

from __future__ import annotations

import datetime as dt
import json
import random
import time
from dataclasses import dataclass, field
from pathlib import Path

# Prawdopodobieństwa losowych zdarzeń na urządzenie na minutę (plan 3.5.1, P17).
OUTAGE_P_PER_MIN = 0.002        # ~1 awaria łączności na 8 h (rewizja 2 miała błędne 3%/min)
DUPLICATE_P = 0.003             # ponowna wysyłka tego samego odczytu
P_WIND_SPIKE = 0.002
P_DOSE_SPIKE = 0.002
P_DRIFT_START = 0.0005
P_FROZEN_START = 0.0005
P_NULL_DOSE = 0.001


@dataclass
class DemoSchedule:
    """Wymuszone zdarzenia: indeks urządzenia → (minuta startu, czas trwania / parametry)."""

    outage: dict[int, tuple[int, int]] = field(default_factory=lambda: {1: (30, 20)})     # paczka po 20 min
    wind_spike: dict[int, int] = field(default_factory=lambda: {4: 70})
    dose_spike: dict[int, int] = field(default_factory=lambda: {5: 75})
    drift: dict[int, tuple[int, int]] = field(default_factory=lambda: {6: (40, 120)})    # dryf 2 h
    frozen: dict[int, tuple[int, int]] = field(default_factory=lambda: {7: (120, 15)})
    null_dose: dict[int, int] = field(default_factory=lambda: {8: 90})
    duplicate: dict[int, int] = field(default_factory=lambda: {2: 20})
    # Realne odchylenie ×5 od modelu dla CAŁEJ grupy sąsiadów (0, 1, 2) — sygnał, nie usterka.
    area_deviation: tuple[tuple[int, ...], int, int, float] = ((0, 1, 2), 100, 60, 5.0)


class DeviceState:
    def __init__(self, idx: int, device: dict, firmware_changes: list[tuple[dt.datetime, str]], retired_at: dt.datetime | None):
        self.idx = idx
        self.device_id = device["device_id"]
        self.cell_id = device["cell_id"]
        self.firmware_changes = sorted(firmware_changes)
        self.retired_at = retired_at
        self.connected = True
        self.buffer: list[dict] = []
        self.outage_left = 0
        self.drift_left, self.drift_offset = 0, 0.0
        self.frozen_left, self.frozen_value = 0, None
        self.pending_duplicate: dict | None = None

    def firmware_at(self, t: dt.datetime) -> str:
        fw = "v1"
        for since, version in self.firmware_changes:
            if t >= since:
                fw = version
        return fw


class SensorSimulator:
    def __init__(self, devices, firmware_changes, retirements, expected_dose, expected_wind,
                 dq_cfg: dict, background_usv_h: float, seed: int, demo: DemoSchedule | None = None):
        self.rng = random.Random(seed)  # jeden RNG, stała kolejność urządzeń → pełny determinizm
        self.devices = [
            DeviceState(i, d, firmware_changes.get(d["device_id"], []), retirements.get(d["device_id"]))
            for i, d in enumerate(devices)
        ]
        self.expected_dose = expected_dose
        self.expected_wind = expected_wind
        self.bg = background_usv_h
        self.sigma = dq_cfg["measurement_sigma"]
        self.bg_sigma = dq_cfg["background_sigma"]
        self.dose_max = dq_cfg["dose_rate_usv_h"]["hard_max"]
        self.demo = demo

    # -------------------------------------------------------------- odczyt
    def _reading(self, d: DeviceState, t: dt.datetime, minute: int) -> dict:
        rng, demo = self.rng, self.demo
        hour = t.replace(minute=0, second=0, microsecond=0)
        median = self.expected_dose.get((d.cell_id, hour), 0.0)
        # P17: szum TAKŻE na tle — inaczej czujnik poza smugą wysyłałby stałą 0.05
        # i reguła „zamrożony odczyt” oflagowałaby całą sieć.
        dose = self.bg * rng.lognormvariate(0, self.bg_sigma) + median * rng.lognormvariate(0, self.sigma)
        # Wiatr z danych meteo tej godziny + lokalny szum (nie losowy gauss(8,3)).
        wind = max(self.expected_wind.get(hour, 5.0) + rng.gauss(0, 1.0), 0.0)

        if demo:
            ids, start, dur, factor = demo.area_deviation
            if d.idx in ids and start <= minute < start + dur:
                dose *= factor
            if demo.drift.get(d.idx, (None,))[0] == minute:
                d.drift_left = demo.drift[d.idx][1]
            if demo.frozen.get(d.idx, (None,))[0] == minute:
                d.frozen_left, d.frozen_value = demo.frozen[d.idx][1], round(dose, 4)

        roll = rng.random()
        if roll < P_WIND_SPIKE or (demo and demo.wind_spike.get(d.idx) == minute):
            wind = rng.uniform(60, 90)                         # (1) spike wiatru → drop
        elif roll < P_WIND_SPIKE + P_DOSE_SPIKE or (demo and demo.dose_spike.get(d.idx) == minute):
            dose = rng.uniform(5 * self.dose_max, 20 * self.dose_max)  # (1) poza zakresem detektora → drop
        elif roll < P_WIND_SPIKE + P_DOSE_SPIKE + P_DRIFT_START and d.drift_left == 0:
            d.drift_left = 120                                 # (2) dryf na 2 h
        elif roll < P_WIND_SPIKE + P_DOSE_SPIKE + P_DRIFT_START + P_FROZEN_START and d.frozen_left == 0:
            d.frozen_left, d.frozen_value = 15, round(dose, 4)  # (3) zawieszony firmware

        if d.drift_left > 0:
            # +1% oczekiwanej wartości na minutę → po 2 h ok. +120% (rozkalibrowany detektor)
            d.drift_offset += 0.01 * max(median, self.bg)
            dose += d.drift_offset
            d.drift_left -= 1
            if d.drift_left == 0:
                d.drift_offset = 0.0
        if d.frozen_left > 0:
            dose = d.frozen_value
            d.frozen_left -= 1

        battery = round(rng.uniform(20, 100), 1)
        dose_out = round(dose, 4)
        if rng.random() < P_NULL_DOSE or (demo and demo.null_dose.get(d.idx) == minute):
            dose_out = None                                    # (4) brak wartości przy zasilaniu

        reading = {
            "device_id": d.device_id,
            "event_time": t.isoformat(),
            "dose_rate_usv_h": dose_out,
            "wind_speed_ms": round(wind, 2),
            "battery": battery,
        }
        if d.firmware_at(t) == "v2":
            # Firmware v2 dodaje temperaturę detektora → nowa kolumna (schema evolution).
            reading["detector_temp_c"] = round(rng.gauss(18, 4), 1)
        return reading

    # -------------------------------------------------------------- łączność
    def _tick_device(self, d: DeviceState, t: dt.datetime, minute: int) -> list[dict]:
        if d.retired_at is not None and t >= d.retired_at:
            return []  # urządzenie wycofane (DELETE w rejestrze CDC) — milczy
        rng, demo = self.rng, self.demo
        forced = demo.outage.get(d.idx) if demo else None
        if d.connected and (rng.random() < OUTAGE_P_PER_MIN or (forced and forced[0] == minute)):
            d.connected = False
            d.outage_left = forced[1] if forced and forced[0] == minute else int(rng.uniform(5, 45))

        reading = self._reading(d, t, minute)
        if not d.connected:
            d.buffer.append(reading)
            d.outage_left -= 1
            if d.outage_left > 0:
                return []                    # nic nie wysyłamy podczas awarii
            d.connected = True
            flushed, d.buffer = d.buffer, []
        else:
            flushed = [reading]

        # sent_at ustawia URZĄDZENIE w chwili nadania — dla paczki z bufora wspólny
        # i późniejszy niż event_time. Stąd jawny lag = sent_at - event_time (plan P1).
        sent_at = t + dt.timedelta(seconds=rng.randint(0, 3))
        for r in flushed:
            r["sent_at"] = sent_at.isoformat()

        out = list(flushed)
        if d.pending_duplicate is not None:
            out.append(d.pending_duplicate)   # ta sama paczka wysłana drugi raz
            d.pending_duplicate = None
        if flushed and (rng.random() < DUPLICATE_P or (demo and demo.duplicate.get(d.idx) == minute)):
            d.pending_duplicate = dict(flushed[-1])
        return out

    def run(self, start: dt.datetime, minutes: int, out_dir: str, realtime_sleep_s: float = 0.0) -> int:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        n = 0
        for minute in range(minutes):
            t = start + dt.timedelta(minutes=minute)
            batch = []
            for d in self.devices:
                batch.extend(self._tick_device(d, t, minute))
            if batch:
                # Stała nazwa pliku z czasem symulacji: ponowne uruchomienie
                # nadpisuje te same pliki tą samą treścią (seed), a strumień
                # pamięta w checkpoincie, które pliki już przetworzył → brak duplikatów.
                f = out / f"batch_{t.strftime('%Y%m%dT%H%M%S')}.json"
                f.write_text("\n".join(json.dumps(r) for r in batch), encoding="utf-8")
                n += len(batch)
            if realtime_sleep_s:
                time.sleep(realtime_sleep_s)   # na demo: dane „płyną na żywo”
        return n
