# Jak czytać ten kod

Przewodnik po repozytorium: od czego zacząć, za co odpowiada każdy plik i jak pliki
łączą się ze sobą. Architektura i uzasadnienia decyzji są w [`ARCHITECTURE.md`](ARCHITECTURE.md),
a przygotowanie danych w [`przygotowanie-danych.md`](przygotowanie-danych.md).

---

## 0. Najpierw jedno nieporozumienie: czujniki NIE są wejściem modelu

Łatwo pomyśleć, że na początku do bronze wczytujemy surowe dane z czujników razem z pogodą,
a potem Monte Carlo robi z nich scenariusze. **Tak nie jest.** Kolejność jest odwrotna,
a czujniki to osobna, późniejsza ścieżka:

```
KROK 1  Pogoda (prawdziwa, ERA5) ──► bronze.meteo ──► silver.meteo
                                                         │
KROK 2  Monte Carlo: tabele scenariuszy (MAŁE: 75 scenariuszy + harmonogramy + próbki Q) ──┤
                                                         ▼
KROK 3  Model transportu: scenariusze × godziny uwolnienia × kroki lotu obłoku × komórki × nuklidy
                                                         ◄── TU jest „eksplozja” wierszy
                                                         │
KROK 4  Agregacja → gold.city_exposure  ◄── odpowiedź „czy miasto Y zostanie skażone?”
─────────────────────────────────────────────────────────────── koniec ścieżki A (model)
KROK 5  Z JEDNEGO scenariusza modelu liczymy oczekiwaną dawkę (gold.expected_dose)
KROK 6  Symulator czujników GENERUJE odczyty na podstawie tej dawki + celowe usterki
KROK 7  Odczyty czujników ──► bronze.sensor_raw ──► czyszczenie ──► alerty
─────────────────────────────────────────────────────────────── ścieżka B (czujniki)
```

Najważniejsze fakty:

1. **Model (ścieżki A) w ogóle nie używa czujników.** Jego wejście to pogoda, położenie
   elektrowni, parametry fizyczne i lista miast.
2. **Czujniki powstają Z modelu, a nie odwrotnie.** Nie mamy prawdziwej sieci czujników
   (EURDEP ma zakaz wykorzystania danych). Symulator bierze więc dawkę przewidzianą przez
   model i dokłada szum oraz celowe usterki: awarie łączności, spike, dryf, zamrożenie.
3. **Po co w takim razie czujniki?** Ścieżka B pokazuje umiejętność czyszczenia brudnych
   danych strumieniowych, czego wymaga kurs (streaming, jakość danych, schema evolution).
   Symuluje sytuację w trakcie awarii: pomiary na żywo porównuje się z przewidywaniem modelu.
   **Nie służy do sprawdzania trafności modelu.** Do tego jest walidacja na prawdziwych
   pomiarach z Fukushimy (JAEA), patrz `validation/`.
4. **Monte Carlo nie tworzy nowych danych wejściowych.** Tworzy małe tabele „przepisów
   na scenariusz”:
   - `silver.scenarios`: kiedy awaria, jaka wersja fizyki, jakie zaburzenie wiatru,
   - `silver.release_schedule`: ile uwalnia się w każdej godzinie,
   - `silver.q_samples`: ile łącznie.

   Dużo wierszy pojawia się dopiero w kroku 3, gdy obłok z każdej godziny uwolnienia
   śledzimy co 15 minut i liczymy jego wpływ na pobliskie komórki. I nawet tam zapisujemy
   tylko wynik zagregowany.
5. **Dwa rodzaje Monte Carlo.**
   - **Klimatologia** („nie wiadomo, kiedy”) losuje **różne dni** z 10 lat prawdziwej pogody.
   - **Zdarzenie** (`radplume event`, „znam dzień i godziny zrzutów”) bierze **jeden dzień**
     i zaburza jego pogodę o małe wartości: kierunek ±15°, prędkość ±20%.

   Oba liczą się tym samym kodem i trafiają do tych samych tabel, rozróżnione kolumną `scenario_set`.

### Ile wierszy jest na każdym etapie (rzeczywiste liczby z przebiegu `--offline`, konfiguracja `local`, model `puff`)

| Etap | Tabela | Wierszy | Skąd ta liczba |
|---|---|---|---|
| pogoda | `bronze.meteo` | 175 584 | 2 lokalizacje × 10 lat × 8760 h (+ 10 dni marca 2011 dla Fukushimy) |
| siatka | `silver.grid` | 3 362 | 2 lokalizacje × 41 × 41 komórek |
| **scenariusze MC** | `silver.scenarios` | **75** | 2 × (12 epizodów × 3 warianty) + 3 walidacyjne. Mała tabela! (+30 na każde zdarzenie z 30 członkami) |
| ilości uwolnienia | `silver.source_terms` | 6 | zestaw × lokalizacja × nuklid (+2 na zdarzenie) |
| harmonogram | `silver.release_schedule` | 288 | 2 × 2 nuklidy × 24 h (klimatologia) + 2 × 96 h (walidacja); zdarzenie z 2 zrzutami: +4 |
| próbki ilości | `silver.q_samples` | 180 | (4 + 2) pary zestaw-lokalizacja-nuklid × 30 próbek (+60 na zdarzenie) |
| **model: odcinki trajektorii** (w pamięci) | — | **62 208** (Lubiatowo, klimatologia) | 36 scenariuszy × 24 obłoki (godziny uwolnienia) × 72 kroki po 15 min |
| **model: wiersze depozycji** (w pamięci, niezapisywane) | — | **~487 tys.** (Lubiatowo, klimatologia) | odcinek × komórki w prostokącie ±4σy wokół niego × 2 nuklidy (ok. 4 komórki na odcinek i nuklid) |
| model: zapisany wynik | `silver.dispersion_episode` | 21 442 (Lubiatowo, klimatologia) | suma po godzinach i odcinkach → 1 wiersz na (scenariusz, komórka, nuklid) |
| gold: × próbki Q (w pamięci) | — | ~640 tys. (Lubiatowo, klimatologia) | każdy wiersz × 30 próbek ilości uwolnienia |
| **gold: odpowiedzi** | `gold.risk_map` | ~20 tys. | komórki × progi skażenia, z prawdopodobieństwami (wszystkie zestawy, z jednym zdarzeniem) |
| **gold: odpowiedzi o miasta** | `gold.city_exposure` | 160 (+56 na zdarzenie) | miasta w promieniu 100 km × 4 progi × zestawy scenariuszy |
| — ścieżka B — | | | |
| oczekiwana dawka (1 scenariusz demo) | `gold.expected_dose` | ok. 200 tys. | 1681 komórek × ok. 120 h |
| odczyty czujników | `bronze.sensor_raw` | ok. 1 750 | 10 czujników × 180 minut (minus awarie i wycofane urządzenie) |
| po czyszczeniu | `silver.sensor_clean` | ok. 1 710 | minus spóźnione (> 15 min) i te w kwarantannie |

Kształt „lejka” jest typowy dla symulacji: **mało danych wejściowych → ogromny iloczyn
w trakcie obliczeń → mały, zagregowany wynik.** W PROD to rząd 10⁸ wierszy depozycji w jednym
przebiegu (plan P20, `ARCHITECTURE.md` sekcja 12). Dlatego dla każdego odcinka bierzemy tylko
pobliskie komórki i nie zapisujemy wyniku godzinowego.

---

## 1. Kolejność czytania (ścieżka nauki)

Nie czytaj plików alfabetycznie. Czytaj w kolejności przepływu danych:

| # | Plik | Co z niego wyniesiesz |
|---|---|---|
| 1 | `src/radplume/conf/local.yaml`, `sites.yaml` | jakie lokalizacje, jaka skala, skąd dane |
| 2 | `src/radplume/cli.py` | lista wszystkich kroków i jak się je uruchamia |
| 3 | `src/radplume/pipelines/batch.py` | **spis treści ścieżki A**: każda funkcja `step_*` czyta tabele, woła transformację, zapisuje |
| 4 | `src/radplume/ingest/meteo.py` → `bronze/meteo.py` → `silver/meteo.py` | droga pogody od API do klasy stabilności |
| 5 | `src/radplume/silver/scenarios.py` | jak powstają scenariusze MC, ilości i harmonogram uwolnienia |
| 6 | `src/radplume/silver/dispersion.py` | wzory fizyczne (Briggs, gauss, depozycja), prosta smuga i wybór modelu (`unit_deposition`) |
| 6a | `src/radplume/silver/puff.py` | **serce projektu**: obłoki przesuwane zmiennym wiatrem (domyślny model) |
| 7 | `src/radplume/gold/aggregates.py` | jak z tysięcy scenariuszy powstaje „15% szans” |
| 8 | `src/radplume/app/city_query.py` | jak wygląda odpowiedź dla użytkownika |
| 8a | `src/radplume/silver/events.py` → `pipelines/event.py` | tryb zdarzenia: `radplume event` i zespół zaburzonej pogody |
| 9 | `src/radplume/pipelines/stream.py` | **spis treści ścieżek B i C** (czujniki, rejestr urządzeń) |
| 10 | `simulators/` → `silver/sensor_clean.py` → `silver/sensor_quality.py` → `gold/live_alerts.py` | brudne dane i ich czyszczenie |
| 11 | `src/radplume/core/` | infrastruktura; wystarczy wiedzieć, co robi (sekcja 2) |
| 12 | `tests/` | każdy test to mały, wykonywalny przykład działania funkcji |

**Wskazówka:** pliki w `pipelines/` to mapa. Gdy nie wiesz, skąd bierze się tabela,
szukaj w nich jej nazwy (np. `"city_exposure"`). Zobaczysz, który krok ją zapisuje
i z czego ją liczy.

---

## 2. Za co odpowiada każdy plik

### Punkt wejścia i orkiestracja

| Plik | Odpowiedzialność | Woła | Wołany przez |
|---|---|---|---|
| `cli.py` | komenda `radplume <krok>`: parsuje argumenty, buduje `Context`, uruchamia kroki, `event` albo `ask` | `pipelines/*`, `app/city_query.py`, `silver/events.py` (parsowanie wycieków), `core/*` | terminal, Docker, Job na Databricks |
| `pipelines/context.py` | `Context` = konfiguracja + sesja Spark + storage, przekazywany do każdego kroku | `core/storage.py` | `cli.py`, kroki, testy |
| `pipelines/batch.py` | kroki ścieżki A (`ingest-meteo` … `expected-dose`) i słownik `BATCH_STEPS` | `ingest/`, `bronze/`, `silver/`, `gold/`, `validation/` | `cli.py`, test integracyjny |
| `pipelines/event.py` | `run_event`: pogoda dla dnia zdarzenia (dociągana, jeśli brak) → scenariusze zdarzenia → transport → gold; `event_summary` | `silver/events.py`, funkcje z `pipelines/batch.py` (`compute_dispersion`, `write_scenario_tables`, `refresh_*_meteo`, `step_gold`) | `cli.py` |
| `pipelines/stream.py` | kroki ścieżek B i C (`device-registry` … `alerts`, `sensor-reset`) i `STREAM_ORDER` | `simulators/`, `silver/sensor_*`, `silver/devices_cdc.py`, `gold/live_alerts.py` | `cli.py` |

### `core/`: infrastruktura (jedyne miejsce zależne od środowiska)

| Plik | Odpowiedzialność |
|---|---|
| `core/config.py` | wczytuje YAML (domena + środowisko), scala je, waliduje (`load_config`, `active_sites`) |
| `core/session.py` | sesja Spark: lokalna z Delta Lake albo sesja klastra Databricks; wymusza UTC |
| `core/storage.py` | `Storage`: odczyt i zapis tabel (`read`, `merge`, `overwrite`, `append_idempotent`), ścieżki landing i checkpointów. Lokalnie `data/delta/...`, w chmurze Unity Catalog |
| `core/dq_metrics.py` | `log_metrics`: dopisuje metryki jakości do `ops.dq_metrics` |

### `conf/`: konfiguracja (YAML jedzie w paczce)

| Plik | Zawartość |
|---|---|
| `local.yaml` / `dev.yaml` / `prod.yaml` | gdzie są dane i jaka skala (siatka, liczba epizodów i próbek) |
| `sites.yaml` | elektrownie: współrzędne, jurysdykcja, ilość uwolnienia, epizod walidacyjny |
| `physics.yaml` | nuklidy (rozpad, depozycja, dawka), profil wiatru, rozkłady wariantów |
| `thresholds.yaml` | co znaczy „skażone” (37 kBq/m², 20 mSv/rok…) i promień miast |
| `sensor_dq.yaml` | progi jakości danych czujników + scenariusz demo |
| `cities_fallback.csv` | zapasowa lista miast (tryb offline) |

### `ingest/`: pobieranie PRAWDZIWYCH danych do landing (bez Sparka)

| Plik | Odpowiedzialność | Zapisuje |
|---|---|---|
| `ingest/meteo.py` | zapytanie do Open-Meteo, kontrakt (m/s, UTC), ponawianie; generator syntetyczny dla `--offline` | `data/landing/meteo/<site>/<okres>.json` |
| `ingest/cities.py` | pobranie GeoNames `cities5000` albo kopia listy zapasowej | `data/landing/cities/` |

### `simulators/`: dane SYMULOWANE (bez Sparka)

| Plik | Odpowiedzialność | Zapisuje |
|---|---|---|
| `simulators/device_registry.py` | rozmieszczenie 10 czujników (6 w smudze, 4 w tle) i feed zmian CDC (INSERT, UPDATE firmware, DELETE, duplikat) | `data/landing/device_cdc/` |
| `simulators/sensor_sim.py` | `SensorSimulator`: co minutę odczyt dawki i wiatru z modelu + szum + usterki; `DemoSchedule` wymusza usterki w konkretnych minutach | `data/landing/sensor_stream/batch_*.json` |

### `bronze/`: landing → surowe tabele Delta

| Plik | Odpowiedzialność | Tabela |
|---|---|---|
| `bronze/meteo.py` | JSON z tablicami godzinowymi → 1 wiersz na godzinę; jawny schemat; `source_file` | `bronze.meteo` |
| `bronze/cities.py` | TSV GeoNames albo CSV zapasowy → wspólny schemat | `bronze.cities` |
| `bronze/source_term.py` | przebieg uwolnienia z pliku (Katata/Terada), jeśli jest w `landing/source_term/` | `bronze.source_term` |

### `silver/`: czyszczenie i obliczenia

| Plik | Odpowiedzialność | Tabela |
|---|---|---|
| `silver/meteo.py` | kontrola jakości, **kierunek smugi = wiatr + 180°**, składowe u/v, **klasa Pasquilla** | `silver.meteo` |
| `silver/grid.py` | siatka ±100 km wokół elektrowni; przypisanie miast i pomiarów do komórek; odległości i azymuty | `silver.grid`, `silver.city_cells` |
| `silver/scenarios.py` | **scenariusze MC** dla klimatologii i walidacji: momenty awarii × warianty fizyczne; ilości (`source_terms`), **harmonogram uwolnienia** (także z pliku Katata), próbki Q; wspólna klasa `ScenarioTables` | `silver.scenarios`, `source_terms`, `release_schedule`, `q_samples` |
| `silver/events.py` | **tryb zdarzenia**: parsowanie `--release` (strefy czasowe), zespół członków z zaburzonym wiatrem, harmonogram z godzin zrzutów | te same cztery tabele, zestaw `event_…` |
| `silver/dispersion.py` | wzory fizyczne jako funkcje `Column` (Briggs, gauss, kolumna, wymywanie), godziny uwolnienia z pogodą i zaburzeniem, **prosta smuga** (`straight`), wybór modelu (`unit_deposition`), agregacja do epizodów, oczekiwana dawka dla czujników | `silver.dispersion_episode` (+ `gold.expected_dose`) |
| `silver/puff.py` | **model obłoków** (domyślny): trajektorie co 15 min, σ z przebytej drogi, depozycja z odcinka × ΔΦ, prostokąt komórek wokół odcinka | (ten sam wynik co `dispersion.py`) |
| `silver/devices_cdc.py` | feed CDC → historia wersji urządzeń (SCD typ 2) | `silver.devices` |
| `silver/sensor_clean.py` | reguły bezstanowe czujników: spóźnienie (lag), deduplikacja, reguły twarde → kwarantanna, dołączenie wersji urządzenia | `silver.sensor_clean`, `ops.sensor_*` |
| `silver/sensor_quality.py` | reguły wymagające historii: reszta vs model, dryf, zamrożenie, skok wiatru, warm-up, porównanie z sąsiadami | `silver.sensor_quality` |

### `gold/`: odpowiedzi

| Plik | Odpowiedzialność | Tabela |
|---|---|---|
| `gold/aggregates.py` | nałożenie próbek Q, prawdopodobieństwa przekroczenia progów, percentyle, najgroźniejszy kierunek wiatru, ranking lokalizacji | `gold.risk_map`, `gold.city_exposure`, `gold.site_ranking` |
| `gold/live_alerts.py` | pasmo P5–P95 modelu, alerty „sygnał” vs „usterka”, podsumowanie jakości danych | `gold.live_alerts`, `gold.sensor_dq_summary` |

### `validation/` i `app/`

| Plik | Odpowiedzialność |
|---|---|
| `validation/metrics.py` | pomiary JAEA z CSV → porównanie z modelem w tej samej komórce → FAC2, FAC5, pokrycie P5–P95 (`gold.validation`) |
| `app/city_query.py` | `radplume ask`: rozpoznanie miasta, zapytanie do `gold.city_exposure`, odpowiedź po polsku, odmowa dla niemodelowanych danych |
| `app/guardrails.py` | `validate_select`: tylko SELECT, tylko tabele gold, wymuszony LIMIT (parser sqlglot) |

### Pliki poza `src/`

| Plik | Odpowiedzialność |
|---|---|
| `pyproject.toml` | zależności, komenda `radplume`, konfiguracja ruff i pytest |
| `Dockerfile`, `docker-compose.yml` | środowisko lokalne (Python + Java 17 + Spark + Delta) |
| `databricks.yml`, `resources/job_radplume.yml` | szkielet wdrożenia na Databricks: każdy krok CLI = task Joba |
| `.github/workflows/ci.yml` | CI: lint + testy na każdym PR |
| `tests/unit/`, `tests/integration/` | testy; opis w sekcji 5 |

---

## 3. Połączenia: który krok, który plik, która tabela

### Ścieżka A: model

```mermaid
flowchart TD
    subgraph K1[krok ingest-meteo / ingest-cities]
        IM[ingest/meteo.py] --> LM[(landing/meteo)]
        IC[ingest/cities.py] --> LC[(landing/cities)]
    end
    subgraph K2[krok bronze]
        LM --> BMpy[bronze/meteo.py] --> BM[(bronze.meteo)]
        LC --> BCpy[bronze/cities.py] --> BC[(bronze.cities)]
    end
    subgraph K3[krok silver]
        BM --> SMpy[silver/meteo.py] --> SM[(silver.meteo)]
        GRpy[silver/grid.py] --> GR[(silver.grid)]
        BC --> GRpy2[silver/grid.py<br/>assign_points_to_cells] --> CC[(silver.city_cells)]
        BST[(bronze.source_term<br/>opcjonalnie)] --> SCpy
        SCpy[silver/scenarios.py] --> SC[(silver.scenarios)]
        SCpy --> RS[(silver.release_schedule)]
        SCpy --> STT[(silver.source_terms)]
        SCpy --> QS[(silver.q_samples)]
    end
    subgraph K4[krok dispersion]
        SM --> DI[silver/dispersion.py unit_deposition<br/>→ silver/puff.py puff_unit_deposition<br/>→ aggregate_episodes]
        GR --> DI
        SC --> DI
        RS --> DI
        STT --> DI
        DI --> DE[(silver.dispersion_episode)]
    end
    subgraph K5[krok gold]
        DE --> AG[gold/aggregates.py]
        QS --> AG
        CC --> AG
        AG --> RM[(gold.risk_map)]
        AG --> CE[(gold.city_exposure)]
        AG --> SR[(gold.site_ranking)]
    end
    CE --> ASK[app/city_query.py<br/>radplume ask]
    RM --> VA[validation/metrics.py] --> GV[(gold.validation)]
```

### Tryb zdarzenia (`radplume event`)

```mermaid
flowchart TD
    CLI[cli.py event<br/>--site --date --release] --> PR[silver/events.py<br/>parse_release, EventSpec]
    PR --> RE[pipelines/event.py run_event]
    RE -->|brak pogody dla dnia| IM[ingest/meteo.py] --> BMF[bronze.meteo → silver.meteo]
    RE --> BT[silver/events.py build_event_tables<br/>zespół członków]
    BT -->|replaceWhere scenario_set = event_…| SCT[(silver.scenarios, source_terms,<br/>release_schedule, q_samples)]
    SCT --> CD[pipelines/batch.py compute_dispersion<br/>tylko ten zestaw]
    CD --> DE[(silver.dispersion_episode)]
    DE --> GOLD[pipelines/batch.py step_gold] --> CE[(gold.city_exposure)]
    CE --> SUM[event_summary<br/>+ radplume ask --scenario-set event_…]
```

Tryb zdarzenia nie ma własnych tabel ani własnego modelu: dopisuje nowy `scenario_set`
do tych samych tabel i używa tych samych funkcji co `run-batch`.

### Ścieżki B i C: czujniki i rejestr urządzeń

```mermaid
flowchart TD
    SM[(silver.meteo)] --> ED
    SC[(silver.scenarios)] --> ED[krok expected-dose<br/>silver/dispersion.py<br/>expected_dose_series]
    ED --> GED[(gold.expected_dose)]

    GED --> DR[krok device-registry<br/>simulators/device_registry.py]
    DR --> LCDC[(landing/device_cdc)] --> SCD[silver/devices_cdc.py] --> DEV[(silver.devices SCD2)]

    GED --> SIM[krok sensor-sim<br/>simulators/sensor_sim.py]
    DEV --> SIM
    SM --> SIM
    SIM --> LS[(landing/sensor_stream)]

    LS --> ST[krok sensor-stream<br/>silver/sensor_clean.py]
    DEV --> ST
    ST --> RAW[(bronze.sensor_raw)]
    ST --> LATE[(ops.sensor_late_rejected)]
    ST --> CL[(silver.sensor_clean)]
    ST --> QU[(ops.sensor_quarantine)]

    CL --> SQ[krok sensor-quality<br/>silver/sensor_quality.py]
    GED --> SQ
    SQ --> SQT[(silver.sensor_quality)]
    SQT --> AL[krok alerts<br/>gold/live_alerts.py]
    AL --> GLA[(gold.live_alerts)]
    AL --> DQS[(gold.sensor_dq_summary)]
```

Kluczowa strzałka: `gold.expected_dose` → symulator. To jedyne miejsce, gdzie model
wpływa na czujniki. W drugą stronę (czujniki → model) nie ma żadnej strzałki.

### Tabela: kto zapisuje, kto czyta

| Tabela | Zapisuje (krok → plik) | Czytają |
|---|---|---|
| `bronze.meteo` | `bronze` → `bronze/meteo.py` | `silver` |
| `bronze.cities` | `bronze` → `bronze/cities.py` | `silver` |
| `silver.meteo` | `silver` → `silver/meteo.py` | `dispersion`, `expected-dose`, `sensor-sim`, `ask` (sprawdza, czy dane syntetyczne) |
| `silver.grid` | `silver` → `silver/grid.py` | `dispersion`, `gold`, `validation`, `expected-dose`, `device-registry` |
| `silver.city_cells` | `silver` → `silver/grid.py` | `gold` |
| `bronze.source_term` | `bronze` → `bronze/source_term.py` (gdy jest plik) | `silver` |
| `silver.scenarios`, `source_terms`, `release_schedule`, `q_samples` | `silver` → `silver/scenarios.py` (bez zdarzeń); `event` → `silver/events.py` (tylko swoje zdarzenie) | `dispersion`, `event`, `gold`, `expected-dose`, `device-registry`, `sensor-sim`, `list-events` |
| `silver.dispersion_episode` | `dispersion` / `event` → `silver/dispersion.py` + `silver/puff.py` | `gold` |
| `gold.risk_map` | `gold` → `gold/aggregates.py` | `validation`, dashboard |
| `gold.city_exposure` | `gold` → `gold/aggregates.py` | `site_ranking`, `ask`, `list-cities` |
| `gold.expected_dose` | `expected-dose` → `silver/dispersion.py` | `device-registry`, `sensor-sim`, `sensor-quality` |
| `silver.devices` | `device-registry` → `silver/devices_cdc.py` | `sensor-sim`, `sensor-stream` |
| `bronze.sensor_raw` | `sensor-stream` | `sensor-stream` (dalsze zapytania), `alerts` |
| `silver.sensor_clean`, `ops.sensor_*` | `sensor-stream` → `silver/sensor_clean.py` | `sensor-quality`, `alerts` |
| `silver.sensor_quality` | `sensor-quality` → `silver/sensor_quality.py` | `alerts` |
| `gold.live_alerts`, `gold.sensor_dq_summary` | `alerts` → `gold/live_alerts.py` | dashboard |
| `ops.dq_metrics` | `silver`, `dispersion`, `sensor-stream` → `core/dq_metrics.py` | dashboard |

---

## 4. Śledzenie jednej liczby od końca do początku

Weźmy odpowiedź „teren skażony w Lęborku: 24,5% scenariuszy” (przebieg offline z tabeli w sekcji 0).

1. **`app/city_query.py`** czyta wiersz z `gold.city_exposure` dla `lubiatowo_kopalino` + `Lębork`
   + próg `cs137_contaminated`, zestaw `climatology`. Kolumna `p_exceed = 0.245`.
2. **`gold/aggregates.py` → `build_city_exposure`**: dla komórki siatki, w której leży Lębork,
   wziął wszystkie scenariusze (36 epizodo-wariantów × 30 próbek Q = 1080), policzył depozycję
   `unit_dep_per_bq × Q` i zliczył, w ilu przekroczyła 37 kBq/m². 265 / 1080 = 24,5%.
   Scenariusze, w których smuga nie doleciała, nie mają wierszy, ale liczą się do mianownika.
3. **`silver.dispersion_episode`**: dla każdego scenariusza depozycja na 1 Bq uwolnienia
   w tej komórce, zsumowana po 24 obłokach (godzinach uwolnienia) i ich odcinkach lotu.
4. **`silver/puff.py` → `puff_unit_deposition`**:
   - obłok z każdej godziny uwolnienia (masa = ułamek z `silver.release_schedule`) przesuwał się
     co 15 min wiatrem z `silver.meteo`,
   - dla każdego odcinka lotu, który przeszedł obok Lęborka, policzono σy/σz z przebytej drogi,
     wzór gaussowski × ΔΦ oraz depozycję suchą i mokrą.
5. **`silver.scenarios`**: epizod = losowy moment awarii z lat 2015–2024 (`silver/scenarios.py`).
6. **`silver.meteo`** ← **`bronze.meteo`** ← plik JSON z Open-Meteo w `data/landing/meteo/lubiatowo_kopalino/`.

To samo możesz prześledzić sam w terminalu:

```bash
radplume ask --site lubiatowo_kopalino --city Lębork    # odpowiedź + użyty SQL
radplume show gold city_exposure -n 20                   # wiersz odpowiedzi
radplume show silver scenarios -n 40                     # „przepisy” scenariuszy
radplume show silver dispersion_episode -n 20            # wynik modelu przed nałożeniem Q
radplume show silver meteo -n 24                         # pogoda godzina po godzinie
```

Powyższe liczby (24,5%, 1080) pochodzą z przebiegu offline (pogoda syntetyczna), więc na prawdziwej pogodzie będą inne.

---

## 5. Testy jako przykłady użycia

Każda funkcja transformacji ma test pokazujący, jak jej użyć na kilku wierszach. To najszybszy
sposób, żeby zrozumieć fragment kodu: przeczytaj test, zmień w nim liczbę i uruchom go
(`pytest tests/unit/test_physics.py -k mass_balance -v`).

| Test | Pokazuje działanie |
|---|---|
| `tests/unit/test_physics.py` | `silver/dispersion.py`: wzory, bilans masy, smuga tylko z wiatrem |
| `tests/unit/test_puff.py` | `silver/puff.py`: Φ, zgodność ze smugą przy stałym wietrze, chmura skręcająca z wiatrem |
| `tests/unit/test_scenarios.py` | `silver/scenarios.py`: harmonogram (przedziały → godziny, dwa zrzuty), plik walidacyjny, próbki Q |
| `tests/unit/test_events.py` | `silver/events.py`: zapis wycieków, strefy czasowe, zespół członków |
| `tests/unit/test_meteo.py` | `ingest/meteo.py` i `silver/meteo.py`: odrzucenie km/h, kierunek wiatru, klasy Pasquilla |
| `tests/unit/test_sensor_dq.py` | `silver/sensor_clean.py` i `sensor_quality.py`: każdy typ usterki |
| `tests/unit/test_sensor_sim_and_cdc.py` | `simulators/` i `silver/devices_cdc.py`: determinizm symulatora, SCD2 |
| `tests/unit/test_guardrails_and_app.py` | `app/`: które zapytania SQL są blokowane |
| `tests/unit/test_config.py` | `core/config.py`: środowiska, walidacja |
| `tests/integration/test_pipeline.py` | cała ścieżka A: idempotencja, każde miasto ma odpowiedź, spójność progów |
| `tests/integration/test_event.py` | `radplume event` end-to-end: dociągnięcie pogody, gold, klimatologia nietknięta |

---

## 6. Słowniczek

| Pojęcie | Znaczenie w tym projekcie |
|---|---|
| **landing** | folder z plikami dokładnie takimi, jak przyszły ze źródła |
| **bronze / silver / gold** | surowe tabele → oczyszczone i policzone → odpowiedzi na pytania |
| **ops** | tabele operacyjne: odrzucone rekordy, metryki jakości |
| **epizod** | jeden hipotetyczny moment awarii (start + 24 h uwolnienia) z prawdziwą pogodą z tamtego czasu |
| **wariant fizyczny** | jedna wersja niepewnych parametrów modelu (stabilność ±1, depozycja, wysokość) |
| **członek zespołu** | wariant w trybie zdarzenia: parametry fizyczne + zaburzenie wiatru (kierunek, prędkość); członek 0 = niezaburzony |
| **próbka Q** | jedna możliwa ilość uwolnionego cezu/jodu |
| **harmonogram uwolnienia** | jaki ułamek całkowitej ilości wychodzi w każdej godzinie (`release_fraction`, suma = 1) |
| **scenariusz** | epizod × wariant × próbka Q |
| **`scenario_set`** | `climatology` (losowe momenty z 10 lat, czyli „co jeśli”), `validation_2011` (prawdziwe daty Fukushimy) albo `event_…` (zdarzenie z `radplume event`) |
| **obłok (puff)** | masa uwolniona w jednej godzinie, śledzona co 15 min wzdłuż trajektorii wyznaczanej przez zmienny wiatr |
| **ΔΦ** | część obłoku, która „przeszła” obok komórki na danym odcinku trajektorii |
| **unit deposition** | depozycja na 1 Bq całkowitego uwolnienia (rozłożonego wg harmonogramu); prawdziwa = unit × Q (model jest liniowy) |
| **`p_exceed`** | odsetek scenariuszy, w których przekroczono próg |
| **klasa Pasquilla** | stabilność atmosfery A (silne mieszanie) … F (bardzo stabilnie, wąska smuga) |
| **SCD typ 2** | tabela z historią wersji rekordu (`valid_from`, `valid_to`) |
| **lag** | `sent_at − event_time`: o ile spóźniony jest odczyt czujnika |
| **reszta (residual)** | `ln(odczyt / przewidywanie modelu)`: ≈ 0 dla sprawnego czujnika |
