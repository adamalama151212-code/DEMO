# radplume — czy uwolnienie w elektrowni X skazi miasto Y?

Platforma danych do modelowania dyspersji radionuklidów w atmosferze.
Projekt finalny kursu Databricks. Pełny plan i uzasadnienie decyzji są w
[`plan_finalny_radplume.md`](plan_finalny_radplume.md), wymagania kursu w
[`final-project-spec.md`](final-project-spec.md).

> **Pytanie główne:** „Gdyby doszło do uwolnienia radionuklidów do atmosfery
> w elektrowni X, czy miasto Y zostanie skażone? Jak bardzo, jak szybko i przy
> jakiej pogodzie?”
>
> Odpowiedź nigdy nie brzmi „tak/nie”. Ma postać (przykład na danych syntetycznych):
> „w 15,6% scenariuszy depozycja Cs-137 w Lęborku przekracza 37 kBq/m²; mediana
> czasu dotarcia smugi 12,3 h; najgroźniejszy wiatr z sektora N”.

Ta wersja to **PoC lokalny** (plan, Część III). Działa na laptopie, ale ma
strukturę gotową do przeniesienia na Databricks bez zmian w logice
(patrz [Przejście do chmury](#przejście-do-chmury)).

---

## Spis treści

1. [Co jest w środku](#co-jest-w-środku)
2. [Uruchomienie: Docker (zalecane, także Windows)](#uruchomienie-docker-zalecane-także-windows)
3. [Uruchomienie: natywnie (Linux / macOS / WSL2)](#uruchomienie-natywnie-linux--macos--wsl2)
4. [Pierwszy przebieg krok po kroku](#pierwszy-przebieg-krok-po-kroku)
5. [Zadawanie pytań](#zadawanie-pytań)
6. [Kroki potoku i tabele](#kroki-potoku-i-tabele)
7. [Demo czujników (brudne dane)](#demo-czujników-brudne-dane)
8. [Konfiguracja](#konfiguracja)
9. [Walidacja na prawdziwych pomiarach](#walidacja-na-prawdziwych-pomiarach)
10. [Testy](#testy)
11. [Przejście do chmury](#przejście-do-chmury)
12. [Ograniczenia](#ograniczenia)
13. [Rozwiązywanie problemów](#rozwiązywanie-problemów)

---

## Co jest w środku

```
ŚCIEŻKA A — BATCH (odpowiedź na pytanie główne)
Open-Meteo (ERA5) ─┐
GeoNames (miasta) ─┼─► landing ─► bronze ─► silver ────────────────► gold
scenariusze MC ────┘              meteo     meteo (klasa Pasquilla)   risk_map          (mapa ryzyka)
                                  cities    grid, city_cells          city_exposure  ◄── „czy X skazi Y?”
                                            scenarios, q_samples      site_ranking
                                            dispersion_episode        expected_dose     (dla czujników)

ŚCIEŻKA B — STREAMING (czujniki z celowo brudnymi danymi)
symulator ─► landing/sensor_stream ─► bronze.sensor_raw ─┬─ lag > 15 min ─► ops.sensor_late_rejected
                                                         └─ dedup ─► reguły twarde ─┬─► silver.sensor_clean
                                                                                    └─► ops.sensor_quarantine
silver.sensor_clean ─► silver.sensor_quality (dryf vs model, zamrożenie, sąsiedzi) ─► gold.live_alerts

ŚCIEŻKA C — CDC (zdolność zaawansowana)
rejestr urządzeń (INSERT/UPDATE/DELETE) ─► bronze.device_cdc ─► silver.devices (SCD typ 2)
```

| Element | Plik | Po co |
|---|---|---|
| Konfiguracja | `src/radplume/conf/*.yaml` | fizyka, lokalizacje, progi skażenia, reguły DQ, środowiska local/dev/prod |
| Sesja Spark | `src/radplume/core/session.py` | jedyne miejsce, które wie, czy działamy lokalnie, czy na Databricks |
| Przechowywanie | `src/radplume/core/storage.py` | tabele Delta: lokalnie katalogi, w chmurze Unity Catalog |
| Model fizyczny | `src/radplume/silver/dispersion.py` | smuga gaussowska w czystym Sparku (Briggs, Pasquill, depozycja, rozpad) |
| Agregacja MC | `src/radplume/gold/aggregates.py` | prawdopodobieństwa i percentyle po scenariuszach |
| Czyszczenie czujników | `src/radplume/silver/sensor_clean.py`, `sensor_quality.py` | spóźnienia, duplikaty, spike, dryf, zamrożenie |
| Aplikacja | `src/radplume/app/` | odpowiedzi o miasta + guardrails SQL |
| Testy | `tests/` | wartości analityczne fizyki, bilans masy, DQ, CDC, idempotencja |

Kod ma **dużo komentarzy** wyjaśniających, *dlaczego* wybrano dane rozwiązanie,
a nie inne. Warto czytać go razem z planem.

**Jak to jest zbudowane i dlaczego:** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

**Jak czytać kod (od czego zacząć, za co odpowiada każdy plik):** [`docs/HOWTOREAD.md`](docs/HOWTOREAD.md).

**Jakie dane musisz przygotować sam i gdzie je wrzucić:**
[`docs/przygotowanie-danych.md`](docs/przygotowanie-danych.md). Do podstawowego
przebiegu nie trzeba niczego; ręcznie pobierasz tylko dane do walidacji modelu.

### Struktura repozytorium

```
.
├── src/radplume/
│   ├── conf/          # YAML: środowiska (local/dev/prod) + domena (fizyka, lokalizacje, progi, DQ)
│   ├── core/          # infrastruktura: config, sesja Spark, storage (ścieżki / Unity Catalog), metryki DQ
│   ├── ingest/        # konektory do PRAWDZIWYCH źródeł → strefa landing (Open-Meteo, GeoNames)
│   ├── simulators/    # generatory danych SYMULOWANYCH (czujniki, rejestr CDC) — osobno od ingest/
│   ├── bronze/        # landing → surowe tabele Delta + metadane pochodzenia
│   ├── silver/        # czyszczenie, walidacja, obliczenia fizyczne (czyste funkcje DataFrame → DataFrame)
│   ├── gold/          # agregaty biznesowe: mapa ryzyka, narażenie miast, alerty
│   ├── validation/    # ocena modelu na niezależnych pomiarach (FAC2/FAC5)
│   ├── app/           # warstwa serwująca: pytania o miasta, guardrails SQL
│   ├── pipelines/     # orkiestracja: kolejność kroków (1 krok = 1 task Joba), bez logiki obliczeń
│   └── cli.py         # punkt wejścia `radplume <krok>` (lokalnie i na Databricks)
├── tests/
│   ├── unit/          # szybkie testy pojedynczych funkcji (fizyka, meteo, DQ, CDC, guardrails)
│   └── integration/   # potok end-to-end w małej skali (idempotencja, spójność wyników)
├── resources/         # zasoby Databricks Asset Bundle (Joby) — szkielet
├── docs/              # dokumentacja: architektura, jak czytać kod, przygotowanie danych
├── data/              # (git-ignored) landing, tabele Delta, checkpointy — tworzone przy uruchomieniu
├── databricks.yml     # definicja bundla (targety dev/prod)
├── Dockerfile, docker-compose.yml
└── plan_finalny_radplume.md, final-project-spec.md
```

Zasady podziału (typowe dla projektów data engineering):
- **Transformacje** (`bronze/silver/gold`) to czyste funkcje: nie czytają konfiguracji ani plików
  same, więc da się je testować na małych DataFrame'ach.
- **Orkiestracja** (`pipelines/`) tylko czyta tabele wejściowe, woła transformację i zapisuje wynik.
- **Infrastruktura** (`core/`) to jedyne miejsce zależne od środowiska (lokalnie / Databricks).
- **Dane prawdziwe** (`ingest/`) i **symulowane** (`simulators/`) są rozdzielone, żeby od razu było widać, co jest realne.

---

## Uruchomienie: Docker (zalecane, także Windows)

Spark na natywnym Windowsie wymaga `winutils.exe`, `hadoop.dll` i ręcznego
ustawiania `HADOOP_HOME`, co zwykle kończy się godzinami walki z błędami.
Kontener Linux działa tak samo na każdym systemie i tak samo jak CI.

**Wymagania:** [Docker Desktop](https://www.docker.com/products/docker-desktop/)
z przydzielonymi co najmniej **6 GB RAM** (Settings → Resources).

W PowerShellu, w katalogu repozytorium:

```powershell
# 1. Zbuduj obraz (raz, ok. 3–5 min: Python, Java 17, Spark, jary Delta)
docker compose build

# 2. Szybki test bez internetu (dane syntetyczne, ok. 2–3 min)
docker compose run --rm radplume radplume --offline run-all

# 3. Pełny przebieg na prawdziwych danych (Open-Meteo + GeoNames)
docker compose run --rm radplume radplume run-all

# 4. Pytanie
docker compose run --rm radplume radplume ask --site lubiatowo_kopalino --city Lębork
```

Repozytorium jest zamontowane w kontenerze, więc:
- zmiany w kodzie działają od razu, bez przebudowy obrazu,
- wyniki lądują w `data/` (albo `data-offline/`) na Twoim dysku.

Żeby nie pisać za każdym razem `docker compose run --rm radplume`, otwórz powłokę
w kontenerze i wydawaj komendy `radplume ...` bezpośrednio:

```powershell
docker compose run --rm radplume bash
```

---

## Uruchomienie: natywnie (Linux / macOS / WSL2)

**Wymagania:** Python 3.10–3.12 i **Java 17 lub nowsza** (Spark 4 nie działa na Java 8/11).

```bash
java -version            # musi pokazać 17+
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"  # instaluje pakiet + komendę `radplume`

radplume --offline run-all   # szybki test
radplume run-all             # prawdziwe dane
```

Przy pierwszym starcie Spark pobierze jary Delta Lake z Maven Central
(ok. 5 MB, potem są w cache `~/.ivy2.5.2`).

---

## Pierwszy przebieg krok po kroku

`run-all` = `run-batch` + `run-stream`. Kroki można też uruchamiać pojedynczo:
każdy jest idempotentny, więc ponowne uruchomienie nie dubluje danych.

```bash
radplume ingest-meteo     # pobiera meteo (Open-Meteo ERA5) do data/landing/meteo/
radplume ingest-cities    # pobiera listę miast (GeoNames cities5000) do data/landing/cities/
radplume bronze           # landing → tabele Delta bronze (MERGE po kluczu)
radplume silver           # kontrola jakości meteo, klasa Pasquilla, siatka, miasta, scenariusze MC
radplume dispersion       # model smugi dla wszystkich scenariuszy
radplume gold             # mapa ryzyka, narażenie miast, ranking lokalizacji
radplume validation       # porównanie z pomiarami (pomijane, jeśli brak plików)
radplume expected-dose    # oczekiwana moc dawki dla symulatora czujników

radplume device-registry  # rejestr czujników jako CDC → silver.devices (SCD2)
radplume sensor-sim       # generuje brudne odczyty czujników
radplume sensor-stream    # Structured Streaming: spóźnione / kwarantanna / czyste
radplume sensor-quality   # dryf, zamrożenie, sąsiedzi
radplume alerts           # alerty + metryki jakości danych
```

Orientacyjne czasy na laptopie (4 rdzenie) w konfiguracji `local`:

| Etap | Offline | Prawdziwe dane |
|---|---|---|
| pobranie meteo (2 lokalizacje × 10 lat) | — | 1–3 min (tylko za pierwszym razem, potem cache) |
| batch (bronze → gold) | ~1 min | ~2–4 min |
| czujniki | ~1 min | ~1 min |

Dane pobrane z API zostają w `data/landing/` i działają jak cache. Chcesz pobrać
je od nowa? Usuń odpowiedni plik lub katalog.

---

## Zadawanie pytań

```bash
# Czy uwolnienie w Lubiatowie skazi Lębork? (polskie znaki opcjonalne)
radplume ask --site lubiatowo_kopalino --city Lebork

# Walidacyjny epizod Fukushimy z prawdziwą pogodą z marca 2011
radplume ask --site fukushima_daiichi --city Iwaki --scenario-set validation_2011

# Jakie miasta są w zasięgu (100 km)?
radplume list-cities --site lubiatowo_kopalino

# Podgląd dowolnej tabeli
radplume show gold site_ranking
radplume show gold live_alerts -n 50
```

Przykładowa odpowiedź (dane offline, więc liczby są syntetyczne):

```
Lubiatowo-Kopalino → Lębork: 29,0 km, azymut 191°, ok. 35 000 mieszkańców.
Zestaw scenariuszy: climatology — 1080 kombinacji (pogoda × warianty fizyczne × ilość uwolnienia).

• teren skażony (definicja po Czarnobylu, IAEA): przekroczony w 15,6% scenariuszy
• strefa ścisłej kontroli (po Czarnobylu): przekroczony w 9,0% scenariuszy
...
Depozycja Cs-137 [kBq/m²]: P5 = 0,0, mediana = 0,0, P95 = 4 576,9
Mediana czasu dotarcia smugi: 12,3 h od początku uwolnienia; najgroźniejszy wiatr: z sektora N.

Zastrzeżenie: to wynik prostego modelu gaussowskiego ...
SQL:
SELECT ... FROM delta.`.../gold/city_exposure` WHERE site_id = :site AND city_name = :city ...
```

Zasady (plan P13):
- każda liczba w odpowiedzi pochodzi z tabeli `gold.city_exposure`,
- dla miasta albo lokalizacji, których model nie liczył, program **odmawia**, zamiast zgadywać,
- odpowiedź zawsze pokazuje użyty SQL.

Lokalnie SQL jest szablonem. Na Databricks wygeneruje go LLM (Foundation Model API)
i przejdzie przez te same guardrails (`app/guardrails.py`).

**Uwaga:** mediana 0 przy P95 = 4577 to nie błąd. W większości scenariuszy
pogodowych wiatr wieje gdzie indziej, a skażenie pojawia się tylko w ich części.
Dlatego podstawową miarą jest `p_exceed`, a nie średnia.

---

## Kroki potoku i tabele

Lokalnie tabele leżą w `data/delta/<warstwa>/<tabela>/`, na Databricks jako
`radplume_<env>.<warstwa>.<tabela>`.

| Tabela | Klucz | Opis |
|---|---|---|
| `bronze.meteo` | site_id, time_utc | godzinowe meteo z API + współrzędne żądane i zwrócone, plik źródłowy |
| `bronze.cities` | city_id | miasta z populacją (GeoNames albo lista zapasowa) |
| `silver.meteo` | site_id, time_utc | wiatr w m/s, kierunek smugi (+180°), składowe u/v, klasa Pasquilla |
| `silver.grid` | cell_id | siatka ±100 km wokół każdej lokalizacji (układ lokalny w km) |
| `silver.city_cells` | site_id, city_id | miasta w promieniu 100 km przypisane do komórek |
| `silver.scenarios` | scenario_set, site_id, episode_id, variant_id | epizody pogodowe × warianty fizyczne |
| `silver.q_samples` | site_id, nuclide, q_sample_id | próbki ilości uwolnienia (lognormal) |
| `silver.dispersion_episode` | scenariusz × komórka × nuklid | depozycja na 1 Bq uwolnienia, czas dotarcia, dominujący wiatr |
| `gold.risk_map` | scenario_set, site, cell, próg | P(przekroczenia), P5/P50/P95 depozycji |
| `gold.city_exposure` | scenario_set, site, city, próg | **odpowiedź na pytanie główne** |
| `gold.site_ranking` | scenario_set, site, próg | oczekiwana liczba mieszkańców w miastach z przekroczeniem |
| `gold.expected_dose` | cell, hour | oczekiwana moc dawki dla czujników (scenariusz demo) |
| `silver.devices` | device_id, valid_from | rejestr czujników, SCD typ 2 |
| `bronze.sensor_raw` | — | surowe odczyty (append-only, niezmienne) |
| `ops.sensor_late_rejected` | — | odczyty spóźnione > 15 min (odseparowane, nie zgubione) |
| `ops.sensor_quarantine` | — | odczyty łamiące reguły twarde, z powodem |
| `silver.sensor_clean` | device_id, event_time | odczyty po czyszczeniu, z wersją urządzenia z chwili odczytu |
| `silver.sensor_quality` | device_id, event_time | status: clean / warmup / frozen / drift_suspect / wind_jump |
| `gold.live_alerts` | device, hour, typ | `outside_model_band` (sygnał) vs `sensor_fault` (usterka) |
| `gold.sensor_dq_summary` | metric | % spóźnionych, w kwarantannie, statusy (dla dashboardu) |
| `ops.dq_metrics` | — | dziennik metryk jakości z każdego przebiegu |

**Zestawy scenariuszy (`scenario_set`):**
- `climatology`: losowe momenty awarii z 10 lat pogody. To jest produkt, czyli odpowiedź „co jeśli”.
- `validation_2011`: prawdziwe daty awarii w Fukushimie. Służy do porównania z pomiarami.

---

## Demo czujników (brudne dane)

Symulator (`simulators/sensor_sim.py`) generuje odczyty 10 stacji wokół Fukushimy
w pogodzie z marca 2011. Scenariusz demo wymusza każdy przypadek w przewidywalnym
momencie:

| Zdarzenie | Urządzenie | Oczekiwany wynik |
|---|---|---|
| awaria łączności 20 min → wysyłka zaległej paczki | dev-01 | odczyty z lag > 15 min w `ops.sensor_late_rejected` |
| wiatr 80 m/s | dev-04 | `ops.sensor_quarantine`, powód `wind_physical` |
| dawka poza zakresem detektora | dev-05 | `ops.sensor_quarantine`, powód `dose_in_detector_range` |
| brak dawki przy zasilaniu | dev-08 | `ops.sensor_quarantine`, powód `no_null_when_powered` |
| dryf detektora +1%/min przez 2 h | dev-06 | `drift_suspect` → alert `sensor_fault` |
| zamrożony odczyt przez 15 min | dev-07 | `frozen` |
| ponowna wysyłka tej samej paczki | dev-02 | jeden rekord w `silver.sensor_clean` |
| **odchylenie ×5 u trzech sąsiadów naraz** | dev-00..02 | `clean` + alert `outside_model_band`: realny sygnał, nie usterka |
| aktualizacja firmware v1 → v2 (CDC) | dev-03 | nowa kolumna `detector_temp_c`, dwie wersje w `silver.devices` |
| wycofanie urządzenia (CDC DELETE) | dev-09 | urządzenie milknie, wersja zamknięta |

```bash
radplume sensor-reset                # wyczyść ścieżkę czujników (pliki, checkpointy, tabele)
radplume run-stream                  # przelicz od nowa
radplume show gold sensor_dq_summary
radplume show gold live_alerts -n 60
```

Na prezentację ustaw w `conf/local.yaml` `sensor_sim.realtime_sleep_s: 0.5`.
Dane będą wtedy „płynąć na żywo” i można uruchomić `sensor-stream` w drugim terminalu.

---

## Konfiguracja

Wszystkie pliki leżą w `src/radplume/conf/` i jadą w paczce, więc Databricks dostaje tę samą konfigurację.

| Plik | Zawartość |
|---|---|
| `local.yaml` / `dev.yaml` / `prod.yaml` | **tylko** to, co różni środowiska: gdzie są dane, skala (siatka, liczba scenariuszy) |
| `sites.yaml` | lokalizacje: współrzędne, jurysdykcja (RLS), ilość uwolnienia, epizod walidacyjny |
| `physics.yaml` | nuklidy (rozpad, depozycja, ground shine), profil wiatru, rozkłady wariantów |
| `thresholds.yaml` | definicja „skażenia”: progi depozycji i dawki, promień miast |
| `sensor_dq.yaml` | progi jakości danych czujników, scenariusz demo |

Środowisko wybierasz przez `--env local|dev|prod` albo zmienną `RADPLUME_ENV`.
Katalog danych możesz przenieść zmienną `RADPLUME_DATA_DIR`.

**Nowa lokalizacja = zero zmian w kodzie:** dopisz wpis w `sites.yaml` i jej
`site_id` do `run.sites` w `local.yaml`, potem uruchom `radplume run-batch`.

> ⚠️ Współrzędne Lubiatowa-Kopalina są **przybliżone** (test API zwrócił
> `elevation: 0.0`, czyli linię brzegową). Przed poważnymi wnioskami podmień je
> na dokładne z dokumentów PEJ (plan, P21).

---

## Walidacja na prawdziwych pomiarach

Repozytorium celowo **nie** zawiera pomiarów. Nie ma w nim „przykładowych” liczb
udających dane z Fukushimy. Pobierz pomiary depozycji Cs-137 z
[JAEA EMDB](https://emdb.jaea.go.jp/emdb/) (pomiary lotnicze MEXT/DOE, próbki
gleby) i zapisz jako CSV w `data/landing/validation/deposition/`
(szczegóły, format i pułapki: [`docs/przygotowanie-danych.md`](docs/przygotowanie-danych.md)):

```csv
site_id,lat,lon,nuclide,measured_kbq_m2,source
fukushima_daiichi,37.60,140.75,Cs-137,1234.5,JAEA-airborne-2011
```

Potem uruchom `radplume validation`. Wyniki trafią do `gold.validation`:
FAC2, FAC5, pokrycie przedziału P5–P95 i średni błąd logarytmiczny.

---

## Testy

```bash
pytest -q           # 61 testów, ok. 1–2 min (lokalny Spark)
ruff check src tests
```

W Dockerze: `docker compose run --rm radplume pytest -q`.

| Plik | Co sprawdza |
|---|---|
| `test_physics.py` | wzory Briggsa, stężenie na osi i w poprzek (wartości analityczne), **bilans masy**, całka kolumny, kierunek smugi |
| `test_meteo.py` | odrzucenie km/h, kierunek wiatru (270° → wschód, 311° → SE), klasy Pasquilla |
| `test_sensor_dq.py` | lag 10/20 min, duplikat, spike, dryf vs odchylenie obszarowe, zamrożenie, tło ≠ zamrożenie, warm-up |
| `test_sensor_sim_and_cdc.py` | determinizm generatora, ~90% odczytów w paśmie modelu, SCD2 z UPDATE/DELETE/duplikatem |
| `test_guardrails_and_app.py` | blokada DROP/DELETE/INSERT, tabel spoza gold, wielu zapytań; normalizacja nazw miast |
| `test_pipeline.py` | **idempotencja** (dwa przebiegi → te same sumy kontrolne), spójność progów, każde miasto ma odpowiedź |

CI (`.github/workflows/ci.yml`) uruchamia lint i testy na każdym PR.

---

## Przejście do chmury

To, co się zmienia, jest **wyłącznie** w konfiguracji i w pliku bundla:

| Lokalnie | Databricks |
|---|---|
| `storage.mode: path` → `data/delta/...` | `storage.mode: catalog` → `radplume_dev.silver.meteo` (Unity Catalog) |
| `data/landing/` | volume `/Volumes/radplume_dev/raw/landing` |
| sesja budowana w `core/session.py` | sesja klastra (`DATABRICKS_RUNTIME_VERSION` wykryte automatycznie) |
| Structured Streaming z katalogu plików | Auto Loader (`cloudFiles`) + `schemaEvolutionMode=addNewColumns` |
| SCD2 funkcjami okna (`silver/devices_cdc.py`) | `dlt.create_auto_cdc_flow(..., stored_as_scd_type=2)` |
| `radplume <krok>` w terminalu | task Lakeflow Joba (`resources/job_radplume.yml`) z tym samym entry pointem |
| szablon SQL w `ask` | text-to-SQL na Foundation Model API + te same guardrails |

`databricks.yml` i `resources/` to **szkielet**. Przed pierwszym deployem
(bloki 1–3 planu) trzeba:
- uzupełnić adresy workspace'ów,
- wykonać jednorazowy bootstrap (katalogi, volume, grupy, secret scope z Key Vault),
- dodać `sql/governance.sql` (RLS/CLS).

---

## Ograniczenia

Pełna lista jest w planie (Część VIII). Najważniejsze:
- model gaussowski: płaski teren, jednorodny wiatr w domenie, zasięg ok. 100 km,
- meteo ERA5 ok. 25 km, więc lokalna bryza i rzeźba terenu są wygładzone,
- **tylko uwolnienie do atmosfery**, bez skażenia wód,
- ilość uwolnienia dla Lubiatowa jest **hipotetyczna** (skala Fukushimy), a nie wynik analizy bezpieczeństwa AP1000,
- dane czujników są symulowane z tego samego modelu, więc alerty sprawdzają potok, a **nie trafność modelu**. Trafność ocenia tylko walidacja na pomiarach JAEA.

Narzędzie służy do porównywania scenariuszy, **nie** do prognoz ani decyzji kryzysowych.

---

## Rozwiązywanie problemów

| Objaw | Przyczyna / rozwiązanie |
|---|---|
| `UnsupportedClassVersionError`, `Java gateway process exited` | za stara Java; Spark 4 wymaga 17+ (`java -version`) |
| na Windows: `winutils`, `HADOOP_HOME`, `UnsatisfiedLinkError` | uruchamiaj przez Docker albo WSL2, nie natywnie |
| `OutOfMemoryError` / kontener zabity | zwiększ RAM Dockera do ≥ 6 GB albo `spark.driver_memory` w `local.yaml`; zmniejsz `grid_size` / `n_episodes` |
| `MeteoValidationError: ... km/h` | zapytanie do API bez `wind_speed_unit=ms` (plan P21) — plik celowo nie trafia do landing |
| `HTTP 429` z Open-Meteo | limit zapytań; ingest sam ponawia z opóźnieniem, a pobrane lata zostają w cache |
| brak internetu | `radplume --offline run-all` (dane syntetyczne w osobnym katalogu `data-offline/`) |
| „Smuga nie dociera do żadnej komórki w oknie symulacji” | zmień `demo.sim_start_offset_h` w `sensor_dq.yaml` |
| chcę zacząć od zera | usuń `data/` (albo `data-offline/`) |
