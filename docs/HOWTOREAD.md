# Jak czytać to repozytorium i jego wyniki

> **Dla kogo:** osoba, która widzi repozytorium pierwszy raz (prowadzący, recenzent, kolejny agent AI) i chce szybko
> wiedzieć, gdzie co jest i jak czytać tabele wynikowe. **Stan na:** 2026-10-06.
> Architektura i uzasadnienia: [`ARCHITECTURE.md`](ARCHITECTURE.md). Dane wejściowe: [`przygotowanie-danych.md`](przygotowanie-danych.md).
> Decyzje, etapy i dziennik: [`PLAN_SMOG.md`](../PLAN_SMOG.md).

---

## 0. Cztery nieporozumienia, które warto wyjaśnić na starcie

1. **Model nie uczy się z API na żywo.** Uczy się raz, na **archiwum** GIOŚ 2021–2024 (rok 2025 to test), i jest
   zapisywany. API na żywo dostarcza tylko **cechy dnia dzisiejszego**, z których zapisany model liczy prognozę na jutro.
2. **Popsute dane z demo nigdy nie trafiają do modelu.** `fault_replay` to kopia prawdziwych pomiarów z wstrzykniętymi
   usterkami, osobnym torem (`source = 'fault_replay'`) — służy do pokazania, że czyszczenie działa przewidywalnie.
   Ten sam kod czyści prawdziwy strumień `live`.
3. **To nie jest Monte Carlo.** Prawdopodobieństwo liczy wyuczony klasyfikator (gradient boosted trees, Spark MLlib)
   w jednym przebiegu. „Na ile to pewne” sprawdzamy kalibracją na roku testowym, a nie losowaniem scenariuszy.
4. **Panel tylko czyta tabele gold.** Nie pobiera danych i nie uruchamia potoku — robią to Joby (Databricks) albo
   komendy `smogcast run-…` (lokalnie).

## 1. Od czego zacząć (30 minut)

| Kolejność | Co | Po co |
|---|---|---|
| 1 | [`README.md`](../README.md) | pytanie biznesowe, wyniki, jak uruchomić |
| 2 | [`ARCHITECTURE.md`](ARCHITECTURE.md), sekcje 1–3 | przepływ danych (ścieżki A/B/C), wheele, grafy tasków |
| 3 | ten plik, sekcje 2–4 | gdzie jest kod i jak wygląda jeden krok od środka |
| 4 | [`resources/job_smogcast.yml`](../resources/job_smogcast.yml) | ten sam potok jako Job Databricks |
| 5 | [`silver_transform/features.py`](../packages/silver_transform/src/smogcast/silver_transform/features.py) i [`model/evaluate.py`](../packages/model/src/smogcast/model/evaluate.py) | serce projektu: cechy bez wycieku i uczciwa ocena |
| 6 | ten plik, sekcja 7 | jak czytać tabele wynikowe |

## 2. Mapa repozytorium

```
.
├── packages/                  kod: osobny wheel na każdy krok potoku (decyzja D14)
│   ├── core/                  wspólna infrastruktura + konfiguracja conf/*.yaml (jedyna paczka, którą importują inne)
│   ├── ingest/ bronze/ silver_clean/ silver_transform/ model/ gold/     wheele kroków
│   ├── app/                   panel Streamlit + asystent (guardrails SQL, RAG, model językowy)
│   └── cli/                   lokalna komenda `smogcast` + test integracyjny całego potoku
├── resources/                 definicje Lakeflow Jobów (batch, na żywo)
├── databricks.yml             Asset Bundle: artefakty (wheele), targety dev/prod
├── sql/                       skrypty SQL dla Unity Catalog (bootstrap katalogu, schematów, Volume'ów)
├── docs/                      dokumentacja robocza (po polsku); docs/rag/ — baza wiedzy asystenta (po angielsku)
├── .github/workflows/ci.yml   CI: lint, testy jednostkowe, test integracyjny, budowa wheeli
├── Dockerfile, docker-compose.yml   lokalne środowisko (Spark 4 + Delta + Java 17)
├── conftest.py, pyproject.toml      wspólne fikstury pytest i konfiguracja narzędzi (ruff, pytest)
├── VERSION                    wspólna wersja wszystkich wheeli
└── PLAN_SMOG.md, HANDOFF.md, CLAUDE.md   plan i dziennik, przekazanie pracy, zasady dla agentów AI
```

Infrastruktura Azure (Key Vault, service principal, grupy, katalog) jest w **osobnym repo Terraform**
(`smog-cast-terraform`) — to repo dostaje z niego tylko wartości (adres workspace'u, id principala).

## 3. Anatomia paczki krokowej

Każda paczka krokowa ma ten sam układ — po przeczytaniu jednej czyta się wszystkie:

```
packages/bronze/
├── pyproject.toml                 zależność od smogcast-core==<VERSION>, entry point smogcast-bronze
├── src/smogcast/bronze/           (brak src/smogcast/__init__.py — wspólna przestrzeń nazw PEP 420)
│   ├── main.py                    STEPS = {"pm-hourly": step_pm_hourly, …};  main = make_main("bronze", STEPS)
│   ├── steps.py                   kroki: funkcje (Context) -> None — czytają wejście, wołają transformacje, zapisują
│   └── pm_archive.py, weather.py… transformacje: czyste funkcje DataFrame -> DataFrame (łatwe do testowania)
└── tests/                         testy jednostkowe paczki
```

**Jak wykonuje się jeden krok** (na przykładzie `smogcast-bronze --step pm-hourly --env dev`):

1. [`core/runner.py`](../packages/core/src/smogcast/core/runner.py) `make_main` — parsuje `--step`, `--env`, `--offline`
   (Spark startuje dopiero po parsowaniu, więc literówka kończy się od razu).
2. [`core/config.py`](../packages/core/src/smogcast/core/config.py) `load_config("dev")` — wspólne pliki domenowe
   (`cities.yaml`, `thresholds.yaml`, `model.yaml`…) + **jeden** plik środowiska (`local.yaml` / `dev.yaml` / `prod.yaml`).
   Środowiska różnią się **tylko** tym plikiem — w kodzie nie ma `if env == …`.
3. [`core/context.py`](../packages/core/src/smogcast/core/context.py) — `Context(cfg, spark, storage)`; sesja Sparka:
   lokalna z Deltą albo istniejąca sesja klastra ([`core/session.py`](../packages/core/src/smogcast/core/session.py)).
4. `STEPS["pm-hourly"](ctx)` → [`bronze/steps.py`](../packages/bronze/src/smogcast/bronze/steps.py) → transformacje z
   `pm_archive.py` → zapis przez [`core/storage.py`](../packages/core/src/smogcast/core/storage.py).
5. `Storage` ukrywa różnicę lokalnie/chmura: `storage.mode: path` → katalogi `data/delta/<warstwa>/<tabela>`;
   `storage.mode: catalog` → tabele `smogcast_dev.<warstwa>.<tabela>` i Volume'y. Ta sama metoda `overwrite` /
   `merge` / `append_idempotent` w obu trybach.
6. Metryki jakości kroku → `ops.dq_metrics` ([`core/dq_metrics.py`](../packages/core/src/smogcast/core/dq_metrics.py)).

Ten sam krok wywołują: task Joba na Databricks (`python_wheel_task`), lokalne `smogcast run bronze pm-hourly` i testy —
jedna ścieżka kodu dla wszystkich trzech.

## 4. Gdzie jest która logika

### `smogcast-core` — infrastruktura i kontrakty

| Moduł | Za co odpowiada |
|---|---|
| `config.py` | ładowanie konfiguracji, `active_cities` |
| `session.py` | sesja Sparka (lokalnie z Deltą, na klastrze — istniejąca); strefa UTC |
| `storage.py` | odczyt/zapis tabel i ścieżki landing/checkpoint/models w obu trybach; zapisy idempotentne |
| `timeutil.py` | **jedyne** miejsce konwersji czasu (CET, czas lokalny, UTC, doba CET, dzień wydania prognozy) |
| `schema.py` | wspólne kontrakty: nagłówki plików GIOŚ (polskie etykiety — nie tłumaczyć), schemat cech |
| `runner.py`, `context.py` | wspólny punkt wejścia wheeli i kontekst kroku |
| `dq_metrics.py` | dziennik metryk jakości `ops.dq_metrics` |
| `conf/*.yaml` | cała konfiguracja (sekcja 6) — w wheelu jako package data |

### Kroki potoku

| Ścieżka | Krok (`paczka krok`) | Moduł | Wejście → wyjście |
|---|---|---|---|
| A | `ingest gios-archive` | `ingest/gios_archive.py` | GIOŚ → `landing/gios_archive/raw` (z ochroną przed złym plikiem) |
| A | `ingest weather-forecast-history` | `ingest/weather.py` | Open-Meteo → `landing/weather/{short_range,day_ahead}` |
| A | `bronze station-meta` | `bronze/station_meta.py` | metadane xlsx → `bronze.gios_stations`, `bronze.gios_positions` |
| A | `bronze pm-hourly` | `bronze/pm_archive.py` | zip/xlsx (format szeroki) → `bronze.pm_hourly` (format długi, UTC) |
| A, B | `bronze weather-forecast` | `bronze/weather.py` | JSON → `bronze.weather_forecast` |
| A | `silver-clean pm-hourly` | `silver_clean/station_city.py`, `pm_clean.py` | → `silver.station_city`, `silver.pm_hourly` (+ `ops.pm_quarantine`) |
| A | `silver-transform pm-daily` | `silver_transform/pm_daily.py` | → `silver.pm_daily_station`, `silver.pm_daily_city` (reguła 18 h, miasto = najgorsza stacja) |
| A, B | `silver-transform weather-daily` | `silver_transform/weather_daily.py` | → `silver.weather_daily` (doba CET) |
| A | `silver-transform features` | `silver_transform/features.py` | → `silver.features` (cechy z D, etykieta z D+1) |
| A | `model train` | `model/prepare.py`, `train.py`, `baselines.py` | siatka na walidacji 2024 → `gold.model_selection`, modele `tuned`/`final` |
| A | `model backtest` | `model/evaluate.py` | → `gold.backtest_predictions`, `backtest_metrics`, `reliability`, `acceptance` |
| C | `ingest gios-registry` → `bronze station-snapshots` → `silver-clean stations-scd2` | `ingest/gios_registry.py`, `bronze/station_snapshots.py`, `silver_clean/stations_scd2.py` | migawki rejestru → CDC `silver.station_changes` → SCD2 `silver.stations` |
| B | `ingest gios-live` | `ingest/gios_live.py` | API → `landing/gios_live` |
| B | `silver-clean pm-stream` | `silver_clean/pm_stream.py` (+ reguły z `pm_clean.py`) | Structured Streaming → `bronze.pm_stream`, `ops.pm_late_rejected`, `ops.pm_quarantine`, `silver.pm_stream` |
| B | `ingest weather-forecast-live` | `ingest/weather.py` | prognoza na jutro → `landing/weather/live` |
| B | `silver-transform features-live` | `silver_transform/features.py` (te same funkcje co historia) | → `silver.features_live` |
| B | `model forecast` | `model/steps.py` | zapisany model `final` → `gold.forecast_tomorrow` |
| B | `gold dq-summary` | `gold/dq_summary.py` | → `gold.dq_summary` |
| demo | `silver-clean fault-reset` → `ingest fault-replay` → … | `ingest/fault_replay.py` | usterki wg `fault_injection.yaml` → te same tabele strumienia, `source = 'fault_replay'` |
| testy | (podpięty pod kroki ingest) | `ingest/synthetic.py` | pliki w formatach źródeł, wartości deterministyczne |

Kolejności kroków są w dwóch miejscach, które muszą się zgadzać: `BATCH_ORDER` / `LIVE_ORDER` / `FAULT_DEMO_ORDER` w
[`cli/main.py`](../packages/cli/src/smogcast/cli/main.py) i grafy tasków w `resources/job_*.yml` (pilnuje tego test
`packages/cli/tests/test_cli.py`).

### Aplikacja (`smogcast-app`)

| Moduł | Za co odpowiada |
|---|---|
| `forecast.py` | odpowiedź „czy jutro w Y?” — deterministyczna, bez modelu językowego, z historią kalibracji |
| `guardrails.py` | SQL od modelu językowego: tylko jeden `SELECT`, tylko tabele gold z białej listy, `LIMIT` (parser `sqlglot`, nie wyrażenia regularne) |
| `gold.py` | odczyt gold (podmiana `gold.<tabela>` na nazwę środowiska) |
| `assistant.py` | routing pytań (prognoza / liczby → text-to-SQL / wiedza → RAG / poza tematem → odmowa) |
| `rag.py` | indeks dokumentów, wyszukiwanie hybrydowe (embeddingi + BM25) |
| `llm.py` | dowolny endpoint zgodny z OpenAI (lokalnie LM Studio) |
| `ui/` | Streamlit: `views.py` (strony Info, Tomorrow, City, Model, Data quality, Assistant), `charts.py`, `theme.py` |

## 5. Uruchamianie (lokalnie, Docker)

```powershell
docker compose build                                                    # raz (kilka minut)
docker compose run --rm smogcast smogcast steps                         # lista kroków wszystkich wheeli
docker compose run --rm smogcast smogcast run-batch                     # historia → model (prawdziwe dane, internet; dziesiątki minut — w tle)
docker compose run --rm smogcast smogcast run-live                      # prognoza na jutro (wymaga run-batch)
docker compose run --rm smogcast smogcast run-fault-demo                # demo czyszczenia (2–3 min)
docker compose run --rm smogcast smogcast show gold forecast_tomorrow -n 20
docker compose run --rm smogcast smogcast ask --city Kraków             # odpowiedź słowami, bez modelu językowego
docker compose up -d app                                                # panel: http://localhost:8501
```

Pojedynczy krok tak jak w tasku Joba: `docker compose run --rm smogcast smogcast-bronze --step pm-hourly`.
Bez internetu: `smogcast --offline run-batch` — dane syntetyczne w `data-offline/`, **wyniki nie są prawdziwe**.

## 6. Konfiguracja (`packages/core/src/smogcast/core/conf/`)

| Plik | Co ustawia |
|---|---|
| `local.yaml`, `dev.yaml`, `prod.yaml` | **jedyna różnica między środowiskami**: tryb i miejsce zapisu, katalog UC, Volume'y, ustawienia Sparka, miasta, lata |
| `cities.yaml` | 10 miast: nazwa w GIOŚ, województwo i `jurisdiction_code` (RLS), współrzędne do pogody; zanieczyszczenia |
| `thresholds.yaml` | normy (PM10 50, PM2.5 25 µg/m³) ze źródłami, reguła 18 h, próg ostrzeżenia 0,5 |
| `model.yaml` | podział lat (trening/walidacja/test), cechy, siatki parametrów, **kryteria K1–K4 (zamrożone)** |
| `pm_dq.yaml` | reguły jakości: kwarantanna, spóźnienia, zamrożenie, skok, dryf |
| `gios.yaml`, `weather.yaml` | adresy i identyfikatory źródeł |
| `fault_injection.yaml` | okno i harmonogram usterek demo |
| `synthetic.yaml` | generator danych testowych |
| `app.yaml` | aplikacja: model językowy, text-to-SQL, RAG (źródła dokumentów) |

## 7. Jak czytać wyniki (tabele gold)

Lokalnie: `smogcast show gold <tabela>`; na Databricks: `smogcast_<env>.gold.<tabela>`. Każdy widok panelu pokazuje
swój SQL, więc można go skopiować do SQL Editora.

### `gold.forecast_tomorrow` — odpowiedź na pytanie główne

Jeden wiersz na (miasto, zanieczyszczenie, dzień docelowy).

| Kolumna | Jak czytać |
|---|---|
| `issue_day` → `target_day` | prognoza wydana w dniu D (CET) na dzień D+1 |
| `probability` | prawdopodobieństwo, że **średnia dobowa najgorszej stacji miasta** przekroczy normę (`limit_ug_m3`) |
| `warned` | `probability ≥ 0,5` (próg z `thresholds.yaml`) |
| `status`, `unusable_reason` | `ok` albo `no_forecast` z powodem (np. brak wczorajszych pomiarów, niepełna prognoza pogody) — **nigdy zgadywana wartość** |
| `model`, `model_version`, `weather_source` | który model operacyjny i jaka prognoza pogody |
| `pm_d1_max_ug_m3`, `pm_d_morning_max_ug_m3`, `t_mean_c`, `wind_mean_ms`, `calm_hours`, `precip_sum_mm` | najważniejsze cechy — „dlaczego taka prognoza” (wczorajsza średnia, dzisiejszy poranek, pogoda na jutro) |
| `jurisdiction_code` | województwo — kolumna pod filtr wierszy (RLS) |

```sql
SELECT city_name, pollutant, target_day, ROUND(probability, 2) AS p, warned, status
FROM gold.forecast_tomorrow ORDER BY probability DESC;
```

### Jak dobry jest model — `backtest_metrics`, `reliability`, `acceptance`

- `gold.backtest_metrics` — wiersz na (model, split, zanieczyszczenie, miasto; `city_id = 'ALL'` = wszystkie miasta).
  `model` = `gbt` / `logistic` / `persistence` / `climatology`; `split` = `validation` (2024) / `test` (2025).

  | Metryka | Po ludzku | Lepiej |
  |---|---|---|
  | `brier` | średni błąd kwadratowy prawdopodobieństwa | niżej (0 = idealnie) |
  | `bss` | o ile lepiej niż klimatologia (średnia z kalendarza) | > 0 |
  | `auc` | jak dobrze model porządkuje dni od najbezpieczniejszych do najgroźniejszych | bliżej 1 |
  | `pod` | z dni z przekroczeniem — w ilu model ostrzegł | wyżej |
  | `far` | z ostrzeżeń — ile było fałszywych | niżej |
  | `csi` | łączna miara trafień bez dni „spokojnych” | wyżej |

- `gold.reliability` — kalibracja: grupy dni o podobnej prognozie (`bin_from`–`bin_to`), średnia prognoza
  (`mean_probability`) i jak często naprawdę było przekroczenie (`observed_frequency`); `gap` = rozbieżność.
  „70%” ma znaczyć ok. 70%. Przedziały z < 30 dniami nie są oceniane (K3).
- `gold.acceptance` — werdykt **K1–K4** na roku testowym 2025 (`criterion`, `value`, `threshold`, `passed`).
  Kryteria zamrożono **przed** treningiem i nie zmienia się ich po zobaczeniu wyniku.
- `gold.model_selection` — siatka parametrów z Brierem walidacyjnym; `operational = true` = model użyty do prognoz
  (najniższy Brier na walidacji 2024 — reguła zamrożona przed wynikami).

```sql
SELECT pollutant, criterion, description, ROUND(value, 3) AS value, threshold, passed
FROM gold.acceptance ORDER BY pollutant, criterion;
```

Obowiązujący werdykt (2026-10-06): **PM2.5 spełnia K1–K3; PM10 nie spełnia K4a** (wykrywalność 44% < 60%), a K3 jest
na granicy. Szczegóły i dlaczego PM10 jest trudniejszy: [`ARCHITECTURE.md`](ARCHITECTURE.md), sekcja 6, oraz
[`ulepszenia-pm10.md`](ulepszenia-pm10.md).

### Jakość danych — `gold.dq_summary` i tabele `ops`

- `gold.dq_summary` — per źródło (`live` / `fault_replay`): unikalne odczyty, ponowne wysyłki, spóźnione, kwarantanna
  wg powodu, flagi; `pct_of_unique` = udział w unikalnych odczytach.
- `ops.pm_quarantine` (z `quarantine_reason`), `ops.pm_late_rejected` (z `lag_hours`) — nic nie znika po cichu.
- `silver.pm_stream.dq_flag` = `ok` / `frozen` / `spike` / `drift` — odczyty oflagowane zostają, ale nie liczą się do
  prognozy.
- Gotowe zapytania z komentarzem: [`demo-jakosc-danych.md`](demo-jakosc-danych.md).

## 8. Testy

| Co | Gdzie | Komenda |
|---|---|---|
| wszystko (≈ 6 min) | `packages/*/tests/` | `docker compose run --rm smogcast pytest -q -p no:logging` |
| tylko jednostkowe | | `… pytest -q -p no:logging -m "not integration"` |
| test integracyjny (≈ 3 min) | [`cli/tests/test_integration.py`](../packages/cli/tests/test_integration.py) | `… pytest -q -p no:logging -m integration` |
| lint | | `… ruff check packages conftest.py` |

Testy-strażnicy, które warto znać (każdy pilnuje decyzji, której łatwo nieświadomie złamać):

| Test | Pilnuje |
|---|---|
| `test_no_leakage_changing_tomorrow_changes_only_the_label` (`silver_transform/tests`) | zmiana danych z D+1 zmienia **tylko** etykietę, nie cechy |
| `test_gbt_is_the_same_whatever_the_partitioning`, `test_float_noise_in_inputs_does_not_reach_the_model` (`model/tests`) | powtarzalność modelu między laptopem a klastrem |
| `cli/tests/test_cli.py` | kolejności kroków w CLI = grafy tasków Jobów |
| `core/tests/test_versions.py` | jedna wersja wszystkich wheeli; wheele nie ciągną PySparka na Databricks |
| `test_integration.py` | cały potok na danych syntetycznych: warstwy, ponowne uruchomienie bez dubli, K1–K4, prognozy, usterki, CDC |

Testy używają jednej sesji Sparka (`conftest.py`), a każdy test zapisujący tabele — własnego katalogu tymczasowego.

## 9. Wdrożenie na Databricks

- [`databricks.yml`](../databricks.yml) — artefakty (po wheelu na paczkę krokową; bez `app`), targety `dev`
  (`mode: development`) i `prod` (`mode: production`, `run_as` = service principal; wdraża tylko CI).
- [`resources/job_smogcast.yml`](../resources/job_smogcast.yml) — Job batch: task = `python_wheel_task` z wheelem kroku
  + `smogcast-core`, parametry `--step … --env ${var.env}`; klastry jednomaszynowe, typ maszyny z `var.node_type`.
- [`resources/job_smogcast_live.yml`](../resources/job_smogcast_live.yml) — Job na żywo (co godzinę, harmonogram
  wstrzymany).
- [`sql/bootstrap_uc.sql`](../sql/bootstrap_uc.sql) — katalog, schematy, Volume'y (docelowo przejmuje to Terraform).
- Co poprawiono przed pierwszym deployem i jak przebiegła walidacja na DEV:
  [`poprawki-przed-deployem.md`](poprawki-przed-deployem.md), [`migracja-databricks.md`](migracja-databricks.md).

## 10. Typowe zmiany — gdzie dotknąć

| Chcę… | Zmień | Pamiętaj |
|---|---|---|
| dodać miasto | `cities.yaml` + `run.cities` w `conf/<env>.yaml` | stacje dobiorą się same; nowy `jurisdiction_code` = nowa grupa RLS w Terraformie |
| dodać rok archiwum | `gios.yaml` (`yearly_file_ids`) + `run.archive_years` | sprawdź id po `Content-Disposition` ([`przygotowanie-danych.md`](przygotowanie-danych.md)) |
| dodać krok | funkcja w `steps.py` + wpis w `STEPS` paczki + task w `resources/job_*.yml` + kolejność w `cli/main.py` | test zgodności CLI i Jobów |
| dodać cechę modelu | `silver_transform/features.py`, `model.yaml`, `core/schema.py` | tylko informacja znana w D o 12:00 CET; ocena na nowym okresie, **nie** na 2025 |
| zmienić próg normy | `thresholds.yaml` | zmienia etykiety → ponowny trening i backtest |
| zmienić regułę jakości | `pm_dq.yaml` | demo usterek i test integracyjny pokażą skutek |
| podnieść wersję | `VERSION` + wszystkie `packages/*/pyproject.toml` | pilnuje `test_versions.py` |
