# Poprawki przed pierwszym deployem na Azure Databricks (DEV)

> **Dla kogo:** autor projektu i każdy, kto robi pierwszy `databricks bundle deploy`.
> **Stan na:** 2026-10-06. Wprowadzone poprawki P1.1–P1.3 z [`migracja-databricks.md`](migracja-databricks.md)
> plus jedna niespójność znaleziona przy okazji (układ Volume'ów). Nic z tego nie było jeszcze uruchomione na
> Databricks — sprawdzone lokalnie (testy, budowa wheeli, instalacja w czystym środowisku).

## W skrócie

| # | Problem | Poprawka | Stan |
|---|---|---|---|
| P1.1 | wheele dociągały PySparka i `delta-spark` z PyPI na klaster | oba pakiety w dodatku `[local]` wheela `smogcast-core` (tylko Docker i CI) | ✅ zrobione i sprawdzone |
| P1.2 | klastry `SINGLE_USER` bez właściciela; Joby na PROD działałyby jako osoba wdrażająca | `single_user_name` w obu Jobach, `run_as` (service principal) w targecie PROD | ✅ w konfiguracji; id principala do wpisania przed PROD |
| P1.3 | adresy workspace'ów to szablony `adb-XXXX…` | wyraźnie oznaczone miejsca do wpisania; wersja runtime opisana jako 17.3 LTS | ⚠️ **adres DEV musisz wpisać Ty** |
| nowe | dokumentacja mówiła o „volume `raw`”, a kod oczekuje **schematu** `raw` z trzema Volume'ami | skrypt `sql/bootstrap_uc.sql` (katalog, schematy, Volume'y) | ✅ skrypt gotowy, do uruchomienia raz na workspace |

---

## P1.1 — PySpark i Delta tylko lokalnie

**Problem.** Każdy wheel kroku zależy od `smogcast-core`, a ten miał w zależnościach `pyspark` i `delta-spark`.
Przy instalacji wheela na klastrze pip mógłby doinstalować PySparka z PyPI obok tego z runtime'u Databricks
(Databricks to odradza — może zepsuć sesję), a `delta-spark` z PyPI mógłby przesłonić wbudowany moduł `delta`,
z którego korzysta `storage.py` (`from delta.tables import DeltaTable`).

**Zmiana.**
- [`packages/core/pyproject.toml`](../packages/core/pyproject.toml): `pyspark` i `delta-spark` przeniesione z
  `dependencies` do `[project.optional-dependencies] local`. W zależnościach zostały tylko `pyyaml` i `tzdata`.
- [`Dockerfile`](../Dockerfile) i [`.github/workflows/ci.yml`](../.github/workflows/ci.yml): instalacja
  `-e "packages/core[local]"` zamiast `-e packages/core` — lokalnie i w CI Spark jest jak dotąd.
- Kod bez zmian: `configure_spark_with_delta_pip` był już importowany tylko w ścieżce lokalnej (`session.py`), a na
  klastrze sesję daje runtime (`SparkSession.builder.getOrCreate()`).
- Nowy test [`test_wheels_do_not_pull_spark_onto_databricks`](../packages/core/tests/test_versions.py): żaden
  `pyproject.toml` nie może mieć `pyspark`/`delta-spark` w zwykłych zależnościach, a dodatek `local` musi je mieć.

**Sprawdzone.**
- Metadane zbudowanego wheela `smogcast-core`: `Requires-Dist: pyyaml`, `tzdata`; `pyspark` i `delta-spark` tylko
  z `extra == "local"`.
- Instalacja (próbna) wheela kroku `smogcast-silver-transform` w czystym venv bez dostępu do PyPI: instaluje się
  tylko `PyYAML`, `tzdata`, `smogcast-core`, `smogcast-silver-transform` — **bez PySparka**.
- Obraz Dockera przebudowany z nowym Dockerfile (PySpark 4.0.4 z dodatku `[local]`); na nim ruff czysto i **pełny
  zestaw testów 127/127**; panel uruchomiony na nowym obrazie.

**Do sprawdzenia na klastrze:** `pip show pyspark` przed i po instalacji wheeli pokazuje tę samą wersję (runtime'u).

## P1.2 — tożsamość, z którą działają Joby

**Problem.** Klaster w trybie `SINGLE_USER` (wymagany przez Unity Catalog przy klasycznych klastrach) musi mieć
właściciela (`single_user_name`) i może go używać tylko ta tożsamość, jako która działa Job. Bez tego uruchomienie
Joba kończy się błędem. Na PROD Joby nie powinny działać jako osoba, która akurat wdrażała.

**Zmiana.**
- [`resources/job_smogcast.yml`](../resources/job_smogcast.yml) i
  [`resources/job_smogcast_live.yml`](../resources/job_smogcast_live.yml): w definicji klastra
  `single_user_name: ${workspace.current_user.userName}` — czyli tożsamość, która wykonuje deploy:
  - **DEV** (tryb `development`): Joby działają jako osoba wdrażająca → klaster należy do niej. Pasuje.
  - **PROD**: deploy robi **CI zalogowane jako service principal** (OIDC, E9) → `current_user` = principal, a
    `run_as` też wskazuje principala. Pasuje.
- [`databricks.yml`](../databricks.yml), target `prod`: `run_as: {service_principal_name: …}` z szablonem
  `00000000-…` i komentarzem TODO — wpisać **application (client) id** principala, gdy powstanie (E9).

**Uwaga.** Deploy na PROD z laptopa (jako człowiek) przy `run_as` = principal dałby klaster należący do człowieka
i Job działający jako principal — to się nie uruchomi. To zamierzone: PROD wdraża tylko CI (tak też wymaga kurs).

**Do sprawdzenia przy pierwszym deployu:** że `${workspace.current_user.userName}` dla service principala daje
jego application id (tak opisuje dokumentacja bundli; nie było jeszcze uruchomione).

## P1.3 — adresy workspace'ów i runtime

**Zmiana.** W [`databricks.yml`](../databricks.yml) adresy zostały szablonami (kod nie może ich znać), ale są
jednoznacznie oznaczone `# TODO: DEV workspace URL` / `PROD workspace URL`, a nagłówek pliku wymienia, co wpisać
przed deployem. Wersja runtime `17.3.x-scala2.13` opisana jako **17.3 LTS** (Spark 4.0 — ta sama główna wersja co
lokalnie, Python 3.12, a wheele wymagają `>=3.10,<3.13`).

**Do zrobienia przez Ciebie:** wpisać adres workspace'u DEV (np. `https://adb-1234567890123456.7.azuredatabricks.net`).
Typ maszyny: patrz „Poprawki po pierwszym deployu” — `Standard_D4ds_v5` okazał się niedostępny w regionie.

## Nowe: układ Volume'ów i skrypt startowy Unity Catalog

**Problem.** Konfiguracja (`conf/dev.yaml`, `conf/prod.yaml`) używa ścieżek
`/Volumes/smogcast_dev/raw/landing`, `…/raw/checkpoints`, `…/raw/models`. Ścieżka Volume'u to zawsze
`/Volumes/<katalog>/<schemat>/<volume>`, więc `raw` musi być **schematem z trzema Volume'ami** (`landing`,
`checkpoints`, `models`), a nie — jak pisała dokumentacja — jednym „volume `raw`”. Przy bootstrapie zgodnym z
dokumentacją pierwszy krok `ingest` nie znalazłby katalogu na pliki.

**Zmiana.** Nowy skrypt [`sql/bootstrap_uc.sql`](../sql/bootstrap_uc.sql): katalog `smogcast_dev`, schematy
`bronze`, `silver`, `gold`, `ops`, `raw` i trzy Volume'y w `raw`. Każda instrukcja ma `IF NOT EXISTS`, więc skrypt
można uruchamiać wielokrotnie. Dla PROD — to samo z `smogcast_prod` (w E9 uruchamiane z CI, nie ręcznie).
Poprawiony opis w `migracja-databricks.md` (P3).

**Uwaga Azure:** metastore workspace'u DEV (`metastore_azure_germanywestcentral`) **nie ma** domyślnej lokalizacji
(sprawdzone 2026-10-06), więc `CREATE CATALOG` ma `MANAGED LOCATION` w istniejącej lokalizacji zewnętrznej workspace'u
(`databricks_course_ws`, konto `dbstoragekwe4l73eqcgmo`). PROD będzie potrzebował własnej.

---

## Poprawki po pierwszym deployu na DEV (2026-10-06)

| Co wyszło | Poprawka |
|---|---|
| bundle wysyłał do `<bundle>/files` całe repozytorium (plany, dokumenty, `legacy/` radplume, Docker) — Joby z tego nie korzystają, instalują tylko wheele z `artifacts/` | `sync.paths: [sql]` w `databricks.yml` — do workspace'u idą tylko skrypty SQL; ponowny deploy usunął 165 zbędnych plików |
| wheel `smogcast-app` budowany i wysyłany, choć żaden Job go nie używa | usunięty z `artifacts`; panel/asystent ma być hostowany osobno (decyzja autora: Azure, odczyt gold przez SQL Warehouse — do ustalenia w E9) |
| pierwszy task padł przed startem kodu: `CLOUD_PROVIDER_RESOURCE_STOCKOUT` — brak wolnych `Standard_D4ds_v5` w Germany West Central | oba Joby na **`Standard_DC4as_v5`** (4 vCPU, 16 GB) — na tej rodzinie działa klaster kursu w tym workspace'ie, więc ma pojemność i limit rdzeni; Job batch też **jednomaszynowy** (zamiast 1 driver + 1 worker): połowa kosztu i limitu vCPU, lokalnie historia i tak liczy się na jednej maszynie |

**Pierwszy task na DEV — ✅ (2026-10-06):** `ingest_gios_registry` na `Standard_DC4as_v5`: 53 stacje, 354 stanowiska,
migawka zapisana w `/Volumes/smogcast_dev/raw/landing/gios_registry/`. Potwierdzone: wheele bez PySparka instalują się i
działają na runtime 17.3, punkt wejścia `--step … --env dev`, dostęp klastra do internetu (API GIOŚ), zapis na Volume,
tożsamość Joba (`single_user_name`). Czas: 13 min, z czego kod ~20 s (reszta: start klastra i instalacja wheeli).

**Pierwszy Job batch na DEV (2026-10-06):** ścieżka PM przeszła w całości (archiwum GIOŚ 4,9 min, bronze 6,1 min,
czyszczenie 1,8 min, średnie dobowe 0,9 min). Padło pobieranie archiwum pogody (`ingest_weather_history`): po ok. 35 z 100
plików Open-Meteo odpowiedziało treścią, która nie jest JSON-em, a `fetch` ponawiał tylko błędy HTTP 429/5xx →
`JSONDecodeError` i koniec taska. Lokalnie tego nie było widać — pliki pobierały się stopniowo i leżały w pamięci
podręcznej. **Poprawka** (`packages/ingest/.../weather.py`): odpowiedź, która nie jest JSON-em, to błąd przejściowy
(`NotJsonResponse`) ponawiany jak 429/5xx, w logu kod HTTP i początek treści; 6 prób z przerwami 5–80 s (łącznie ponad
minutowe okno limitu Open-Meteo); 2 nowe testy. Ponowienie: „repair run” tylko nieudanego taska i zależnych —
pobrane już pliki zostały na Volume, więc nie są pobierane drugi raz.

**Walidacja DEV zakończona (2026-10-06):** Job batch ✅ (po naprawie pogody i poprawkach powtarzalności GBT — wyniki
identyczne z lokalnymi), **Job na żywo ✅** — 11/11 tasków w 12,2 min, 20/20 prognoz na jutro w `gold.forecast_tomorrow`.
Szczegóły: `PLAN_SMOG.md`, dziennik 2026-10-06.

## Co musisz zrobić przed pierwszym deployem na DEV

1. **Databricks CLI** na laptopie (Windows): `winget install Databricks.DatabricksCLI`, potem
   `databricks auth login --host <adres DEV>` (logowanie w przeglądarce). W obrazie Dockera CLI nie ma.
2. **Adres DEV** w `databricks.yml` (target `dev`, `workspace.host`).
3. **Pakiet `build`** w Pythonie na laptopie (`pip install build`) — bundle buduje wheele poleceniem
   `python -m build --wheel` (sekcja `artifacts`).
4. **Bootstrap Unity Catalog:** wkleić `sql/bootstrap_uc.sql` do SQL Editora w workspace DEV i uruchomić (albo
   `databricks` CLI / notebook). Potrzebne uprawnienie `CREATE CATALOG` na metastore.

Service principal, OIDC z GitHubem, Key Vault i workspace PROD są potrzebne dopiero do CI/CD i PROD (E9).

## Próbny deploy na DEV — kolejność

```bash
databricks bundle validate -t dev          # składnia, zmienne, ścieżki wheeli
databricks bundle deploy -t dev            # buduje 7 wheeli kroków, wysyła je i tworzy 2 Joby ([dev <user>] …)
databricks bundle run -t dev smogcast_live --only ingest_gios_registry   # 1 task: sieć, Volume'y, uprawnienia
                                           # (--only w nowszych wersjach CLI; inaczej: w UI Joba „Run now” wybranego taska)
databricks bundle run -t dev smogcast_batch                               # historia → model (najdłuższe)
databricks bundle run -t dev smogcast_live                                # na żywo → gold.forecast_tomorrow
```

Porównanie z lokalnym wynikiem: `gold.acceptance` (te same werdykty K1–K4) i `gold.forecast_tomorrow` (20 wierszy).
Punkty, które mogą wyjść dopiero na klastrze (zapis modeli na Volume, `localCheckpoint`, odczyt id strumienia z
checkpointu, dostęp do internetu, dysk drivera przy archiwum xlsx): `migracja-databricks.md`, sekcja P2.

## Commity do odtworzenia

`build(core): pyspark and delta-spark only in the local extra`, `feat(bundle): job identity (single-user owner, PROD run_as)`,
`feat(bundle): Unity Catalog bootstrap script`, `docs: pre-deploy fixes`
