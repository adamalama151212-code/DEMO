# Przed pierwszym deployem na Azure Databricks — co poprawić w kodzie

> **Dla kogo:** autor projektu i agent, który będzie robił próbny deploy na DEV (etap E9 albo wcześniej).
> **Stan na:** 2026-10-01, po E6. Nic z tej listy nie było jeszcze uruchomione na Databricks — to wynik przeglądu kodu.
> Po próbnym deployu: odhacz punkty, dopisz, co wyszło w praktyce, i przenieś wnioski do `PLAN_SMOG.md` (E9).

## Werdykt w skrócie

Architektura jest gotowa na chmurę: środowisko wybiera konfiguracja (`conf/dev.yaml`, `conf/prod.yaml` →
Unity Catalog + Volumes), a nie kod; każdy krok to wheel z entry pointem `--step … --env …`; bundle i oba Joby istnieją.
**Do poprawy przed pierwszym deployem były 3 rzeczy (P1) — wprowadzone 2026-10-06, opis w
[`poprawki-przed-deployem.md`](poprawki-przed-deployem.md); do wpisania zostały tylko adres workspace'u i (dla PROD)
id service principala.** Reszta to punkty do sprawdzenia przy pierwszym uruchomieniu
(P2) i jednorazowe przygotowanie platformy (P3). Nowe funkcje wymagane przez kurs (pipeline deklaratywny Lakeflow,
RLS/CLS, dashboard, RAG) to nie migracja, tylko zakres E9 — lista na końcu.

---

## P1. Trzeba poprawić przed deployem

### P1.1 `pyspark` i `delta-spark` w zależnościach `smogcast-core` — ✅ 2026-10-06

**Gdzie:** [`packages/core/pyproject.toml`](../packages/core/pyproject.toml), linie 16 i 18.

**Problem:** każdy wheel kroku zależy od `smogcast-core`, a ten od `pyspark` i `delta-spark`. Przy instalacji wheela
na klastrze pip może doinstalować PySpark z PyPI obok PySparka z runtime'u — Databricks odradza instalowanie PySparka
jako biblioteki (może zepsuć sesję). `delta-spark` z PyPI może przesłonić wbudowany w runtime moduł `delta`, z którego
korzysta [`storage.py:19`](../packages/core/src/smogcast/core/storage.py#L19) (`from delta.tables import DeltaTable`).
Komentarz w `pyproject.toml` („pip keeps it because the version matches”) to nigdy niesprawdzone założenie.

**Poprawka:** przenieść oba pakiety do dodatku opcjonalnego, instalowanego tylko lokalnie:
```toml
[project.optional-dependencies]
local = ["pyspark>=4.0,<4.1", "delta-spark>=4.0,<4.1"]
```
i zmienić instalację na `-e "packages/core[local]"` w:
- [`Dockerfile`](../Dockerfile), linie 31–33,
- [`.github/workflows/ci.yml`](../.github/workflows/ci.yml), krok „Install all packages (editable)”.

Kod się nie zmienia: `configure_spark_with_delta_pip` jest już importowane tylko w ścieżce lokalnej
([`session.py:53`](../packages/core/src/smogcast/core/session.py#L53)).

**Sprawdzenie:** w czystym venv `pip install smogcast_core-*.whl` nie instaluje `pyspark`; na klastrze
`pip show pyspark` przed i po instalacji wheeli pokazuje tę samą wersję.

### P1.2 Tożsamość, z którą działają Joby (`SINGLE_USER` bez użytkownika) — ✅ 2026-10-06 (id principala do wpisania przed PROD)

**Gdzie:** [`resources/job_smogcast.yml:20`](../resources/job_smogcast.yml#L20),
[`resources/job_smogcast_live.yml:37`](../resources/job_smogcast_live.yml#L37).

**Problem:** klaster `data_security_mode: SINGLE_USER` musi wiedzieć, czyj to klaster (`single_user_name`).
Bez tego deploy albo uruchomienie się nie powiedzie. W CI/CD Joby powinny działać jako **service principal**, nie jako
osoba, która akurat wdrażała.

**Poprawka:** w `databricks.yml` dla każdego targetu `run_as: {service_principal_name: <application id>}`, a w klastrach
`single_user_name` = ten sam principal (albo nowsza nazwa trybu z dokumentacji — sprawdzić aktualną). Uprawnienia
principala: `USE CATALOG`, `CREATE SCHEMA`/`MODIFY`/`SELECT` na `smogcast_<env>`, `READ VOLUME`/`WRITE VOLUME` na `raw`.

### P1.3 Adresy workspace'ów — ⚠️ miejsca oznaczone, adresy wpisuje autor

**Gdzie:** [`databricks.yml`](../databricks.yml), linie 63 i 72 (`adb-XXXX…`, `adb-YYYY…` — TODO).

**Poprawka:** wpisać prawdziwe hosty DEV i PROD. Przy okazji sprawdzić w aktualnej dokumentacji wersję runtime
(`spark_version: 17.3.x-scala2.13` — czy to LTS ze Sparkiem 4.0) i dostępność `node_type_id: Standard_D4ds_v5`
w regionie i limicie rdzeni subskrypcji.

---

## P2. Powinno działać — sprawdzić przy pierwszym uruchomieniu na DEV

Każdy punkt jest poprawny „na papierze”, ale nigdy nie był uruchomiony na Databricks. Kolejność = kolejność, w jakiej
wyjdzie przy pierwszym przebiegu Jobów.

| # | Co | Gdzie | Ryzyko | Jak sprawdzić / plan B |
|---|---|---|---|---|
| P2.1 | **`cache()` i `localCheckpoint()`** | [`silver_clean/steps.py:120,133,135`](../packages/silver_clean/src/smogcast/silver_clean/steps.py#L120), [`model/steps.py:30,79,131`](../packages/model/src/smogcast/model/steps.py#L30), [`model/train.py:69-70`](../packages/model/src/smogcast/model/train.py#L69) | działają na klastrach Jobów (tak jest skonfigurowane), **nie działają na serverless** | zostać przy klastrach Jobów. Przejście na serverless wymaga zamiany `localCheckpoint()` na zapis do tabeli tymczasowej — **nie usuwać** go bez zamiennika: zamraża klasyfikację przed MERGE (bez tego metryki liczyły same duplikaty, E6d) |
| P2.2 | **Plikowe API Pythona na Volumes** (`Path`, `glob`, `open`, `shutil.rmtree`) | landing: [`ingest/steps.py:92`](../packages/ingest/src/smogcast/ingest/steps.py#L92), [`ingest/gios_live.py:35-60`](../packages/ingest/src/smogcast/ingest/gios_live.py#L35), [`bronze/steps.py:73,97`](../packages/bronze/src/smogcast/bronze/steps.py#L73); resety: [`silver_clean/steps.py:208,221`](../packages/silver_clean/src/smogcast/silver_clean/steps.py#L208) | `/Volumes/...` działa jak zwykły system plików na klastrach z UC (tryb dedicated / single user). Możliwe różnice: wolniejszy `glob`, uprawnienia do kasowania | pierwszy przebieg `ingest gios-registry` + `gios-live` + `silver-clean live-reset` |
| P2.3 | **Odczyt id strumienia z checkpointu** | [`silver_clean/steps.py:112`](../packages/silver_clean/src/smogcast/silver_clean/steps.py#L112) (`<checkpoint>/metadata`) | zakłada, że Spark zapisuje plik `metadata` w katalogu checkpointu przed pierwszą mikro-paczką (tak jest w Spark 4.0 lokalnie). Bez tego `txnAppId` nie wie o resecie — patrz dziennik E6 | `pm-stream`, potem `live-reset`, `pm-stream` — liczby w `ops.pm_quarantine` muszą być takie same |
| P2.4 | **Zapis i odczyt modeli MLlib na Volume** | [`model/steps.py:45,48,72,139`](../packages/model/src/smogcast/model/steps.py#L45) → `models_root` z `conf/dev.yaml` | `PipelineModel.save('/Volumes/...')` powinien działać na UC; jeśli nie — zapis przez MLflow (rejestr modeli w UC, i tak rozważany w E9) | `model train` → `model forecast` |
| P2.5 | **Archiwum xlsx na driverze** | [`bronze/pm_archive.py:111`](../packages/bronze/src/smogcast/bronze/pm_archive.py#L111) (zip → `tempfile` → openpyxl → CSV gzip w landing) | lokalnie ~2,5 min; na Volumes zapis wolniejszy. Dysk tymczasowy drivera musi pomieścić rozpakowany rok (~kilkaset MB) | `bronze pm-hourly` na DEV; w razie potrzeby większy dysk drivera |
| P2.6 | **Dostęp klastra do internetu** | `ingest gios-archive`, `gios-live`, `gios-registry`, `weather-*` | workspace z VNet injection i ograniczonym ruchem wychodzącym zablokuje API GIOŚ i Open-Meteo | `ingest gios-registry` na DEV jako pierwszy test sieci |
| P2.7 | **Ta sama wersja `0.1.0` przy każdym deployu** | wszystkie `pyproject.toml`, `VERSION` | na klastrach Jobów (nowy klaster na każde uruchomienie) nie przeszkadza; na klastrze interaktywnym stary wheel zostaje w pamięci podręcznej | wersja z numerem builda (`0.1.0+<sha>`) albo `dynamic_version` artefaktów w nowszym Databricks CLI — sprawdzić w dokumentacji |
| P2.8 | **Strefa czasowa procesu** | [`session.py:30`](../packages/core/src/smogcast/core/session.py#L30) (`TZ=UTC`) | na klastrze ustawiane w procesie Pythona drivera — powinno wystarczyć; sesja Sparka i tak ma `UTC` | wiersz w `gold.forecast_tomorrow`: `issue_day` = dziś w CET |

---

## P3. Jednorazowe przygotowanie platformy (poza kodem)

Specyfikacja kursu: „nothing built by hand in PROD”. Dlatego wszystko poniżej najlepiej jako skrypt (SQL + CLI)
uruchamiany przez CI, a ręcznie tylko to, czego inaczej się nie da (subskrypcja, workspace'y).

- [ ] 2 workspace'y Azure Databricks (DEV, PROD) z Unity Catalog (metastore przypisany do obu).
- [ ] Katalogi `smogcast_dev`, `smogcast_prod`; schematy `bronze`, `silver`, `gold`, `ops` i **schemat `raw` z Volume'ami
      `landing`, `checkpoints`, `models`** — skrypt [`sql/bootstrap_uc.sql`](../sql/bootstrap_uc.sql) (poprawka 2026-10-06:
      wcześniej błędnie „volume `raw`”) (ścieżki z [`conf/dev.yaml`](../packages/core/src/smogcast/core/conf/dev.yaml) i `prod.yaml`).
      [`storage._ensure_schema`](../packages/core/src/smogcast/core/storage.py#L124) tworzy schematy na DEV jako zabezpieczenie,
      ale na PROD mają być z bootstrapu.
- [ ] **Service principal** + **federacja OIDC z GitHub Actions** (bez sekretów w repo); uprawnienia jak w P1.2.
- [ ] Secret scope oparty o Azure Key Vault (wymóg kursu, nawet jeśli API GIOŚ klucza nie wymaga — np. token do
      Foundation Model API w E9).
- [ ] Grupy pod RLS/CLS (województwo = `jurisdiction_code`, decyzja D12).

## P4. CI/CD — co dopisać

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) dziś: lint, testy, budowa wheeli. Docelowo:

```
pull_request        → lint + testy + budowa wheeli + `databricks bundle validate -t dev`
push na main        → `databricks bundle deploy -t dev`   (bundle przebudowuje wheele i podmienia je w Jobach)
tag / zatwierdzenie → `databricks bundle deploy -t prod`  (environment z wymaganym zatwierdzeniem w GitHub)
```
Joby używają klastrów tworzonych na każde uruchomienie, więc po deployu kolejne uruchomienie bierze już nowe wheele.

## P5. Aplikacja (asystent + panel) na Databricks

Aplikacja (`packages/app`) jest napisana tak, żeby w chmurze zmieniać tylko konfigurację tam, gdzie się da:

| Element | Lokalnie | Na Databricks | Co zmienić |
|---|---|---|---|
| Hosting UI | `docker compose up app` (Streamlit, port 8501) | **Databricks Apps** (Streamlit jest wspierany natywnie) | plik `app.yaml` Databricks Apps z komendą `streamlit run …/ui/streamlit_app.py`, zasób aplikacji w bundle'u |
| Dostęp do tabel gold | Spark lokalny (`GoldReader` → `delta.\`/ścieżka\``) | **SQL Warehouse** przez `databricks-sql-connector` — w Databricks Apps nie ma sesji Sparka | drugi backend w [`app/gold.py`](../packages/app/src/smogcast/app/gold.py) (ta sama metoda `query`, te same guardrails; zapytania z RLS wykonywane z tożsamością użytkownika) |
| Model językowy | LM Studio na hoście (`SMOGCAST_LLM_BASE_URL`) | Foundation Model API **albo** własny destylat | tylko zmienne `SMOGCAST_LLM_*` (endpointy serving są zgodne z API OpenAI); token z secret scope / Key Vault |
| Własny destylat | LM Studio (GGUF) | (a) **Model Serving** z GPU — model zarejestrowany w Unity Catalog (MLflow), jeśli architektura jest wspierana przez provisioned throughput, inaczej custom model (drogo, GPU 24/7 lub scale-to-zero z zimnym startem); (b) **własny host** (np. Azure Container Apps / VM z GPU z vLLM lub llama.cpp) podpięty jako *external model* endpoint w Databricks | decyzja kosztowa — do opisania w write-upie |
| Indeks RAG | plik numpy w `data/models/rag_index` + `sentence-transformers` | **Vector Search** (indeks na tabeli fragmentów, embeddingi z endpointu) | implementacja `Retriever` dla Vector Search; dokumenty i fragmenty w Volume / tabeli |
| Embeddingi | `paraphrase-multilingual-MiniLM-L12-v2` (lokalnie, CPU) | endpoint embeddingów — **musi być wielojęzyczny** (pytania po polsku, część dokumentów po angielsku) | sprawdzić dostępne modele; jeśli tylko angielski — tłumaczyć pytanie przed wyszukiwaniem |
| `docs/rag/*.md` | czytane z repozytorium (`app.rag.project_docs_dir`) | nie ma ich w wheelu | bundle (`sync`) lub krok kopiujący do Volume |

## Nie migracja, tylko zakres E9 (dla porządku)

- **Pipeline deklaratywny Lakeflow** (wymóg kursu): dziś `silver-clean pm-stream` to Structured Streaming z
  `foreachBatch` w zwykłym Jobie. Pipeline deklaratywny to nowy kod (Auto Loader + expectations), który użyje funkcji
  z wheela `smogcast-silver-clean`; obecny krok może zostać w Jobie jako alternatywa.
- RLS/CLS (`sql/governance.sql`), dashboard AI/BI na tabelach gold, aplikacja AI (text-to-SQL + RAG) na tych samych
  guardrails ([`app/guardrails.py`](../packages/app/src/smogcast/app/guardrails.py) — już obsługuje nazwy UC
  `katalog.gold.tabela`).

## Proponowana kolejność próbnego deployu na DEV

1. P1.1–P1.3 (pół dnia), testy lokalnie.
2. `databricks bundle validate -t dev`, `databricks bundle deploy -t dev` z laptopa (DEV pozwala; PROD tylko z CI).
3. Ręcznie jeden task: `ingest gios-registry` — test sieci, Volumes, uprawnień (P2.2, P2.6).
4. Cały Job batch (historia → model, P2.1, P2.4, P2.5), potem Job live (P2.3).
5. Porównać z lokalnym: `gold.acceptance` (te same werdykty K1–K4), `gold.forecast_tomorrow` (20 wierszy).
6. Dopiero wtedy deploy z CI (P4) i PROD.
