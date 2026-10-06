# HANDOFF — stan pracy na 2026-10-06, E8 prawie zamknięty, przed E9 (dla kolejnego agenta)

> **Autor pisze po polsku i tak mu odpowiadaj.** Kod, komentarze i logi po angielsku; dokumentacja robocza po polsku;
> **aplikacja (interfejs, odpowiedzi asystenta, baza wiedzy RAG) po angielsku.**
> Demo Day kursu: ok. **2026-10-19** (13 dni od tej sesji). Najwięcej wymagań kursu jest w E9 — pilnuj czasu.

## 0. Repozytoria i co przeczytać przed pracą

| Repo | Lokalnie | GitHub | Rola |
|---|---|---|---|
| **smogcast** (główne) | `C:\Users\Leszek\DEMO RADPLUME\DEMO` | https://github.com/adamalama151212-code/DEMO (gałąź `feat/databricks-local-demo`) | kod potoku (wheele `packages/*`), bundle Databricks, aplikacja. **Repo robocze PoC** — autor przeniesie kod do czystego repo i sam odtworzy historię commitów |
| **smog-cast-terraform** | `C:\Users\Leszek\smog-cast-terraform\smog-cast-terraform` | https://github.com/adamalama151212-code/smog-cast-terraform (puste, bez commitów) | infrastruktura Azure/Databricks (Terraform). Kod gotowy, `validate` OK, **niczego nie uruchomiono** |
| wzór stylu (tylko do odczytu) | `C:\Users\Leszek\remote-ally-terraform\remote-ally-terraform` | — | repo Terraform z poprzedniej pracy autora; na nim wzorowana struktura `smog-cast-terraform` (workspace = środowisko, `-var-file`, `CLAUDE.md` z zasadami). **Nie zmieniać, nie kopiować wartości** |

**Przeczytaj w tej kolejności:**
1. ten plik,
2. [`CLAUDE.md`](CLAUDE.md) — zasady pracy w repo smogcast,
3. [`PLAN_SMOG.md`](PLAN_SMOG.md) — plan, decyzje D1–D17, kryteria K1–K4, etapy (**E5 ramka „Aktualizacja”, E7 uwagi
   autora, E8, E9 z „Ustaleniami autora” i „Pytaniami do prowadzącego”**) i dziennik (wpisy z 2026-10-06),
4. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — diagramy, tabele, wyniki modelu, aplikacja,
5. [`docs/poprawki-przed-deployem.md`](docs/poprawki-przed-deployem.md) — co zmieniono przed deployem i jak przebiegła
   walidacja na Databricks DEV,
6. [`docs/migracja-databricks.md`](docs/migracja-databricks.md) — punkty P2–P5 do E9,
7. `C:\Users\Leszek\smog-cast-terraform\smog-cast-terraform\README.md` i `…\CLAUDE.md` — moduły, zasady pracy z
   Terraformem,
8. [`docs/HOWTOREAD.md`](docs/HOWTOREAD.md) (mapa kodu, jak czytać tabele gold) i
   [`docs/przygotowanie-danych.md`](docs/przygotowanie-danych.md) (źródła, formaty, czas, pułapki),
9. w razie potrzeby: [`docs/demo-jakosc-danych.md`](docs/demo-jakosc-danych.md) (scenariusz demo czyszczenia),
   [`docs/ulepszenia-pm10.md`](docs/ulepszenia-pm10.md) (materiał na demo), [`docs/rag-dokumenty.md`](docs/rag-dokumenty.md)
   (baza wiedzy asystenta).

**Zasady autora (ważne):** nie commitujesz — autor sam robi commity w obu repo; przy każdym etapie aktualizujesz listę
„Commity do odtworzenia” w `PLAN_SMOG.md` (Terraform: w jego `README.md`). W repo Terraforma **tylko generujesz kod
i komendy** — `init/plan/apply/import` uruchamia autor. Długie komendy (build, pełne testy, przebiegi potoku, Joby)
uruchamiasz w tle. Decyzje, które mogłyby wyglądać na „dopasowanie do wyników” (zmiana modelu, progu, kryteriów
po zobaczeniu roku testowego), zawsze konsultuj — autor świadomie z nich rezygnuje.

## 1. Co przejmujesz — stan na koniec sesji

| Etap | Stan | Uwagi |
|---|---|---|
| E0–E6 | ✅ | historia GIOŚ 2021–2025 (11,4 mln godzin), pogoda, silver/cechy bez wycieku, model + backtest, strumień `live` + `fault_replay`, CDC/SCD2, prognoza na jutro |
| E5 wyniki | ✅ (zaktualizowane 2026-10-06) | po **poprawce powtarzalności GBT** (jedna uporządkowana partycja + zaokrąglenie cech do 6 miejsc) — **identyczne lokalnie i na Databricks**. PM2.5 GBT: spełnia K1–K3. PM10 GBT: **nie spełnia K4a** (POD 44%), K3 8,5 p.p. ✅ ale **kruche** (wahało się 8,5–21 p.p. między wersjami różniącymi się szumem). E5b **porzucony** (decyzja autora) |
| E7 panel + asystent | ✅ | + uwagi autora U1–U4 (zakładka Info, opis kalibracji, tabela K1–K4) |
| **E8** | 🚧 | zrobione: usunięty `legacy/`, generator syntetyczny, test integracyjny (6/6), scenariusz demo DQ, **README, HOWTOREAD, przygotowanie-danych, ARCHITECTURE**, poprawione `ci.yml` (symulacja w czystym kontenerze: 137/137). **Zostało: potwierdzić zielony przebieg na GitHub Actions** (push autora) |
| E9 Databricks | 🚧 | **próbny deploy na DEV ✅**: bundle, Job batch i Job na żywo działają (20/20 prognoz). Reszta wymagań kursu — niżej |
| E10 demo | ☐ | write-up, scenariusz całości, próba |

Testy na koniec sesji: **137/137** (w tym test integracyjny 6/6), ruff czysto — 2026-10-06, także w czystym kontenerze jak w CI.

## 2. Co zostało do zrobienia (proponowana kolejność)

### 2.1 Dokończ E8 — tylko zielone CI na GitHubie
- `ci.yml` (2026-10-06): PR do `main` + push na `main` + `workflow_dispatch` (push każdej gałęzi dublował przebiegi z PR); kroki: ruff → „Unit tests” (131)
  → „Integration test” (6) → budowa 9 wheeli; jawne `cache-dependency-path` (brak `requirements.txt`).
- Symulacja w czystym kontenerze `python:3.11` + Java 17 z plikami widocznymi dla gita: 137/137 w 4,5 min.
- **W lokalnym repo nic ze smogcast nie jest zacommitowane** (ostatni commit = radplume) — CI zobaczy kod dopiero po
  commitach autora. Poproś o link do przebiegu; jeśli coś padnie — log kroku.
- Po zielonym przebiegu: odhacz checkbox E8 w `PLAN_SMOG.md`, ✅ przy E8 tu i w `docs/ARCHITECTURE.md` (nagłówek, sekcja 9).

### 2.2 Nowe konto Azure (pilne — trial autora kończy się ok. 2026-10-10)
- Obecny DEV (`adb-7405617820175624`, Germany West Central, katalog `smogcast_dev` z danymi) **zniknie** z końcem
  triala. Autor zakłada nowe konto z kredytami; tam powstanie DEV **od zera** i tam będzie stan Terraforma dla DEV i PROD.
  Wspomniane autorowi: alternatywa = upgrade triala do pay-as-you-go (zasoby zostają) — decyzja autora.
- **Na obecnym koncie nie uruchamiaj `terraform apply`.** Do decyzji autora: czy Terraform ma tworzyć też sam workspace
  DEV na nowym koncie (bezpieczne przy tworzeniu od zera; teraz workspace jest tylko wskazywany).
- Po nowym koncie: zaktualizuj `databricks.yml` (host DEV, `node_type` — sprawdź dostępność maszyn w regionie; na
  starym koncie `Standard_D4ds_v5` = stockout, działa `Standard_DC4as_v5`), `environment/dev/.../terraform.tfvars`
  (subskrypcja, workspace, account id, lokalizacja katalogu), potem bootstrap stanu → `terraform plan` (autor) →
  `bundle deploy` → Job batch → Job na żywo.

### 2.3 E9 — wymagania kursu (`final-project-spec.md`)
- **Pipeline deklaratywny Lakeflow** (ścieżka B: Auto Loader + **expectations** — dobre na demo jakości danych).
- **Job demo usterek** `smogcast-fault-demo` w bundlu (te same 6 kroków co `run-fault-demo`) — ustalone z autorem na E9.
- **RLS/CLS** (`sql/governance.sql`: filtr wierszy po `jurisdiction_code`, maska np. `source_file`) na grupach z modułu
  `governance` Terraforma.
- **Sekrety w Key Vault** — na PROD prawdopodobnie istniejący vault kursu (moduł `secrets` do przerobienia na wariant
  „istniejący vault” — czekamy na odpowiedź prowadzącego).
- **CI/CD DEV → PROD** (GitHub Actions + OIDC, service principal z modułu `identity`).
- **Dashboard AI/BI** na gold (m.in. historia prognoz w ciągu dnia).
- **Oficjalna prognoza o 12:00 + `gold.forecast_history`** (wstępnie zaakceptowane przez autora): krok `model forecast`
  dopisuje każde przeliczenie do historii, a `gold.forecast_tomorrow` zapisuje raz — pierwszy przebieg po 12:00 CET;
  panel/asystent przed 12:00 pokazują prognozę wstępną z etykietą. Popraw też komentarz w `resources/job_smogcast_live.yml`
  („po 12:00 się nie zmienia” — nieprawda: prognoza pogody live się odświeża).
- **Aplikacja:** autor ma **zastrzeżenia do tego, co aplikacja pokazuje** — zapytaj go, zanim zaczniesz hosting.
  Hosting docelowo na **Azure** (nie Databricks Apps), odczyt gold przez SQL Warehouse; wheel `smogcast-app` wyjęty z bundla.
- Opcja kosztowa (do write-upu albo jeśli zostanie czas): odpytywanie API GIOŚ przez **Azure Function → pliki** zamiast
  Joba Databricks (start klastra ~7–8 min na ~20 s pracy). Event Hubs rozważony i odradzony (API pull co godzinę).

### 2.4 Pytania do prowadzącego (autor je zada; lista też w `PLAN_SMOG.md`, E9)
nazwa Key Vaulta kursu i czy jest secret scope; czy można tworzyć service principal w Entra ID kursu; administrator
konta Databricks (grupy RLS/CLS); region, nazwa katalogu, lokalizacja; dostęp CI (OIDC). Stan PROD w magazynie na koncie
autora wymaga `tenant_id` per środowisko w providerach Terraforma.

## 3. Najważniejsze ustalenia (nie odkrywaj ich drugi raz)

**Dane i czas**
- Archiwum GIOŚ = koniec godziny w **CET**; API GIOŚ = koniec godziny w **czasie lokalnym** (Europe/Warsaw). Konwersje
  tylko w `smogcast.core.timeutil`. Historia (Job batch) pochodzi z **archiwum xlsx**, nie z API (API trzyma ~3 doby).
- Pogoda: `short_range` (trening), `previous-runs _previous_day1` = prawdziwa prognoza z dnia wcześniej, od 2024-01-19
  (walidacja/test). Prognoza na żywo bierze **najnowszą** prognozę pogody.
- Nagłówki plików GIOŚ (polskie etykiety) są kontraktem w `smogcast.core.schema` — **nie tłumaczyć** (dopasowanie
  znak po znaku); autor o to pytał.

**Strumień i jakość danych**
- API zwraca ~3 doby → duplikaty to norma; „spóźniony” = należny już przy poprzednim odpytaniu. `txnAppId` z id
  zapytania strumieniowego (po resecie). `localCheckpoint()` przed zapisami — nie usuwać.
- Demo usterek (`fault_replay`): **prawdziwe** pomiary Kraków 17–21.01.2025 + usterki z `conf/fault_injection.yaml`;
  osobny tor, nigdy do danych modelu (D17). Scenariusz: `docs/demo-jakosc-danych.md`.

**Model**
- Reguła wyboru (najniższy Brier walidacyjny), siatki i K1–K4 zamrożone; nie stroić na 2025. Każde zanieczyszczenie
  ma własny model (może być mieszanka GBT/logistyczna).
- **Powtarzalność:** `train.fit_input` (jedna partycja, stały porządek) + `prepare.ROUND_DECIMALS = 6` — bez tego GBT
  dawał inne drzewa w Dockerze i na klastrze (szum zmiennoprzecinkowy). Testy: `test_gbt_is_the_same_whatever_the_partitioning`,
  `test_float_noise_in_inputs_does_not_reach_the_model`.

**Databricks (DEV, stare konto)**
- Bundle: `sync.paths: [sql]` (bez reszty repo), bez artefaktu `app`, `node_type` jako zmienna per target, Joby
  jednomaszynowe, `single_user_name: ${workspace.current_user.userName}`, PROD z `run_as` (service principal, TODO).
- Metastore DEV bez domyślnej lokalizacji → katalog z `MANAGED LOCATION` (lokalizacja zewnętrzna workspace'u).
  Układ: schemat `raw` z Volume'ami `landing`, `checkpoints`, `models` (`sql/bootstrap_uc.sql`; docelowo moduł Terraforma).
- Pobieranie Open-Meteo: odpowiedź nie-JSON jest ponawiana (`NotJsonResponse`, 6 prób 5–80 s).

**Tryb offline / testy integracyjne (E8)**
- `smogcast.ingest.synthetic` zapisuje pliki **w formatach źródeł** (zip+xlsx GIOŚ, metadane, rejestr, paczki API,
  JSON Open-Meteo), wartości deterministyczne; `conf/synthetic.yaml`; `OFFLINE_OVERRIDES` (2 miasta, małe siatki,
  katalog `data-offline/`). Zakres ustalony z autorem: **tylko dla CI/testów** (bez dopracowywania komendy dla ludzi).
- `packages/cli/tests/test_integration.py` — cały potok w ~3 min; `pytest -m "not integration"` pomija.

**Asystent i panel** — bez zmian względem E7 (liczby tylko z gold przez guardrails, RAG po angielsku, LM Studio
`qwen2.5-7b` na hoście, panel na `127.0.0.1:8501`). Indeks RAG przebudowany 2026-10-06 (448 fragmentów).

## 4. Interfejs — gdzie co jest

Kod: `packages/app/src/smogcast/app/` — `ui/streamlit_app.py` (nawigacja: **Info** (pierwsza), Tomorrow (domyślna),
City, Model, Data quality, Assistant), `ui/views.py`, `ui/charts.py`, `ui/theme.py`, `forecast.py`, `assistant.py`,
`rag.py`, `llm.py`, `gold.py`. Zasady wyglądu: status nigdy samym kolorem, każdy widok pokazuje swój SQL, liczby
z gold (nie na sztywno w tekście). Zmiany kolorów/wykresów — skill `dataviz`.

## 5. Przydatne komendy

```bash
docker compose run --rm smogcast bash -c "ruff check packages conftest.py && pytest -q -p no:logging"   # pełne testy (~6 min, w tle)
docker compose run --rm smogcast pytest -q -p no:logging -m integration     # sam test integracyjny (~3 min)
docker compose run --rm smogcast smogcast --offline run-batch               # historia na danych syntetycznych (~2,5 min)
docker compose run --rm smogcast smogcast run-batch | run-live | run-fault-demo   # prawdziwe dane
docker compose run --rm smogcast smogcast show gold forecast_tomorrow -n 30
docker compose up -d app ; docker compose restart app                       # panel http://localhost:8501
docker compose run --rm smogcast smogcast-app build-index                   # po zmianie docs/rag/*.md
```

Databricks (z hosta, PowerShell; CLI `databricks` v1.19, profil autora — na nowym koncie trzeba zalogować na nowo):
```powershell
$env:Path = [Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [Environment]::GetEnvironmentVariable("Path","User")
databricks bundle validate -t dev ; databricks bundle deploy -t dev
databricks bundle run -t dev smogcast_batch            # ~1 h (trening GBT ~45 min); --only <task,...> dla części
databricks bundle run -t dev smogcast_live             # ~12 min
databricks jobs repair-run <run_id> --rerun-all-failed-tasks --rerun-dependent-tasks --no-wait
```

Test bezgłowy widoków (bez przeglądarki; każdy widok osobno; plik tymczasowy w repo, uruchom w kontenerze, usuń):
```python
from streamlit.testing.v1 import AppTest
for view in ("view_info", "view_tomorrow", "view_city", "view_model", "view_data_quality", "view_chat"):
    at = AppTest.from_string(f"from smogcast.app.ui import views\nviews.setup()\nviews.{view}()\n", default_timeout=300)
    at.run(); print(view, [e.value for e in at.exception])
```

Dane lokalne: `data/` (prawdziwe), `data-offline/` (syntetyczne) — oba poza gitem.
