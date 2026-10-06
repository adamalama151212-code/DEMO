# Plan zmiany tematu: radplume → prognoza przekroczeń PM10/PM2.5

> **Dla kogo:** dla autora projektu i dla każdego agenta AI, który przejmie pracę.
> **Zasada:** ten plik jest jedynym źródłem prawdy o stanie zmiany tematu. Po każdym
> skończonym etapie zaktualizuj sekcję [10. Dziennik postępu](#10-dziennik-postępu)
> i checkboxy w [sekcji 6](#6-etapy). Nie zaczynaj etapu N+1, dopóki etap N nie spełnia
> kryteriów akceptacji.

Data utworzenia: 2026-10-01. Demo Day: ok. 2026-10-19 (2,5 tygodnia od utworzenia).

---

## Spis treści

1. [Dlaczego zmieniamy temat](#1-dlaczego-zmieniamy-temat)
2. [Nowe pytanie i definicje](#2-nowe-pytanie-i-definicje)
3. [Decyzje (z uzasadnieniem)](#3-decyzje)
4. [Źródła danych — zweryfikowane](#4-źródła-danych--zweryfikowane-2026-10-01)
5. [Architektura docelowa: tabele](#5-architektura-docelowa-tabele)
6. [Etapy](#6-etapy)
7. [Mapa plików: co zostaje, co znika, co się zmienia](#7-mapa-plików)
8. [Harmonogram](#8-harmonogram)
9. [Ryzyka i plan B](#9-ryzyka-i-plan-b)
10. [Dziennik postępu](#10-dziennik-postępu)

---

## 1. Dlaczego zmieniamy temat

Projekt radplume (dyspersja radionuklidów) został zwalidowany na pomiarach gleby po
Fukushimie (2141 punktów, NRA EMDB). Po poprawkach (uczciwe liczenie pudeł, okno 282 h,
zespół pogody, opad ze stacji AMeDAS) wynik: **FAC2 13%, FAC5 30%, pokrycie P5–P95 71%**.
Główny pas skażenia (NW) niedoszacowany ~100×. Przyczyna: założenie jednorodnej pogody
nad terenem górzystym. Kluczowy problem: **nie da się sprawdzić modelu tam, gdzie miał
odpowiadać (Polska)** — brak awarii z pomiarami na płaskim terenie.

Nowy temat ma właściwość, której brakowało: **model sprawdzamy na tysiącach dni
historycznych w tych samych miastach, dla których przewidujemy.**

Stan radplume zostaje zachowany w git (tag `radplume-final`, etap E0) — można do niego
wrócić i użyć wniosków w write-upie („dlaczego zmieniliśmy temat” to dobra historia
o uczciwej walidacji).

## 2. Nowe pytanie i definicje

> **„Czy jutro w mieście Y zostanie przekroczona norma PM10 / PM2.5 i na ile to pewne?”**

| Pojęcie | Definicja w projekcie | Źródło |
|---|---|---|
| doba | doba kalendarzowa w czasie **CET** (UTC+1, bez zmiany na letni) — tak liczy GIOŚ | archiwum GIOŚ podaje dane w CET |
| średnia dobowa stacji | średnia z wyników 1-godzinnych; **ważna tylko przy ≥ 18 z 24 godzin** (75%) | kryterium kompletności UE (dyrektywa 2008/50/WE) |
| przekroczenie PM10 | średnia dobowa > **50 µg/m³** | poziom dopuszczalny (PL/UE), API GIOŚ `levels/getPermissible`; dopuszczalne 35 dni/rok |
| przekroczenie PM2.5 | średnia dobowa > **25 µg/m³** (konfigurowalne) | brak dobowej normy PM2.5 w obecnym prawie PL; 25 µg/m³ = dobowa wartość dopuszczalna z nowej dyrektywy UE 2024/2881 (od 2030); alternatywa: 15 µg/m³ (WHO 2021) |
| przekroczenie w mieście | **co najmniej jedna** stacja miasta ma ważną średnią dobową powyżej progu | tak raportuje GIOŚ (przekroczenie na stanowisku); decyzja D4 |
| „na ile pewne” | **prawdopodobieństwo** z modelu (0–100%), sprawdzone pod kątem **kalibracji**: z dni z prognozą „70%” przekroczenie powinno wystąpić w ~70% | sekcja E5 |
| „jutro” | doba D+1, prognoza wydawana w dobie D o 12:00 CET (dane do 11:00 + prognoza pogody na D+1) | decyzja D6 |

**Progi w konfiguracji** (`conf/thresholds.yaml`), nie w kodzie — z podanym źródłem.

## 2a. Kiedy uznajemy model za wiarygodny (zamrożone 2026-10-01, przed treningiem)

Backtest na roku testowym (2025), którego model nigdy nie widział; pogoda z prawdziwych prognoz
dzień wcześniej. **Wszystkie** kryteria muszą być spełnione (`conf/model.yaml`, `model.acceptance`):

| # | Kryterium | Po ludzku |
|---|---|---|
| K1 | Brier skill score > 0 względem **klimatologii** | prawdopodobieństwa lepsze niż średnia z kalendarza |
| K2 | Brier score niższy niż **persystencja** | model wnosi coś ponad „jutro jak dziś” |
| K3 | Kalibracja: w każdym przedziale prognoz z ≥ 30 dniami \|prognoza − częstość\| ≤ 15 p.p. | „70%” znaczy ok. 70% |
| K4 | PM10: POD ≥ 60% i FAR ≤ 50% (ostrzeżenie przy P ≥ 0,5) | łapie większość dni smogowych, nie alarmuje fałszywie co drugi raz |

Progi to rozsądne wartości startowe, nie norma branżowa. **Nie zmieniać po zobaczeniu wyników.**
Niespełnienie któregoś kryterium raportujemy uczciwie (write-up, dashboard).

## 3. Decyzje

Decyzje D1, D2, D14–D16 potwierdził autor (2026-10-01). Pozostałe to wartości domyślne
wybrane z uzasadnieniem — zmiana dotyczy tylko konfiguracji.

| ID | Decyzja | Uzasadnienie |
|---|---|---|
| D1 | Nazwa projektu: **`smogcast`** (przestrzeń nazw Pythona `smogcast.*`, lokalne CLI `smogcast …`) ✅ | potwierdzone przez autora |
| D2 | Miasta: **10 największych miast Polski** — Warszawa, Kraków, Wrocław, Łódź, Poznań, Gdańsk, Szczecin, Bydgoszcz, Lublin, Białystok ✅ | potwierdzone; sprawdzone w API 2026-10-01: każde ma 3–14 stanowisk PM10 i 3–10 PM2.5, każde w innym województwie (dobre dla RLS); mieszanka miast ze smogiem (Kraków, Łódź) i czystszych (Gdańsk, Szczecin); lista w `conf/cities.yaml` |
| D3 | Lata danych: **2021–2025** (archiwum GIOŚ, `run.archive_years`) + bieżące dane z API | 5 sezonów grzewczych; archiwum prognoz pogody Open-Meteo zaczyna się w 2021, więc wcześniejsze lata PM nie mają pary z prognozą (pierwsza doba 2021 traci cechę „D-1” — pomijamy ją) |
| D4 | Przekroczenie w mieście = max po stacjach miasta | zgodne z raportowaniem; mediana po stacjach ukryłaby przekroczenia lokalne |
| D5 | Model: **regresja logistyczna** (Spark MLlib) jako główny + **GBT** (gradient boosted trees) jako porównanie | logistyczna daje dobrze skalibrowane prawdopodobieństwa i jest łatwa do wyjaśnienia na demo; wszystko w Sparku (tak jak dotąd) |
| D6 | Cechy (features) tylko z informacji **dostępnej w chwili prognozy** (D, 12:00 CET): średnia PM z D-1, średnia z D 00–11, prognoza pogody na D+1, dzień tygodnia, sezon grzewczy | brak „wycieku” przyszłości — inaczej backtest jest zawyżony |
| D7 | Pogoda „na jutro” z **archiwów prognoz**, nigdy z pogody, która faktycznie wystąpiła (ERA5). **Dwa źródła** (zmiana 2026-10-01, `conf/weather.yaml`): `short_range` (Historical Forecast API, od 2021, świeże prognozy na kilka godzin — trochę lepsze niż prawdziwe „na jutro”) do **treningu**; `day_ahead` (Previous Runs API, `_previous_day1`, **od 2024-01-19 12:00 UTC**) do **walidacji i testu**. Dni testu bez `day_ahead` wyłączone z oceny | ocena odpowiada jakości prognoz, jaką model miałby naprawdę; ewentualne zawyżenie dotyczy tylko treningu, nie wyniku |
| D8 | Podział czasowy: **trening 2021–2023, walidacja 2024, test 2025** | nigdy losowo (dni sąsiednie są do siebie podobne → wyciek); test używany **raz**, na koniec |
| D9 | Model musi pokonać **dwie proste prognozy odniesienia**: (a) *persystencja* — „jutro jak dziś”, (b) *klimatologia* — częstość przekroczeń w danym miesiącu w latach treningowych | bez tego nie wiadomo, czy model coś wnosi; to odpowiedź na „czy model ma wartość” |
| D10 | Strumień ma **dwa źródła**: (a) **prawdziwe API GIOŚ** (odpytywanie co godzinę), (b) **odtwarzanie prawdziwych pomiarów z wstrzykiwaniem usterek** (*fault injection replay*) — następca symulatora czujników ✅ | autor chce zachować logikę psucia danych: czyszczenie musi działać na popsutych danych w przewidywalny sposób (testy, demo). Wartości są prawdziwe (archiwum/API), psujemy tylko „transport” i pojedyncze odczyty |
| D11 | CDC: rejestr stacji/stanowisk GIOŚ — porównanie migawek (snapshot diff) → SCD2 | rejestr się realnie zmienia (stacje zamykane, nowe stanowiska); reuse logiki `silver/devices_cdc.py` |
| D12 | RLS po **województwie** (`jurisdiction_code` = kod województwa, np. `PL-12`) | zastępuje jurysdykcję elektrowni |
| D13 | Czas: wszystko w tabelach w **UTC**; „doba” liczona w **CET** (kolumna `day_cet`) | archiwum = CET, API bieżące = czas lokalny (z letnim) — jedno miejsce konwersji, z testem |
| D14 | **Każdy krok potoku = osobny wheel** uruchamiany przez task Lakeflow Joba (`python_wheel_task`); wspólny kod w wheelu `smogcast-core` ✅ | życzenie autora (nauka technologii); szczegóły w [sekcji 5a](#5a-struktura-repo-wiele-wheeli) |
| D15 | **Kod, komentarze, docstringi i logi po angielsku** ✅. Dokumentacja robocza (`README`, `docs/`, ten plan) po polsku. **Zmiana 2026-10-01 (E7): cała aplikacja po angielsku** — interfejs, odpowiedzi asystenta i baza wiedzy RAG; polskie źródła (rozporządzenie, strona GIOŚ) jako nieoficjalne tłumaczenia fragmentów o pyle, oryginały pobierane tylko do wglądu | życzenie autora; kod przeniesiony z radplume tłumaczymy przy okazji |
| D17 | **Brudzenie danych nigdy nie dotyka danych modelu.** Usterki wstrzykujemy tylko na ścieżce B (strumień, E6), do **kopii** prawdziwych pomiarów z wybranego okna demo (`conf/fault_injection.yaml`), do osobnego folderu landing i osobnych tabel strumienia (kolumna `source = 'fault_replay'`). Historia dla modelu (`silver.pm_hourly` z archiwum) nigdy nie czyta tych danych | archiwum GIOŚ jest zweryfikowane i bardzo czyste (2021–2025: 0 ujemnych, 3 wartości > 1000, ok. 8 tys. zer na 11,4 mln) — na demo czyszczenia trzeba usterki wstrzyknąć; model musi zostać nietknięty |
| D16 | Lokalne repo to PoC — **bez commitów w trakcie pracy**. Autor odtworzy historię w docelowym repo; każdy etap ma w planie gotową listę commitów („Commity do odtworzenia”) | życzenie autora |

## 4. Źródła danych — zweryfikowane 2026-10-01

### 4.1 Archiwum GIOŚ (historia, pomiary 1-godzinne)

Strona: https://powietrze.gios.gov.pl/pjp/archives → „Przygotowane dane do pobrania”.
Pobieranie bezpośrednie: `https://powietrze.gios.gov.pl/pjp/archives/downloadFile/<id>`

| Rok | id | Rozmiar | | Plik | id |
|---|---|---|---|---|---|
| 2025 | 644 | 50 MB | | Metadane — stacje i stanowiska (xlsx) | 643 |
| 2024 | 582 | 49 MB | | Statystyki 2000–2025 (xlsx) | 642 |
| 2023 | 564 | 71 MB | | | |
| 2022 | 524 | 73 MB | | | |
| 2021 | 486 | 76 MB | | | |
| 2020 | 424 | 73 MB | | | |
| 2019 | 322 | — | | | |

Uwaga: **etykiety na stronie są przesunięte względem linków** — nazwy plików sprawdzono
nagłówkiem `Content-Disposition` (`curl -sI`). Przy kolejnych latach sprawdzaj tak samo.

Zawartość zip (przykład 2025): m.in. `2025_PM10_1g.xlsx`, `2025_PM25_1g.xlsx`,
`2025_PM10_24g.xlsx`, `2025_PM25_24g.xlsx` (24g = stanowiska manualne, dobowe).
**Używamy plików `_1g`** (automatyczne, godzinowe) — dobowe średnie liczymy sami.

Format arkusza `RRRR_PM10_1g.xlsx` (sprawdzony):
- **format szeroki**: wiersze = godziny, kolumny = stanowiska; ~185 kolumn PM10, ~8766 wierszy,
- 6 wierszy nagłówka: `Nr`, `Kod stacji`, `Wskaźnik`, `Czas uśredniania`, `Jednostka` (ug/m3), `Kod stanowiska`,
- od wiersza 7: `datetime` + wartości; puste = brak pomiaru,
- **znacznik czasu = KONIEC godziny w CET** (`2025-01-01 01:00` = 00:00–01:00 CET),
- znaczniki mają szum mikrosekundowy (`03:00:00.005`) → **zaokrąglać do pełnej godziny**,
- kody stacji (np. `MpKrakAlKras`) są wspólne z API.

### 4.2 API GIOŚ v1 (bieżące dane, rejestr stacji)

Baza: `https://api.gios.gov.pl/pjp-api/v1/rest` (stare `/pjp-api/rest/...` zwraca **410 Gone**).
Dokumentacja: https://api.gios.gov.pl/pjp-api/swagger-ui/ (OpenAPI: `/pjp-api/v3/api-docs`).
Odpowiedzi JSON-LD z **polskimi nazwami pól**, paginacja `page`/`size`.

| Endpoint | Zwraca | Uwagi |
|---|---|---|
| `GET /station/findAll?size=500` | 288 stacji: `Identyfikator stacji`, `Kod stacji`, `Nazwa stacji`, `WGS84 φ N`, `WGS84 λ E`, `Nazwa miasta`, `Województwo` | współrzędne jako **tekst** |
| `GET /station/sensors/{stationId}` | stanowiska: `Identyfikator stanowiska`, `Wskaźnik - kod` (`PM10`, `PM2.5`) | np. Kraków Al. Krasińskiego: stacja 400, PM10 = 2750, PM2.5 = 2752 |
| `GET /data/getData/{idSensor}?size=500` | `Lista danych pomiarowych`: `Kod stanowiska`, `Data` (**czas lokalny**, z czasem letnim), `Wartość` | ok. 3 ostatnie doby; wartości bywają `null` |
| `GET /archivalData/getDataBySensor/{idSensor}?dateFrom=&dateTo=` | dane archiwalne stanowiska | zapas, gdyby xlsx sprawiały problem |
| `GET /levels/getPermissible` | normy: PM10 `standardValue` 50.5, `acceptNumber` 35 | do RAG / weryfikacji progów |

### 4.3 Pogoda (Open-Meteo, już obsługiwane w kodzie)

| Cel | API | Uwagi |
|---|---|---|
| prognoza na D+1 w backteście | `https://historical-forecast-api.open-meteo.com/v1/forecast` | dane od **2021**; `boundary_layer_height` **puste** w tym API — nie używać |
| prognoza na jutro (na żywo) | `https://api.open-meteo.com/v1/forecast` | ten sam zestaw zmiennych co wyżej |
| (opcjonalnie) pogoda faktyczna | `https://archive-api.open-meteo.com/v1/archive` (ERA5) | **nie** jako cecha prognozy (D7); można do analiz |

Zmienne godzinowe (te same w obu API): `temperature_2m`, `wind_speed_10m`,
`wind_direction_10m`, `precipitation`, `relative_humidity_2m`, `surface_pressure`,
`cloud_cover`, `shortwave_radiation`. Zawsze `wind_speed_unit=ms`, `timezone=GMT`
(istniejący `ingest/meteo.py` to wymusza i ma test).

## 5. Architektura docelowa: tabele

```
ŚCIEŻKA A — BATCH (historia + model)
archiwum GIOŚ (xlsx) ─► landing/gios_archive ─► bronze.pm_hourly ─► silver.pm_hourly (DQ) ─► silver.pm_daily_station
Open-Meteo (prognozy) ─► landing/weather ─► bronze.weather_forecast ─► silver.weather_daily
                                                                      └─► silver.features (miasto × doba)
                                                    ─► model (MLlib) ─► gold.backtest_predictions
                                                                      ─► gold.backtest_metrics, gold.reliability

ŚCIEŻKA B — STREAMING (na żywo: prawdziwe API + odtwarzanie z usterkami)
API GIOŚ getData (co 1 h) ─► landing/gios_live (JSON) ──┐
fault_replay (kopia archiwum + usterki) ─► landing/fault_replay ─┴► bronze.pm_stream (append) ─┬─ spóźnione ─► ops.pm_late_rejected
                                                                                                ├─ reguły twarde ─► ops.pm_quarantine
                                                                                                └─ dedup + flagi ─► silver.pm_stream (MERGE)
silver.pm_stream (live) + prognoza pogody live ─► silver.features_live ─► model forecast ─► gold.forecast_tomorrow  ◄── „czy jutro w Y?”
strumień ─► gold.dq_summary

ŚCIEŻKA C — CDC
API GIOŚ findAll/sensors (migawki) ─► bronze.station_snapshots ─► silver.stations (SCD2)
```

| Tabela | Klucz | Opis |
|---|---|---|
| `bronze.pm_hourly` | station_code, pollutant, time_utc | z archiwum: format długi, wartości bez zmian, `source_file` |
| `bronze.pm_stream` | — (append idempotentny) | surowe odczyty strumienia (`source` = `live` / `fault_replay`) |
| `bronze.weather_forecast` | city_id, time_utc, source | prognoza godzinowa (archiwalna lub bieżąca) |
| `bronze.station_snapshots` | snapshot_ts, station_id | migawki rejestru |
| `silver.stations` | station_id, valid_from | SCD2 |
| `silver.pm_stream` | source, station_code, pollutant, time_utc | strumień po czyszczeniu (oddzielnie od historii — D17) |
| `silver.features_live` | city_id, pollutant, issue_day | cechy dnia wydania (dziś) dla prognozy na żywo |
| `silver.pm_hourly` | station_code, pollutant, time_utc | po DQ: < 0 → kwarantanna, > 1000 µg/m³ → kwarantanna, skoki, wartości zamrożone |
| `silver.pm_daily_station` | station_code, pollutant, day_cet | średnia dobowa, `n_hours`, `is_valid` (≥ 18 h) |
| `silver.pm_daily_city` | city_id, pollutant, day_cet | max i średnia po stacjach, `exceeded` |
| `silver.weather_daily` | city_id, day_cet | agregaty prognozy na dobę: t_min, t_mean, wiatr średni, opad suma, ciśnienie, … |
| `silver.features` | city_id, pollutant, day_cet (= D) | cechy z D i prognozy na D+1 + etykieta `exceeded_next_day` |
| `gold.backtest_predictions` | model, city_id, pollutant, day_cet | prawdopodobieństwo vs fakt (zbiór walidacyjny i testowy) |
| `gold.backtest_metrics` | model, split, city_id, pollutant | POD, FAR, CSI, Brier, BSS, AUC, liczba dni |
| `gold.reliability` | model, split, pollutant, prob_bin | kalibracja: prognozowane vs zaobserwowane |
| `gold.forecast_tomorrow` | city_id, pollutant, target_day | **odpowiedź na pytanie główne**: P(przekroczenia), model, wersja, `issued_at` |
| `gold.dq_summary` | source, metric | unikalne odczyty, ponowne wysyłki, spóźnione, kwarantanna wg powodu, flagi (% unikalnych) |
| `ops.dq_metrics` | — | jak dotąd |

**Metryki (do wyjaśnienia na demo prostym językiem):**
- **POD** (probability of detection): z dni, gdy było przekroczenie, w ilu model ostrzegł,
- **FAR** (false alarm ratio): z ostrzeżeń, ile było fałszywych,
- **CSI**: łączna miara trafień bez dni „spokojnych”,
- **Brier score**: średni błąd kwadratowy prawdopodobieństwa (0 = idealnie),
- **BSS** (Brier skill score): o ile lepiej niż klimatologia (> 0 = model coś wnosi),
- **kalibracja** (`gold.reliability`): czy „70%” znaczy 70%.
Ostrzeżenie = prawdopodobieństwo ≥ 0,5 (próg w konfiguracji).

## 5a. Struktura repo: wiele wheeli

Monorepo z osobnym projektem Pythona (własny `pyproject.toml`, własny wheel) na każdy krok.
Wszystkie paczki dzielą przestrzeń nazw `smogcast` (PEP 420 — **brak** pliku
`src/smogcast/__init__.py` w którejkolwiek paczce), więc importy wyglądają jednolicie:
`from smogcast.core.storage import Storage`.

```
packages/
├── core/              smogcast-core              smogcast.core              config, sesja Spark, storage, metryki DQ, czas (CET/UTC),
│                                                                             conf/*.yaml jako package data; BEZ entry pointu (biblioteka)
├── ingest/            smogcast-ingest            smogcast.ingest            archiwum GIOŚ, API GIOŚ (live + rejestr), Open-Meteo → landing
├── bronze/            smogcast-bronze            smogcast.bronze            landing → bronze (xlsx → format długi, JSON, migawki)
├── silver_clean/      smogcast-silver-clean      smogcast.silver_clean      DQ: kwarantanna, spóźnione, dedup, zamrożenie; SCD2 stacji
├── silver_transform/  smogcast-silver-transform  smogcast.silver_transform  średnie dobowe, pogoda dobowa, cechy
├── model/             smogcast-model             smogcast.model             baseline'y, trening, backtest, metryki
├── gold/              smogcast-gold              smogcast.gold              prognoza na jutro, podsumowania DQ, tabele dashboardu
├── app/               smogcast-app               smogcast.app               guardrails SQL, odpowiedź „czy jutro w Y?”
└── cli/               smogcast-cli               smogcast.cli               TYLKO lokalnie: komenda `smogcast`, woła kroki z wszystkich paczek
```

Zasady:
- **Każda paczka krokowa** zależy od `smogcast-core` w **tej samej wersji** i ma jeden entry point,
  np. `smogcast-bronze = smogcast.bronze.main:main`, przyjmujący `--step <nazwa> --env <local|dev|prod>`.
  Jeden task Joba = jeden wheel + jeden `--step`.
- **Wersja wspólna** dla wszystkich paczek (plik `VERSION` w katalogu głównym; wersję wpisuje się
  w każdy `pyproject.toml` skryptem `scripts/bump_version.py` albo ręcznie — wszystkie muszą być równe,
  sprawdza to test).
- **Paczki krokowe nie importują się nawzajem** — tylko `core`. Wspólny kod (definicje tabel, schematy)
  trafia do `core`. Wyjątek: `cli` (lokalnie) importuje wszystko.
- **Testy** w `packages/<paczka>/tests/`; wspólne fikstury (sesja Spark) w `conftest.py` w katalogu
  głównym repo. `pytest` z katalogu głównego uruchamia wszystkie.
- **Docker**: obraz instaluje wszystkie paczki w trybie edytowalnym (`pip install -e packages/core -e …`).
- **Budowanie**: `python -m build --wheel packages/<paczka>` → `packages/<paczka>/dist/*.whl`.
  CI buduje wszystkie wheele i odpala testy.
- **Databricks**: `databricks.yml` → sekcja `artifacts` z wheelem dla każdej paczki; Job → task na krok:
  `python_wheel_task: {package_name: smogcast_bronze, entry_point: smogcast-bronze, parameters: ["--step", "pm-hourly", "--env", "${bundle.target}"]}`
  + `libraries: [whl core, whl bronze]`. Deklaratywny pipeline Lakeflow (ścieżka B) używa logiki
  z wheela `smogcast-silver-clean` (biblioteka w środowisku pipeline'u).
- Koszt tej decyzji (do write-upu): więcej plików konfiguracyjnych, wersjonowanie w lockstepie,
  wolniejszy build CI; zysk: niezależne wdrażanie i jasne granice odpowiedzialności kroków.

## 6. Etapy

Każdy etap: **zadania → pliki → kryteria akceptacji → komendy sprawdzające**.
Długie komendy (build, przebiegi, pełne testy) uruchamiać **w tle** albo dać autorowi
do uruchomienia ręcznie (zasada w `CLAUDE.md`). Wszystko uruchamiamy w Dockerze.

### E0. Zabezpieczenie stanu radplume ✅

Bez commitów (D16) — kopia katalogu.
- [x] Kopia całego repo **bez** `data/` i `data-offline/` (z `.git`) do
      `C:\Users\Leszek\DEMO RADPLUME\radplume-final\` (poza repo).
- **Akceptacja:** kopia istnieje i zawiera `src/radplume/validation/metrics.py` w wersji z poprawkami
  (`n_model_zero`) oraz `scripts/convert_*.py`.
- Commity do odtworzenia (stan radplume przed zmianą tematu):
  `fix(validation): count model zeros as misses, add breakdown by sector and distance`,
  `feat(validation): 282 h window and weather-perturbed ensemble`,
  `feat(meteo): station precipitation from AMeDAS, per-site wind sigma`,
  `chore(scripts): converters for Katata 2015, NRA soil, AMeDAS`.

### E1. Szkielet nowego projektu ✅

- [x] Struktura z [sekcji 5a](#5a-struktura-repo-wiele-wheeli): `packages/<paczka>/{pyproject.toml, src/smogcast/<paczka>/, tests/}`,
      plik `VERSION`, główny `pyproject.toml` tylko z konfiguracją narzędzi (ruff, pytest), główny `conftest.py`.
- [x] Przeniesienie kodu, który zostaje ([sekcja 7](#7-mapa-plików)), do właściwych paczek;
      **tłumaczenie komentarzy, docstringów i logów na angielski** (D15).
- [x] Usunięcie modułów radplume i ich testów; usunięcie `src/radplume/`.
- [x] Dockerfile (instalacja wszystkich paczek edytowalnie, + `openpyxl`, `build`), docker-compose, CI
      (budowanie wszystkich wheeli + testy), `databricks.yml` (artifacts per paczka), `resources/job_smogcast.yml` (szkielet).
- [x] W każdej paczce krokowej `main.py` z argparse (`--step`, `--env`, `--offline`) i rejestrem kroków (na razie stuby).
- [x] Nowa konfiguracja: `conf/cities.yaml` (miasta D2: id, nazwa, województwo, współrzędne
      środka do pogody; stacje dobierane automatycznie po `Nazwa miasta` z rejestru),
      `conf/thresholds.yaml` (sekcja 2), `conf/pm_dq.yaml` (progi DQ), `conf/model.yaml`
      (podział lat D8, cechy, próg ostrzeżenia), `local/dev/prod.yaml` (lata, miasta, ścieżki).
- [x] `cli.py` z pustymi krokami (stuby) i nową listą komend.
- **Akceptacja:** `docker compose build` OK; `smogcast --help` i np. `smogcast-bronze --help` działają;
  `python -m build --wheel` przechodzi dla każdej paczki; `pytest -q` przechodzi (testy infrastruktury:
  config, storage, guardrails); `ruff check packages` czysto.
- Commity do odtworzenia: `chore: archive radplume, start smogcast`, `build: multi-wheel monorepo layout`,
  `refactor(core): move shared infrastructure to smogcast-core`, `ci: build all wheels and run tests`.

### E2. Historia PM z archiwum GIOŚ ✅

- [x] `smogcast.ingest.gios_archive`: pobranie zip dla `run.archive_years` (id w `conf/gios.yaml`, cache w
      `landing/gios_archive/raw/`) + metadane stacji (id 643). **Każde pobranie sprawdza nazwę pliku z
      `Content-Disposition`** (rok / „Metadane”) — chroni przed przesuniętymi etykietami na stronie GIOŚ.
- [x] `smogcast.bronze.pm_archive` (krok `pm-hourly`): arkusz `<rok>_<PM10|PM25>_1g.xlsx` czytany strumieniowo
      (openpyxl, read-only) → długi CSV gzip w `landing/gios_archive/long/` (cache) → Spark → `bronze.pm_hourly`,
      partycja `source_year`, zapis `replaceWhere` per rok (idempotentny, tańszy niż MERGE 11 mln wierszy).
      Nagłówek znajdowany po etykiecie „Kod stanowiska”, nie po numerze wiersza; puste komórki pomijane i liczone.
- [x] Czas: `smogcast.core.timeutil` — zaokrąglenie do najbliższej godziny (szum Excela +5 ms/wiersz, do ~44 s),
      koniec godziny CET → początek godziny UTC; `day_cet_col` na później (E4).
- [x] **Dodatkowo** krok `station-meta` (`smogcast.bronze.station_meta`): metadane → `bronze.gios_stations`
      (1148 stacji, **także zamknięte**: miejscowość, współrzędne, daty, stare kody) i `bronze.gios_positions` (5849).
      Potrzebne do przypisania stacji do miast w latach, gdy API ich już nie zna.
- [x] Testy: konwersja czasu (sylwester, brak czasu letniego, szum), parsowanie (przecinek, śmieci, puste),
      brak nagłówka, krok end-to-end na mini-archiwum + idempotencja, metadane, ochrona przed złym plikiem.
- **Akceptacja: spełniona.** Prawdziwe dane 2021–2025: **11 409 572** wartości godzinowych, 0 nieczytelnych komórek,
  2024 = 8784 h (rok przestępny); ponowne uruchomienie nie dubluje. Każde z 10 miast ma PM10 i PM2.5
  (od 1 stacji — Lublin, Wrocław PM10 — do 9 — Kraków PM10); średnia PM10 2021–2025 najwyższa w Krakowie (27,4 µg/m³).
- **Uwaga dla E4:** 8 kodów stacji z pomiarów nie ma w `bronze.gios_stations` jako `station_code` — sprawdzić
  `old_station_codes` (kolumna „Stary Kod stacji”) i stacje mobilne (`…MOB`) przy przypisaniu do miast.

- Commity do odtworzenia: `feat(ingest): GIOŚ yearly archive download with file-name guard`, `feat(core): CET/UTC time helpers`, `feat(bronze): wide xlsx to long hourly PM table`, `feat(bronze): GIOŚ station and position metadata`, `test(bronze): CET to UTC and wide-to-long conversion`, `docs: living ARCHITECTURE.md with diagrams`

### E3. Pogoda — archiwum prognoz ✅

- [x] `smogcast.ingest.weather` (krok `weather-forecast-history`): dwa archiwa Open-Meteo z `conf/weather.yaml` —
      `short_range` (2021–2025) i `day_ahead` (`_previous_day1`, 2024–2025) — per miasto i rok, cache w
      `landing/weather/<źródło>/<miasto>/<rok>.json`; bieżący rok pobierany ponownie. Kontrakt danych z radplume:
      m/s wymuszone i sprawdzane, UTC, długości tablic; zapisane współrzędne żądane i zwrócone.
- [x] `smogcast.bronze.weather` (krok `weather-forecast`): JSON → `bronze.weather_forecast` czystym Sparkiem
      (`arrays_zip` + `explode`), przyrostek `_previous_day1` zdejmowany → jeden schemat dla obu źródeł.
- [x] `smogcast.silver_transform.weather_daily` (krok `weather-daily`): agregaty na dobę CET i źródło:
      t średnia/min/max/rozpiętość, wilgotność, wiatr średni/min/max, **godziny ciszy** (< 1,5 m/s), średni kierunek
      (średnia wektorowa), opad suma i godziny, ciśnienie, zachmurzenie, promieniowanie; `is_complete` (≥ 20 h).
- [x] Testy: kontrakt API (km/h, UTC, długości, pusto), cache i bieżący rok, bronze dla obu źródeł (przyrostek, null),
      granice doby CET, kierunek wiatru wokół północy, godziny ciszy, dni niekompletne. **65 testów OK.**
- **Akceptacja: spełniona.** `silver.weather_daily`: short_range 18 270 dni (2021–2025 × 10 miast, 10 niekompletnych),
  day_ahead 7140 dni (od 2024-01-19, 20 niekompletnych — początek archiwum i ostatni dzień). Różnica źródeł w tych
  samych dniach: MAE temperatury dobowej 0,40°C, wiatru 0,21 m/s — dane treningowe niewiele lepsze od testowych.
- Decyzja D7 doprecyzowana (dwa źródła, trening/test) i kryteria akceptacji modelu K1–K4 zamrożone (sekcja 2a,
  `conf/model.yaml` → `model.acceptance`) **przed** treningiem.

- Commity do odtworzenia: `feat(ingest): Open-Meteo short-range and day-ahead forecast archives`, `feat(bronze): weather forecast table`, `feat(silver-transform): daily weather aggregates`, `docs: freeze model acceptance criteria K1-K4`

### E4. Silver: czyszczenie, średnie dobowe, cechy ✅

- [x] `smogcast.silver_clean.station_city`: stacje → miasta po miejscowości z metadanych (bez listy na sztywno);
      **stare kody** („Stary Kod stacji”, kilka na stację) mapowane na dzisiejszy kod → `silver.station_city`.
      8 kodów spoza metadanych (E2) to małe miejscowości spoza 10 miast — bez wpływu.
- [x] `smogcast.silver_clean.pm_clean` (krok `pm-hourly`): reguły twarde (< 0, > 1000, brak) → `ops.pm_quarantine`
      z powodem (`source = 'archive'`); flagi `frozen` (≥ 6 kolejnych godzin tej samej wartości, cała seria; przerwa
      dzieli serię) i `spike` (> 3× najwyższego z 2+2 sąsiadów i > 100 µg/m³) → `dq_flag`, `is_valid`;
      `silver.pm_hourly` (tylko stacje 10 miast, kod ujednolicony, `day_cet`, `hour_cet`). Kod do ponownego użycia w E6.
- [x] `smogcast.silver_transform.pm_daily` (krok `pm-daily`): `silver.pm_daily_station` (≥ 18 ważnych godzin;
      „poranek” = godziny 00–10 CET, ≥ 8 h) i `silver.pm_daily_city` (max i średnia po stacjach, `exceeded`, NULL gdy brak danych).
- [x] `smogcast.silver_transform.features` (krok `features`): wiersz na (miasto, zanieczyszczenie, dzień wydania D):
      PM z D-1 i D-2, poranek D, prognoza pogody na D+1 (źródło wg podziału — D7), kalendarz D+1, etykieta
      `exceeded_next_day`, `split`, `is_usable`. Konfiguracja okna porannego w `conf/model.yaml`.
- [x] Testy: mapowanie starych kodów, kwarantanna z powodem, zamrożenie (długość, przerwa), skok vs prawdziwy
      epizod, reguła 18 h, okno poranne, miasto = najgorsza stacja, wyrównanie D-1/D/D+1, źródło pogody per podział,
      **test braku wycieku** (zmiana D+1 zmienia tylko etykietę). 74 testy OK.
- **Akceptacja: spełniona.** Na prawdziwych danych: 149 stacji w 10 miastach (+53 stare kody); 2,10 mln godzin OK,
  2057 `frozen`, 39 `spike`, 0 w kwarantannie (archiwum zweryfikowane — D17). Częstość przekroczeń 2021–2025
  wiarygodna: PM10 od 3,8% (Szczecin) do 17,1% (Kraków), PM2.5 od 10,5% (Szczecin) do 24,8% (Kraków).
  Cechy (usable): trening ~10,7 tys. / ~10,4 tys. wierszy (PM10 / PM2.5), walidacja ~3,5 tys., test ~3,6 tys.
- **Uwaga dla E5:** klasy niezrównoważone — przekroczenie PM10 jutro w 8% dni (trening), 5% (walidacja 2024),
  8% (test 2025); PM2.5 odpowiednio 17% / 15% / 19%. Kryterium K4 (POD ≥ 60% przy P ≥ 0,5) będzie wymagające.

- Commity do odtworzenia: `feat(silver-clean): station to city mapping with old codes`, `feat(silver-clean): hard rules, quarantine, frozen and spike flags`, `feat(silver-transform): daily station/city means and exceedances`, `feat(silver-transform): leakage-free features`, `test: no-leakage feature check`

### E5. Model i backtest ✅ (werdykt obowiązujący od 2026-10-06: PM2.5 spełnia K1–K3; PM10 nie spełnia K4a, K3 na granicy)

> **Aktualizacja 2026-10-06 — wyniki obowiązujące.** Pierwotne wyniki E5 (tabela niżej, „historyczne”) pochodziły z
> GBT, który nie był powtarzalny między środowiskami (szum zmiennoprzecinkowy + podział na partycje — dziennik
> 2026-10-06). Po dwóch poprawkach powtarzalności (`fit_input`, zaokrąglenie cech) laptop i Databricks DEV dają
> **identyczne** wyniki. Reguła wyboru, siatki i kryteria K1–K4 **bez zmian**; nic nie było strojone na roku testowym.
>
> | Rok testowy 2025 | PM10 (GBT gł. 3, 100 drzew) | PM2.5 (GBT gł. 5, 50 drzew) |
> |---|---|---|
> | Brier: model / klimatologia / persystencja | **0,0434** / 0,0615 / 0,0933 | **0,0751** / 0,117 / 0,172 |
> | BSS vs klimatologia · AUC | 0,30 · 0,95 | 0,36 · 0,94 |
> | POD / FAR (P ≥ 0,5) | 44% (129 z 292) / 32% | 70% (490 z 697) / 27% |
> | K1 · K2 | ✅ · ✅ | ✅ · ✅ |
> | K3 kalibracja ≤ 15 p.p. | ✅ **8,5 p.p.** (przedział 0,4–0,5: prognoza 45%, wystąpiło 53%, 60 dni) — **kruche**, patrz niżej | ✅ 7,2 p.p. |
> | K4a POD ≥ 60% · K4b FAR ≤ 50% | ❌ 44% · ✅ 32% | — |
> | **Werdykt** | **NIE spełnia** (K4a) | **spełnia** |
>
> **K3 dla PM10 jest kruche.** W wersjach różniących się tylko szumem obliczeń (ten sam model, te same dane) największa
> rozbieżność kalibracji wynosiła 21, 20,6, 12,9 i 8,5 p.p. — werdykt K3 zmieniał się w obie strony. Powód: przedziały
> wysokich prognoz PM10 mają po ~30 dni (np. 0,7–0,8: **29 dni** w wersji obowiązującej, więc poza oceną; w E5 ≥ 30 dni
> i rozbieżność 21 p.p.). Uczciwie: kalibracja PM10 jest **na granicy** kryterium, a nie wyraźnie spełniona.
> Wykrywalność (K4a, 44%) jest stabilnie poniżej progu — model przegapia ponad połowę dni z przekroczeniem PM10.

- [x] `smogcast.model.prepare`: stałe przekształcenia (log1p stężeń, sin/cos kierunku wiatru i miesiąca,
      uzupełnienie poranka wczorajszą średnią + wskaźnik braku) — bez statystyk z danych, więc bez wycieku.
- [x] `smogcast.model.baselines`: persystencja (0/1 z D-1) i klimatologia (miasto × miesiąc, **tylko lata treningowe**).
- [x] `smogcast.model.train` (krok `train`): MLlib — regresja logistyczna i GBT, miasto jako one-hot; siatki
      z `conf/model.yaml` oceniane Brierem na walidacji 2024; model operacyjny = najniższy Brier walidacyjny
      (reguła zamrożona przed wynikami); refit na 2021–2024 → modele `tuned` i `final` w `data/models/`.
- [x] `smogcast.model.evaluate` (krok `backtest`): `gold.backtest_predictions`, `gold.backtest_metrics`
      (Brier, BSS vs klimatologia, POD, FAR, CSI, AUC; per miasto + ALL), `gold.reliability`, `gold.acceptance`,
      `gold.model_selection`. Dzielenia przez zero (np. miasto bez przekroczeń) → NULL (`try_divide`).
- [x] Testy: metryki na ręcznie policzonym przykładzie, kalibracja, logika K1–K4, klimatologia tylko z treningu,
      pełny train + backtest na danych syntetycznych ze znanym sygnałem. **79 testów OK.**

**Wyniki historyczne z 2026-10-01** (przed poprawką powtarzalności; obowiązujące — wyżej) (rok testowy 2025, 10 miast,
prognoza na jutro z prawdziwą prognozą pogody z poprzedniego dnia):

| | PM10 (model GBT) | PM2.5 (model GBT) |
|---|---|---|
| dni / dni z przekroczeniem | 3613 / 292 (8,1%) | 3627 / 697 (19,2%) |
| Brier: model / klimatologia / persystencja | **0,047** / 0,062 / 0,093 | **0,077** / 0,117 / 0,172 |
| BSS vs klimatologia | 0,24 | 0,35 |
| AUC | 0,94 | 0,94 |
| POD / FAR (P ≥ 0,5) | 47% / 38% | 70% / 27% |
| K1 BSS > 0 | ✅ | ✅ |
| K2 lepszy niż persystencja | ✅ | ✅ |
| K3 kalibracja ≤ 15 p.p. | ❌ 21 p.p. (przedział 0,7–0,8: prognoza 76%, wystąpiło 55%) | ✅ 11,5 p.p. |
| K4a POD ≥ 60% | ❌ 47% | — |
| K4b FAR ≤ 50% | ✅ 38% | — |
| **Werdykt wg kryteriów zamrożonych przed treningiem** | **NIE spełnia** (K3, K4a) | **spełnia** |

Obserwacje do write-upu (bez zmiany werdyktu):
- Wybór GBT dla PM10 nastąpił na walidacji z minimalną przewagą (Brier 0,0391 vs 0,0396). **Na teście regresja
  logistyczna wypadła lepiej** (Brier 0,040, BSS 0,35, FAR 26%) — ale zamiana modelu teraz byłaby wyborem na
  podstawie testu, więc jej nie robimy. Jej ewentualne potwierdzenie wymaga nowego, niewidzianego okresu (niżej).
- GBT PM10 jest **zbyt pewny siebie** w przedziale 50–90%: przy prognozach 50–90% przekroczenie występuje rzadziej.
- 2025 było bardziej smogowe niż 2024 (PM10: 8,1% vs 5,3% dni) — model i tak pobił obie proste reguły w 9 z 10
  miast (wyjątek: PM10 Bydgoszcz, gorszy od klimatologii).
- Persystencja ma ujemny BSS, bo daje prognozy 0/1, które Brier mocno karze — mimo to POD 42%, więc
  „jutro jak dziś” nie jest głupią regułą.

**Dlaczego PM10 przewiduje się gorzej niż PM2.5** (ustalenie z autorem 2026-10-01, do write-upu i na demo):
- PM2.5 to głównie pył ze **spalania** (piece domowe, ruch) — zależy prawie wyłącznie od **pogody, którą model zna
  z prognozy** (zimno → więcej palenia, cisza wiatrowa → dym zostaje nad miastem). Stąd dobry wynik.
- PM10 = PM2.5 **plus pył grubszy**: z suchych ulic podrywany przez ruch, z budów, ziemi, piasku po zimie. Te
  czynniki **nie są w modelu** (brak danych o stanie dróg, robotach, posypywaniu) — model widzi tylko część przyczyn.
- Przekroczenie PM10 jest **rzadsze** (~8% dni vs ~17–19% dla PM2.5) → mniej przykładów w treningu (~900 vs ~1800 dni)
  i łatwiej o fałszywą pewność; norma 50 µg/m³ wymaga **silnego** epizodu, więc model musi przewidzieć „jak bardzo”,
  a nie tylko „czy będzie smog”.
- PM10 mierzy więcej stanowisk, także przy ruchliwych ulicach (Kraków: 9 PM10 vs 2 PM2.5) — lokalny skok jednej
  stacji wystarcza do przekroczenia w mieście, a takich skoków nie widać w prognozie pogody.
- Wniosek: model PM10 dobrze **porządkuje** dni od najmniej do najbardziej groźnych (AUC 0,95), ale przegapia ponad
  połowę dni z przekroczeniem, a jego kalibracja jest na granicy kryterium. Możliwe ulepszenia na przyszłość (poza zakresem PoC): cechy o stanie nawierzchni
  (opad / dni bez deszczu), sezonie posypywania, dniach roboczych przy stacjach komunikacyjnych.

**Możliwy etap E5b (do decyzji autora):** poprawka kalibracji PM10 (np. kalibracja izotoniczna dopasowana na
walidacji) i/lub regresja logistyczna jako model PM10 — oceniane na **nowym, niewidzianym okresie 2026-01…2026-09**
(dane z API GIOŚ `archivalData`, prognozy `day_ahead` dostępne). Wynik 2025 pozostaje taki, jak wyżej.

- Commity do odtworzenia: `feat(core): shared feature schema`, `feat(model): persistence and climatology baselines`, `feat(model): logistic regression and GBT with validation-based selection`, `feat(model): backtest metrics, reliability and K1-K4 verdict`

### E6. Na żywo: streaming + prognoza na jutro + CDC ✅

**Podział na kroki (2026-10-01)** — każdy zamykany osobno, w tej kolejności:

| Krok | Zakres | Tabele | Stan |
|---|---|---|---|
| E6a | rejestr stacji: `ingest gios-registry` (migawka `findAll` + `sensors`) → `bronze station-snapshots` → `silver-clean stations-scd2` (różnice migawek → zmiany INSERT/UPDATE/DELETE → SCD2; wzorzec z `legacy/radplume/silver/devices_cdc.py`) | `bronze.station_snapshots`, `silver.station_changes`, `silver.stations` | ✅ 53 stacje, 354 stanowiska (pierwsza migawka = 53 INSERT); testy CDC: insert/update/delete/powrót |
| E6b | pomiary na żywo: `ingest gios-live` (`getData` dla stanowisk PM10/PM2.5 10 miast z ostatniej migawki) → `landing/gios_live/` (JSON, czas lokalny Europe/Warsaw + `fetched_at`) | landing | ✅ 101 stanowisk (33 manualne bez danych na żywo — pomijane) |
| E6c | wstrzykiwanie usterek: `ingest fault-replay` — kopia prawdziwych godzin z `bronze.pm_hourly` dla okna z `conf/fault_injection.yaml`, ten sam format co E6b, usterki wg harmonogramu (tabela niżej) → `landing/fault_replay/` + migawki rejestru `landing/fault_replay_registry/` | landing | ✅ Kraków 17–21.01.2025 |
| E6d | czyszczenie strumienia: `silver-clean pm-stream` — Structured Streaming (`availableNow`) osobno dla źródeł `live` i `fault_replay`; `foreachBatch`: spóźnione, dedup, reguły twarde, flagi `frozen`/`spike` (kod z E4) + `drift` vs mediana innych stacji miasta; zapis idempotentny. Resety: `silver-clean fault-reset`, `silver-clean live-reset` | `bronze.pm_stream`, `ops.pm_late_rejected`, `ops.pm_quarantine`, `silver.pm_stream` | ✅ każda usterka we właściwym miejscu, prawdziwy smog bez flag |
| E6e | prognoza na jutro: `ingest weather-forecast-live` → `bronze weather-forecast` (źródło `live`) → `silver-transform weather-daily` → `silver-transform features-live` (dzień wydania = dziś CET, tylko `source = 'live'`) → **`model forecast`** (model operacyjny `final`) | `silver.features_live`, `gold.forecast_tomorrow` | ✅ wiersz dla każdego miasta i zanieczyszczenia |
| E6f | `gold dq-summary`, kolejności w CLI (`run-live`, `run-fault-demo`), Job na żywo `resources/job_smogcast_live.yml`, dokumentacja | `gold.dq_summary` | ✅ |

Zmiana względem szkicu: krok prognozy jest w wheelu **`smogcast-model`** (`forecast`), nie `smogcast-gold` — potrzebuje
zapisanego modelu i `prepare` z paczki model, a paczki krokowe nie importują się nawzajem (sekcja 5a).
`silver.pm_stream` jest **oddzielna** od `silver.pm_hourly` (historia dla modelu) — decyzja D17.

- [x] `ingest/gios_live.py`: odpytanie `getData` dla stanowisk PM10/PM2.5 miast z konfiguracji →
      `landing/gios_live/batch_<ts>.json` (czas lokalny → UTC: strefa `Europe/Warsaw`, w `smogcast.core.timeutil`).
- [x] `silver_clean/pm_stream.py` (zamiast `pipelines/stream.py`): Structured Streaming z `landing/gios_live` i
      `landing/fault_replay`: spóźnione (> 3 h **i** należne już przy poprzednim odpytaniu), dedup po
      (źródło, stanowisko, godzina), reguły twarde, flagi, MERGE do **`silver.pm_stream`** (nie `silver.pm_hourly` — D17).
      O klasyfikacji klucza decyduje jego **pierwsze** nadejście w paczce (kilka odpytań w jednej mikro-paczce nie robi
      z ponownej wysyłki „spóźnienia”), wartość — najnowsza niepusta.
- [x] `ingest/gios_registry.py` + `silver_clean/stations_scd2.py` → `silver.stations` (SCD2 z migawek).
- [x] Prognoza: `silver-transform features-live` (te same funkcje średnich i cech co historia; `build_features` z
      `weather_source="live"`, `require_label=False` i siatką miasto × zanieczyszczenie) → `model forecast` →
      `gold.forecast_tomorrow` (miasto bez danych dostaje wiersz `status = no_forecast` z powodem, nie zgadywaną wartość).
      Dzień wydania: `smogcast.core.timeutil.issue_day_cet()` (dziś CET; `SMOGCAST_ISSUE_DATE` do testów i demo).
- [x] **Wstrzykiwanie usterek** (`smogcast.ingest.fault_replay`, przeróbka `simulators/sensor_sim.py`):
      bierze prawdziwe pomiary godzinowe wybranych stanowisk i okna czasu (z `bronze.pm_hourly`),
      emituje je jako paczki JSON do `landing/fault_replay/` (ten sam format co API) i psuje według
      harmonogramu demo (`conf/fault_injection.yaml`, deterministycznie):

      | Usterka | Oczekiwany wynik w potoku |
      |---|---|
      | awaria łączności → paczka zaległych odczytów (lag > progu) | `ops.pm_late_rejected` |
      | ponowna wysyłka tej samej paczki | jeden rekord w `silver.pm_stream` (dedup) |
      | wartość ujemna / > 1000 µg/m³ | `ops.pm_quarantine`, powód reguły |
      | brak wartości przy działającym stanowisku | `ops.pm_quarantine` |
      | zamrożony odczyt (≥ N godzin tej samej wartości) | flaga `frozen` |
      | dryf (rosnący błąd) vs inne stacje miasta | flaga `drift` |
      | skok (spike) pojedynczej godziny | flaga `spike` |
      | prawdziwy epizod smogowy u kilku stacji naraz | **brak** flagi — realny sygnał, nie usterka (test sąsiadów) |

      Dryf wykrywamy względem **mediany pozostałych stacji tego samego miasta** (zastępuje porównanie
      z modelem dyspersji z radplume; logika „sąsiedzi podnoszą się razem = sygnał” zostaje).
- [x] `gold dq-summary` → `gold.dq_summary` (unikalne odczyty, ponowne wysyłki, spóźnione, kwarantanna wg powodu,
      flagi; per źródło `live` / `fault_replay`).
- [x] Komendy `smogcast run-live` i `smogcast run-fault-demo` (z resetem na starcie); Job `resources/job_smogcast_live.yml`
      (co godzinę, wstrzymany); test pilnuje zgodności Jobów z kolejnościami w CLI.
- [x] Tryb `--offline` dla strumienia (wartości syntetyczne, gdy brak archiwum) — zrobione w E8 (generator syntetyczny pisze też paczki `getData` i migawki rejestru).
- **Akceptacja:** `smogcast run-live` pobiera dane, strumień przechodzi bez błędów, ponowne
  uruchomienie nie dubluje; `gold.forecast_tomorrow` ma wiersz dla każdego miasta i zanieczyszczenia.

- Commity do odtworzenia: `feat(ingest): live GIOŚ API polling`, `feat(ingest): station registry snapshots`,
  `feat(silver-clean): station registry SCD2 from snapshot diffs`, `feat(ingest): fault-injection replay of real measurements`,
  `feat(silver-clean): structured streaming cleaning with drift detection`,
  `fix(silver-clean): judge late arrivals by the first poll in a micro-batch`, `fix(silver-clean): scope idempotent appends to the streaming query id`, `feat(silver-clean): live-reset step`,
  `feat(ingest): live weather forecast`, `feat(silver-transform): live features for today's issue day`,
  `feat(model): tomorrow forecast with the operational model`, `feat(gold): stream data-quality summary`,
  `feat(cli): live and fault-demo orders`, `feat(bundle): hourly live Lakeflow Job`, `docs: E6 architecture and plan`

### E7. Aplikacja: panel + asystent (RAG, text-to-SQL) ✅

Zakres uzgodniony z autorem (2026-10-01): **panel + czat** w Streamlit (wzorowany na szkielecie RAG autora,
`C:\Users\Leszek\RAG\ai-knowledge-assistant-main`), model językowy autora przez API zgodne z OpenAI (LM Studio
lokalnie), korpus RAG = normy i zdrowie + wiedza o projekcie po angielsku.

- [x] Zasada: **liczby nigdy z modelu językowego.** Prognoza i jakość modelu z tabel gold przez guardrails (SQL zawsze
      pokazany); model tylko sformułowuje wyniki i odpowiada z dokumentów (źródła zawsze pokazane).
- [x] `smogcast-app` (`packages/app`): `gold.py` (odczyt gold: guardrails → podmiana `gold.<tabela>` na odwołanie
      środowiska → Spark SQL), `forecast.py` (deterministyczna odpowiedź „czy jutro w Y” z kalibracją z roku testowego i
      werdyktem K1–K4 — działa bez modelu językowego), `llm.py` (endpoint zgodny z OpenAI, `model: auto`, wycinanie
      `<think>`), `rag.py` (md/PDF/HTML → fragmenty z miejscem w źródle → embeddingi wielojęzyczne → indeks numpy;
      **wyszukiwanie hybrydowe** embeddingi + BM25), `assistant.py` (trasy: `forecast` / `data` text-to-SQL z 3 próbami
      i przykładami / `knowledge` RAG / `other` odmowa; routing najpierw regułami, model tylko w przypadkach niejasnych),
      `ui/` (Streamlit: Jutro, Miasto, Model, Jakość danych, Asystent; paleta zwalidowana pod daltonizm).
- [x] Komendy: `smogcast ask --city X` (bez LLM), `smogcast-app chat "…"`, `smogcast-app build-index`,
      `smogcast-app ui` / `docker compose up app` (http://localhost:8501); krok `ingest rag-documents`.
- [x] Konfiguracja `conf/app.yaml` (+ zmienne `SMOGCAST_LLM_BASE_URL/MODEL/API_KEY` — klucze nigdy w plikach);
      Docker: PyTorch CPU, dodatki `smogcast-app[ui,rag]`, port 8501, `host.docker.internal` do LM Studio.
- [x] Lista dokumentów RAG: `docs/rag-dokumenty.md`; wiedza o projekcie po angielsku: `docs/rag/*.md`.
- [x] Testy: 33 w paczce app (atrapa LLM i embeddingów; gold na prawdziwym Sparku) + test bezgłowy wszystkich widoków
      (`streamlit.testing`) na prawdziwych danych.
- **Akceptacja: spełniona.** Odpowiedź dla każdego miasta (także „brak prognozy” z powodem); odmowa dla miasta spoza
  konfiguracji (`smogcast ask --city Katowice` → lista dostępnych miast); guardrails bez zmian i z testami.

**Ocena na prawdziwym modelu (`qwen2.5-7b` w LM Studio).** Runda 1 (aplikacja i korpus po polsku): 7/8 po
poprawkach; model mylił poziom informowania (100) z alarmowym (150) mimo poprawnego fragmentu. **Po przejściu na
angielski** (aplikacja, odpowiedzi, korpus — decyzja autora, D15) i z tłumaczeniami polskich źródeł w formie tabel:
9/9 poprawnie — prognoza dla Krakowa, poziom informowania **100** (także na pytanie zadane po polsku; odpowiedź po
angielsku), alarmowy 150, najlepszy Brier PM10 2025 (Szczecin, 0,0285 — text-to-SQL), liczba odczytów w kwarantannie
(170 łącznie; po poprawce promptu: suma ≠ suma + rozbicie), dlaczego PM10 gorzej, limity UE 2030 (podał roczne 10 µg/m³,
pominął dobowe 25 — niepełne, ale poprawne), odmowa dla pytania o mundial. Poprawki z oceny: routing regułami
(dwujęzyczne słowa kluczowe), opis kolumn i przykłady SQL w promptach, wyszukiwanie hybrydowe, trasa `other`,
wycinanie `<think>`/`<thinking>`.

**Uwagi autora do interfejsu (ustalone 2026-10-02, wprowadzane przed E8):**

| # | Uwaga | Ustalenie | Stan |
|---|---|---|---|
| U1 | nowa zakładka „Info”: jak model przewiduje, czy jutro będzie smog | strona `Info` **na początku nawigacji** (domyślną stroną zostaje `Tomorrow`); prosty język dla słuchaczy demo, szczegóły techniczne w rozwijanych sekcjach; progi z konfiguracji, wyniki — odesłanie do strony Model (bez liczb wpisanych na sztywno) | ✅ |
| U2 | „jak działa model Monte Carlo” | model **nie używa** Monte Carlo (to skojarzenie z zespołem zaburzonej pogody w radplume) — prawdopodobieństwo liczy wyuczony GBT w jednym przebiegu. Decyzja autora: **tylko opis GBT**, bez wzmianki o Monte Carlo | ✅ |
| U3 | Model: opis pod wykresem „Calibration: does “70%” mean 70%?” | co na osiach, przekątna, punkt = grupa dni o podobnej prognozie, punkty wyblakłe (< 30 dni), związek z K3; przykład odczytu liczony z `gold.reliability` (największa rozbieżność w ocenianych przedziałach) | ✅ |
| U4 | (zauważone na zrzucie autora) ucięta tabela K1–K4 i podpis osi Briera na stronie Model | autor: poprawić teraz | ✅ |

Wyjaśnione w rozmowie (bez zmian w kodzie): czym są źródła `Live GIOŚ API` i `Fault demo` na stronie Data quality —
model jest uczony na archiwum GIOŚ 2021–2025, nie na API na żywo (szczegóły w dzienniku 2026-10-02).

- Commity do odtworzenia: `feat(app): guarded gold reader with environment-aware table names`,
  `feat(app): English interface, answers and knowledge base`, `docs: unofficial English translations of Polish sources`,
  `feat(app): deterministic tomorrow answer with calibration track record`, `feat(app): OpenAI-compatible LLM client`,
  `feat(ingest): official air-quality documents for RAG`, `feat(app): hybrid RAG index (embeddings + BM25)`,
  `feat(app): assistant routing, text-to-SQL and RAG answers`, `feat(app): Streamlit dashboard and chat`,
  `build: app extras, CPU torch, web service in compose`, `docs: English knowledge base and RAG document list`,
  `feat(app): how-it-works info page and calibration chart guide`, `fix(app): acceptance table and Brier chart no longer clipped`

### E8. Dokumentacja, testy, CI 🚧 (wszystko gotowe; czeka na potwierdzenie zielonego CI na GitHub Actions)

**Zakres ustalony z autorem (2026-10-06):** tryb offline **tylko jako źródło danych dla testów integracyjnych w CI**
(wersja odchudzona — bez dopracowywania komendy dla ludzi, bez `run-all`); scenariusz demo jakości danych w E8, Job
demo usterek na Databricks w E9.

- [x] Usunięcie `legacy/` (wzorce przeniesione w E3–E7) + odwołania w `.dockerignore`, ruff, `databricks.yml`.
- [x] Generator danych syntetycznych `smogcast.ingest.synthetic` (podpięty pod kroki ingest przy `sources: synthetic`):
      zapisuje **te same pliki co prawdziwe źródła** — roczne zip z arkuszami xlsx GIOŚ, metadane (STACJE/STANOWISKA),
      migawki rejestru API, paczki `getData`, JSON-y Open-Meteo (short_range/day_ahead/live) — więc dalsze kroki działają
      bez zmian. Wartości = czyste funkcje (ziarno, stacja, czas): jedna ukryta „prawdziwa pogoda” na miasto, PM od niej
      zależy, prognozy = prawda + błąd. Konfiguracja `conf/synthetic.yaml`; `OFFLINE_OVERRIDES` (2 miasta, małe siatki).
      Nagłówki plików GIOŚ przeniesione do `smogcast.core.schema` (kontrakt generator ↔ bronze; polskie etykiety zostają —
      to dokładne nazwy w plikach GIOŚ). `smogcast --offline run-batch` w Dockerze: 2 min 29 s.
- [x] Test integracyjny `packages/cli/tests/test_integration.py` (znacznik `integration`): historia → model → 2× na żywo
      → demo usterek na danych syntetycznych; sprawdza warstwy, ponowne uruchomienie bez dubli, wybór modelu i K1–K4,
      4 prognozy na jutro, brak duplikatów przy powtórnym odpytaniu, usterki w kwarantannie/odrzuconych/flagach, CDC.
      Wykrył 2 błędy: parser metadanych w bronze (krótsze wiersze xlsx → `IndexError`; poprawione) i liczbę stacji w
      generatorze. Brak wycieku sprawdza istniejący test jednostkowy cech.
- [x] Scenariusz demo jakości danych: `docs/demo-jakosc-danych.md` (kroki, SQL, co powiedzieć, pytania).
- [x] CI (`.github/workflows/ci.yml`) przygotowane: uruchamia się przy PR do `main`, pushu na `main` i ręcznie
      (`workflow_dispatch`) — push na każdą gałąź dublował przebiegi z PR (zmiana po pierwszym pushu autora); osobne kroki „Unit tests” (131) i „Integration test” (6);
      jawna ścieżka pamięci podręcznej pip (`requirements-dev.txt` + `packages/*/pyproject.toml` — domyślnie `setup-python`
      szuka `requirements.txt`, którego nie ma); anulowanie starszego przebiegu tej samej gałęzi.
      **Symulacja w czystym kontenerze** (`python:3.11` + Java 17, tylko pliki widoczne dla gita, kroki jak w `ci.yml`):
      instalacja 2 min, ruff czysto, **137/137 w 4,5 min**, 9 wheeli zbudowanych.
- [ ] CI: potwierdzić zielony przebieg na GitHub Actions (push autora; link do przebiegu).
- [x] Pełny `README.md` (pytanie, wynik K1–K4, diagram architektury, uruchomienie, testy/CI, deploy, wymagania kursu →
      gdzie spełnione, ograniczenia, źródła), nowe `docs/HOWTOREAD.md` (jak czytać repo, anatomia kroku, mapa logiki,
      jak czytać tabele gold, testy-strażnicy, typowe zmiany), nowe `docs/przygotowanie-danych.md` (źródła, formaty,
      czas, pułapki, dane syntetyczne), `docs/ARCHITECTURE.md` (stan, sekcje 9 „Testy i CI” i 10 „Środowiska i wdrożenie”).
- **Akceptacja (zmieniona z autorem):** CI zielone z testem integracyjnym na danych syntetycznych (bez internetu).

- Commity do odtworzenia: `chore: remove legacy radplume modules`, `refactor(core): GIOŚ file format labels as a shared contract`,
  `fix(bronze): metadata rows shorter than the header`, `feat(ingest): deterministic synthetic inputs in source formats`,
  `test: end-to-end integration on synthetic data`, `docs: data-quality demo script`,
  `ci: separate unit and integration steps, manual runs`,
  `docs: README, how-to-read and data preparation for smogcast`, `docs: architecture — tests, CI and environments`

### E9. Databricks (wymagania kursu) ☐

**Przed pierwszym deployem:** lista poprawek i punktów do sprawdzenia w [`docs/migracja-databricks.md`](docs/migracja-databricks.md)
(m.in. `pyspark`/`delta-spark` w zależnościach `smogcast-core`, tożsamość Jobów). Próbny deploy na DEV warto zrobić wcześnie.

- [x] **Poprawki przed deployem (P1.1–P1.3) + skrypt startowy UC** — 2026-10-06, opis w
      [`docs/poprawki-przed-deployem.md`](docs/poprawki-przed-deployem.md). Zostało do wpisania przez autora: adres
      workspace'u DEV (i PROD), id service principala (przed PROD).
- [x] Próbny deploy na DEV (`bundle validate/deploy -t dev`, task `ingest gios-registry`, Job batch, Job na żywo) — ✅ 2026-10-06
      (konto trial autora; do odtworzenia na nowym koncie — patrz ustalenia niżej).

**Ustalenia autora (2026-10-06):**
- **DEV = workspace na koncie autora** (`adb-7405617820175624`, Germany West Central), **PROD = workspace na koncie
  kursu** (inny tenant/subskrypcja, inny metastore). Ten sam kod i bundle, różne targety; per target: adres, typ maszyny
  (`var.node_type`), katalog i lokalizacja (`conf/<env>.yaml`, bootstrap), tożsamość (`run_as`), sekrety CI. Dane i model
  liczone osobno na każdym środowisku (różne metastore'y).
- **Infrastruktura Azure w osobnym repo, Terraform** (workspace'y, Key Vault, service principal, magazyn, uprawnienia; provider
  Databricks do katalogów i secret scope). To repo dostaje z niego tylko wartości wyjściowe (host, id principala, katalog).
- **Kolejność: najpierw pełny, działający DEV**; jak wdrożyć PROD na koncie kursu — autor ustali z prowadzącym (wg
  prowadzącego: pełne uprawnienia w workspace'ie Databricks; uprawnienia na Azure — nieznane).
- **Panel/asystent hostowany na Azure** (poza Jobami), czyta gold przez SQL Warehouse — wheel `smogcast-app` wyjęty z bundla.
  Autor ma zastrzeżenia do tego, **co aplikacja pokazuje** — do omówienia później (przed pracą nad hostingiem).
- **Trial Azure autora kończy się ok. 2026-10-10** → DEV powstanie od zera na nowym koncie (nowy workspace, metastore,
  adres, lokalizacja katalogu); tam też stan Terraforma **dla DEV i PROD**. Na obecnym koncie tylko walidacja potoku
  (Job batch, Job na żywo), **bez `terraform apply`**. Wspomniane autorowi: alternatywa = przejście triala na
  pay-as-you-go (zasoby zostają). Do decyzji: moduł workspace'u w Terraformie dla nowego DEV.
- **Pytania do prowadzącego (PROD na koncie kursu):** nazwa istniejącego Key Vaulta kursu i czy jest już secret scope
  w workspace'ie (moduł `secrets` → wariant „istniejący vault”); czy można tworzyć service principal w Entra ID kursu;
  czy jest administrator konta Databricks (grupy RLS/CLS); region, nazwa katalogu i lokalizacja; dostęp CI (OIDC).
  Stan PROD w magazynie na koncie autora wymaga `tenant_id` per środowisko w providerach Terraforma.

Wymagania z `final-project-spec.md` — każde musi mieć „dowód” na demo:
- [ ] Unity Catalog: katalogi `smogcast_dev` / `smogcast_prod`, schematy bronze/silver/gold/ops, volume `raw`.
- [ ] RLS (województwo) i CLS (np. ukrycie `source_file` dla roli analityka) — `sql/governance.sql`.
- [ ] Sekrety w Key Vault (secret scope) — nawet jeśli API GIOŚ nie wymaga klucza, np. token do Databricks/LLM.
- [ ] **Deklaratywny pipeline Lakeflow** (ścieżka B: Auto Loader + expectations) i **Lakeflow Job** (ścieżka A, task na krok).
- [ ] Asset Bundle (`databricks.yml`) z targetami dev/prod; CI/CD (GitHub Actions) deploy DEV → PROD bez ręcznych kroków.
- [ ] Dashboard AI/BI na gold: „jutro w miastach”, „dni z przekroczeniem w sezonie”, „jak dobry jest model vs persystencja”, „kalibracja”.
- [ ] Aplikacja AI: text-to-SQL (Foundation Model API) na gold + RAG (Vector Search) na dokumentach z E7, te same guardrails.
- [ ] Zdolność zaawansowana: **CDC** (E6) — już spełnia wymaganie.
- **Akceptacja:** deploy z CI na PROD, dashboard i aplikacja działają na PROD.

- Commity do odtworzenia: `build(core): pyspark and delta-spark only in the local extra`,
  `feat(bundle): job identity (single-user owner, PROD run_as)`, `feat(bundle): Unity Catalog bootstrap script`,
  `docs: pre-deploy fixes`, `fix(bundle): single-node DC4as_v5 job clusters, node type per target`,
  `chore(bundle): sync only sql/, no app artifact`, `fix(ingest): retry Open-Meteo responses that are not JSON`,
  `fix(model): reproducible GBT across environments (single ordered partition before fit)`,
  `fix(model): round model inputs so float noise cannot change the trees`,
  `feat(bundle): per-step wheel tasks in Lakeflow Job`, `feat(pipeline): declarative live pipeline`, `feat(governance): UC, RLS/CLS`, `ci: deploy DEV to PROD`, `feat(dashboard): gold dashboard`, `feat(app): text-to-SQL + RAG`

### E10. Demo ☐

- [ ] Write-up: decyzje, koszty/wydajność (w tym koszt wielu wheeli, D14), gdzie AI pomogło + guardrails,
      **historia zmiany tematu** (sekcja 1).
- [ ] Scenariusz demo 20–30 min + próba generalna.

## 7. Mapa plików

| Obecnie | Los | Uwagi |
|---|---|---|
| `core/` (config, session, storage, dq_metrics) | **→ `packages/core`** | tłumaczenie komentarzy (D15) |
| `pipelines/context.py` | → `packages/core` | |
| `pipelines/batch.py`, `stream.py`, `cli.py` | → `packages/cli` (przepisane) + `main.py` w każdej paczce | orkiestracja lokalna; na Databricks orkiestruje Job |
| `app/guardrails.py` | → `packages/app` | nowa biała lista tabel |
| `app/city_query.py` | → `packages/app`, przeróbka (E7) | wzorzec odpowiedzi + SQL zostaje |
| `ingest/meteo.py` | → `packages/ingest`, przeróbka (E3) | + źródło `historical_forecast`, + bieżąca prognoza |
| `ingest/cities.py`, `bronze/cities.py` | usunąć | miasta z konfiguracji, stacje z rejestru GIOŚ |
| `bronze/meteo.py`, `silver/meteo.py` | przeróbka (E3) | konwencje wiatru i test zostają; Pasquill opcjonalnie jako cecha |
| `silver/sensor_clean.py`, `silver/sensor_quality.py` | przeróbka (E4/E6) | wzorce: late/dedup/kwarantanna/zamrożenie |
| `silver/devices_cdc.py` | przeróbka (E6) | SCD2 dla stacji |
| `gold/live_alerts.py` | przeróbka → `gold/forecast.py` + `gold.dq_summary` | |
| `silver/dispersion.py`, `silver/puff.py`, `silver/scenarios.py`, `silver/events.py`, `silver/grid.py`, `pipelines/event.py`, `gold/aggregates.py`, `validation/`, `bronze/source_term.py`, `bronze/meteo_obs.py` | **usunąć** | fizyka radplume; zostaje w tagu `radplume-final` |
| `simulators/sensor_sim.py` | **→ `packages/ingest` jako `fault_replay.py`**, przeróbka (E6) | logika psucia zostaje, wartości z prawdziwych pomiarów zamiast z modelu |
| `simulators/device_registry.py` | → `packages/ingest` (rejestr stacji: migawki z API; tryb offline: zmiany wstrzykiwane jak dotąd) | CDC |
| `silver/sensor_quality.py` | → `packages/silver_clean`, przeróbka | zamrożenie, dryf vs **inne stacje miasta**, sąsiedzi, warm-up |
| `conf/sensor_dq.yaml` | → `conf/pm_dq.yaml` + `conf/fault_injection.yaml` | progi DQ i harmonogram usterek |
| `conf/physics.yaml`, `conf/sites.yaml`, `conf/cities_fallback.csv` | usunąć/zastąpić | nowe pliki z E1 |
| `scripts/convert_*.py` (Katata, NRA, AMeDAS) | → `docs/archive-radplume/scripts/` | historia walidacji radplume |
| testy fizyki/scenariuszy/zdarzeń/walidacji | usunąć | zostają: config, guardrails, storage, meteo (konwencje), cdc (przerobione) |
| `Dockerfile`, `docker-compose.yml`, CI | zostają, przeróbka | instalacja wielu paczek; build wheeli w CI; + `openpyxl` |
| `pyproject.toml` (główny) | zostaje | tylko ruff/pytest; projekty w `packages/*/pyproject.toml` |
| `data/`, `data-offline/` | lokalne, git-ignored | przed E2 można usunąć (`data/` zawiera dane Fukushimy) |
| `legacy/radplume/` | ✅ **usunięte w E8** | moduły przerobione w E3–E7 |
| `docs/archive-radplume/` | zostaje | stare README, ARCHITECTURE, HOWTOREAD, przygotowanie danych, plan radplume (do write-upu) |

## 8. Harmonogram

| Dni | Etapy | Wynik dnia |
|---|---|---|
| 1 (01.10) | plan, E0, E1 | szkielet `smogcast`, testy zielone |
| 2–3 | E2, E3 | historia PM i pogody w bronze/silver |
| 4–5 | E4, E5 | cechy, model, **werdykt backtestu** |
| 6–7 | E6, E7 | strumień na żywo, `ask`, CDC |
| 8 | E8 | dokumentacja, CI, offline |
| 9–14 | E9 | Databricks: UC, Lakeflow, Job, bundle, CI/CD, dashboard, RAG |
| 15–17 | E10 + bufor | write-up, próba demo |

Punkt kontrolny: **koniec dnia 5** — jeśli model nie pokonuje persystencji, patrz plan B (sekcja 9).

## 9. Ryzyka i plan B

| Ryzyko | Sygnał | Plan B |
|---|---|---|
| archiwum GIOŚ zmieni linki / niedostępne | błąd pobierania | `archivalData/getDataBySensor` z API (sekcja 4.2), wolniej, ale działa |
| xlsx za duże dla pamięci (openpyxl) | OOM w kontenerze | `read_only=True` + strumieniowe iterowanie wierszy, zapis CSV porcjami |
| model nie pokonuje persystencji | BSS ≤ 0 vs persystencja | to też jest wynik (uczciwość w rubryce); dodać cechy: kierunek wiatru, inwersja (różnica temperatur 2 m vs 850 hPa z Open-Meteo `temperature_850hPa`), święta; nie stroić na zbiorze testowym |
| za mało dni z przekroczeniem PM2.5 przy progu 25 | < 5% dni | raportować PM2.5 z progiem WHO 15 albo skupić się na PM10 |
| API bieżące zwraca `null` / opóźnienia | puste batch'e | kwarantanna + metryka; prognoza jutro z ostatniej ważnej doby (flaga `degraded`) |
| brak czasu na E9 | dzień 12 bez deployu | priorytet: bundle + Job + dashboard; RAG w minimalnej wersji (kilka dokumentów) |

## 10. Dziennik postępu

Format wpisu: `RRRR-MM-DD — etap — co zrobiono — wynik/werdykt — następny krok`.
Wpisy najnowsze na dole.

- 2026-10-01 — plan — Zweryfikowano źródła danych (sekcja 4): archiwum GIOŚ (id plików, format xlsx,
  czas CET), API v1 (stare API = 410), archiwum prognoz Open-Meteo od 2021 (bez `boundary_layer_height`).
  Stan repo: niezacommitowane zmiany radplume (walidacja, AMeDAS) — do commitu w E0. — Następny krok: E0
  (po zgodzie autora na commit) i decyzje D1, D2.
- 2026-10-01 — decyzje — Autor potwierdził: nazwa `smogcast` (D1), 10 największych miast (D2, sprawdzone
  w API), osobny wheel na każdy krok (D14), kod i komentarze po angielsku (D15), bez commitów lokalnie —
  listy commitów do odtworzenia w każdym etapie (D16). — Następny krok: E0 (kopia), E1 (szkielet multi-wheel).
- 2026-10-01 — E0 ✅ — Kopia radplume w `C:\Users\Leszek\DEMO RADPLUME\radplume-final\` (1,8 MB, bez danych,
  `.venv` i cache). Autor: zachować logikę psucia odczytów czujników → D10 zmieniona: wstrzykiwanie
  usterek do prawdziwych pomiarów (`fault_replay`), E6 rozszerzony. — Następny krok: E1.
- 2026-10-01 — E1 ✅ — Monorepo z 9 paczkami (`packages/*`, przestrzeń nazw `smogcast.*`, wersja 0.1.0 w `VERSION`),
  `smogcast-core` (config, sesja, storage, DQ, `runner.make_main`), 6 wheeli krokowych ze stubami kroków,
  `smogcast-app` (guardrails po angielsku, nowa biała lista gold), `smogcast-cli` (`smogcast steps|run|run-batch|run-live|run-fault-demo|show|ask`).
  Kod radplume do przeróbki w `legacy/radplume/` (lista w `legacy/README.md`), stare dokumenty w `docs/archive-radplume/`.
  Weryfikacja w Dockerze: ruff czysto, **41 testów OK**, 9 wheeli się buduje, instalacja wheeli w czystym venv działa
  (namespace OK, YAML w wheelu core, `load_config('dev')` z wheela). — Następny krok: E2 (archiwum GIOŚ → bronze.pm_hourly).
- 2026-10-01 — E2 ✅ — Archiwum GIOŚ 2021–2025 (~320 MB) → `bronze.pm_hourly` (11,4 mln wierszy, ~2,5 min lokalnie),
  `bronze.gios_stations`/`gios_positions` z metadanych. 54 testy OK. Błąd znaleziony na prawdziwych danych:
  puste napisy jako daty w arkuszu STANOWISKA → `try_cast` + puste napisy jako NULL. Utworzono żywy
  `docs/ARCHITECTURE.md` (diagramy mermaid ze statusami) — aktualizować po każdym etapie. — Następny krok: E3 (pogoda).
- 2026-10-01 — E3 ✅ — Odkrycie: Open-Meteo „historical forecast” to świeże prognozy (kilka h), a prawdziwe prognozy
  z poprzedniego dnia (Previous Runs API) istnieją od 2024-01-19 → trening na short_range, walidacja/test na day_ahead.
  Pogoda w bronze/silver dla 10 miast; 65 testów OK. Kryteria K1–K4 zamrożone. — Następny krok: E4 (czyszczenie PM,
  średnie dobowe, cechy; pamiętać o 8 kodach stacji spoza metadanych — patrz E2).
- 2026-10-01 — E4 ✅ — Czyszczenie, średnie dobowe i cechy na prawdziwych danych; 74 testy OK. Archiwum bardzo czyste
  (0 w kwarantannie, 0,1% flag) — potwierdza D17 (usterki do demo wstrzykujemy w E6). Klasy niezrównoważone
  (PM10 ~5–8% dni z przekroczeniem). — Następny krok: E5 (baseline'y, model, backtest, kryteria K1–K4).
- 2026-10-01 — E5 ✅ — Prawdziwy backtest 2025: **PM2.5 spełnia kryteria (K1–K3)**, **PM10 nie spełnia K3
  (kalibracja 21 p.p.) i K4a (POD 47%)**; oba modele wyraźnie lepsze od klimatologii i persystencji (BSS 0,24 / 0,35,
  AUC 0,94). Logistyczna lepsza od GBT dla PM10 na teście — nie zmieniamy (wybór był na walidacji). 79 testów OK.
  — Następny krok: decyzja autora o E5b (nowy holdout 2026) albo przejście do E6.
- 2026-10-01 — decyzja — Autor wybrał wariant B: przejście do E6; E5b (kalibracja PM10, próg wybrany na walidacji,
  holdout 2026) odłożone „jeśli zostanie czas”. Zapisano wyjaśnienie, dlaczego PM10 przewiduje się gorzej (E5).
- 2026-10-01 — E6a ✅ — Rejestr stacji: migawka API → `bronze.station_snapshots` → `silver.station_changes` (CDC z różnic
  migawek) → `silver.stations` (SCD2). Ustalono: API GIOŚ (`getData`, `archivalData`) stempluje **koniec godziny w czasie
  lokalnym** (sprawdzone: archivalData = wartości i znaczniki archiwum dla dnia zimowego) → `timeutil.local_hour_end_to_utc_start`.
  `archivalData` działa — możliwe źródło holdoutu 2026 dla E5b. — Następny krok: E6b.
- 2026-10-01 — E6a–E6d ✅, E6e 🚧 — Prawdziwe demo usterek (Kraków 17–21.01.2025): 4 spóźnione (lag 4,3–7,3 h),
  kwarantanna below_min/above_max/missing_value, 1 spike, 9 h frozen, dryf wykryty po ~25 h tylko na stacji z dryfem,
  **prawdziwy smog 20.01 (mediana ~104 µg/m³) bez flag**, CDC: 9 INSERT + 1 UPDATE + 1 DELETE. Live: 36 stacji, 3,5 tys.
  godzin. Poprawki po demo: liczniki przed MERGE (`localCheckpoint`), mapowanie stacji z rejestru API (nowe stacje),
  ponowna ocena uzupełnionych pustych wartości. **Przerwano z powodu limitu kontekstu — stan i dalsze kroki w `HANDOFF.md`.**
- 2026-10-01 — E6e, E6f ✅, **E6 ✅** — Pełny zestaw testów na start: 97/97. Nowe kroki: `silver-clean live-reset`,
  `silver-transform features-live`, `model forecast` (→ `gold.forecast_tomorrow`), `gold dq-summary`; `run-live` i
  `run-fault-demo` w CLI, Job `resources/job_smogcast_live.yml`, test zgodności Jobów z CLI. Reset i ponowne czyszczenie
  prawdziwego strumienia ujawniły **dwa błędy** (poprawione, z testami): (1) kilka odpytań w jednej mikro-paczce →
  ponowna wysyłka klasyfikowana jako „spóźniona” (3654 fałszywych odrzuceń) — teraz decyduje pierwsze nadejście klucza,
  wartość = najnowsza; (2) po resecie checkpointu `batch_id` startuje od 0, a Delta po cichu pomijała
  `append_idempotent` z tym samym `txnAppId` → puste `bronze.pm_stream`, kwarantanna i spóźnione (dotyczyło też
  `fault-reset`) — `txnAppId` zawiera teraz id zapytania strumieniowego z checkpointu. Po poprawkach: 4 odpytania
  przeczyszczone od nowa w jednej paczce → 0 spóźnionych, 170 w kwarantannie (= puste wartości ostatniego odpytania),
  4028 przyjętych, 0 `unknown_station`.
  `run-live` na prawdziwych danych (wydanie 2026-10-01, prognoza na 2026-10-02): `silver.features_live` 20/20 wierszy
  użytecznych, `gold.forecast_tomorrow` 20/20 z prawdopodobieństwem (model GBT `final` dla obu zanieczyszczeń), jedno
  ostrzeżenie: **Wrocław PM10 P = 0,51** (jedyna stacja PM10 miasta, Al. Wiśniowa: wczoraj 49,1 µg/m³ przy normie 50,
  rano 43,8, prognoza 9 h ciszy wiatrowej); dalej Kraków PM10 0,48, Gdańsk PM2.5 0,34; reszta ≤ 0,25. Kolejne odpytanie:
  27 nowych godzin, 0 dubli w silver. `gold.dq_summary` (live): 20 650 rekordów = 4198 unikalnych odczytów
  (reszta to ponowne wysyłki API), 4,05% w kwarantannie (puste wartości), 2 `spike`, 96,55% ważnych.
  **Ograniczenie do write-upu:** zbiór stacji na żywo różni się od historii (nowe stacje z rejestru, np. Warszawa 8 vs 6
  stacji PM10, Lublin 2 vs 1) — „miasto = najgorsza stacja” (D4) przy większej liczbie stacji systematycznie podnosi
  maksimum, czyli cechy na żywo mogą być nieco wyższe niż w treningu. Do rozważenia (poza PoC): cechy liczone tylko na
  stacjach obecnych w historii. — Następny krok: E7 (aplikacja `ask`).
- 2026-10-01 — E6 weryfikacja końcowa — `run-fault-demo` od zera (z `fault-reset`, po poprawce `txnAppId`): 4 spóźnione,
  kwarantanna `missing_value` / `below_min` / `above_max`, 1 `spike`, 9 h `frozen`, 26 h `drift`, CDC 9 INSERT + 1 UPDATE
  + 1 DELETE — ten sam wynik co pierwsze demo E6d, więc reset działa. Pełny zestaw: ruff czysto, **105/105 testów OK**.
- 2026-10-01 — E7 ✅ — Panel + asystent w Streamlit na wzór szkieletu RAG autora. Korpus: 2 dokumenty projektu po
  angielsku (`docs/rag/`, bezosobowo, bez etapów i historii zmiany tematu — oryginały robocze zostają po polsku) +
  rozporządzenie PL o poziomach (informowania 100, alarmowy 150 µg/m³ — sprawdzone w PDF), dyrektywy 2008/50 i
  2024/2881, strona GIOŚ z zaleceniami, fact sheet WHO; 458 fragmentów. EUR-Lex blokuje automaty (AWS WAF) — akty UE
  pobierane z repozytorium Urzędu Publikacji (Cellar, identyfikatory z SPARQL; te same pliki). Pełne wytyczne WHO 2021
  niedostępne automatycznie (captcha) — opisane w `docs/rag-dokumenty.md`. Ocena na `qwen2.5-7b`: 7/8 po poprawkach
  (wyniki wyżej, E7). — Następny krok: E8 (dokumentacja, testy integracyjne, tryb offline, usunięcie `legacy/`).
- 2026-10-01 — E7 (zmiana) — Na prośbę autora **cała aplikacja po angielsku**: interfejs, odpowiedzi asystenta,
  komunikaty CLI, prompty („answer in English even if the question is in another language”), baza wiedzy. Polskie
  źródła bez oficjalnych wersji angielskich (rozporządzenie Dz.U. 2021 poz. 845, strona GIOŚ — sprawdzone: `?lang=en`
  tłumaczy tylko menu) → nieoficjalne tłumaczenia fragmentów o pyle w `docs/rag/` (z tekstu PDF/strony, nie z pamięci);
  oryginały pobierane z `index: false`. Indeks: 447 fragmentów, tylko po angielsku. Ocena: 9/9 (E7). Zasada językowa
  zaktualizowana w `CLAUDE.md` i D15. 123 testy OK.
- 2026-10-01 — przekazanie — Autor obejrzał panel: wygląd ogólnie OK, ale **ma kilka uwag do interfejsu — do omówienia
  na początku następnej sesji, przed E8** (szczegóły nieznane; nie zgadywać). HANDOFF.md przepisany pod kolejnego agenta.
- 2026-10-02 — E7 (uwagi autora) — Ustalono zakres U1–U4 (tabela w E7): zakładka Info na początku nawigacji, opis
  kalibracji na stronie Model, poprawka uciętej tabeli K1–K4. Wyjaśniono autorowi: model **nie** jest typu Monte Carlo
  (GBT, jedno prawdopodobieństwo z cech); model uczony na **archiwum** GIOŚ 2021–2025, a API na żywo daje tylko cechy
  do codziennej prognozy; `fault_replay` dowodzi, że czyszczenie strumienia działa przewidywalnie — ten sam kod czyści
  prawdziwy strumień `live` (puste wartości, ponowne wysyłki, skoki).
- 2026-10-02 — E7 (uwagi autora) ✅ — U1–U4 wprowadzone: strona `Info` (pierwsza w menu; progi, lata i ustawienia
  modelu z konfiguracji, werdykt K1–K4 z `gold.acceptance`), opis wykresu kalibracji z przykładem z `gold.reliability`
  (PM10: 70–80% → wystąpiło 55%, 21 p.p.; PM2.5: 80–90% → 74%, 12 p.p.), tabela K1–K4 jako zawijana tabela HTML
  prostym językiem, wykres Briera bez ucinanego podpisu osi. Wyjaśniono też autorowi: lokalnie potok na żywo nie działa
  sam — `run-live` uruchamia się ręcznie; odpytywanie co godzinę to Job Lakeflow (E9, harmonogram wstrzymany).
  Testy: ruff czysto, paczka app 36/36, test bezgłowy 6 widoków bez wyjątków, panel zrestartowany. — Następny krok: E8.
- 2026-10-06 — E9 (przygotowanie) — Na prośbę autora poprawki przed pierwszym deployem, przed E8: (P1.1) `pyspark` i
  `delta-spark` tylko w dodatku `smogcast-core[local]` (Dockerfile, CI) + test; sprawdzone: wheel kroku instaluje się
  w czystym venv bez PySparka; (P1.2) `single_user_name: ${workspace.current_user.userName}` w klastrach obu Jobów,
  `run_as` service principal w targecie PROD; (P1.3) adresy workspace'ów oznaczone do wpisania. Znaleziona niespójność:
  ścieżki `/Volumes/<katalog>/raw/{landing,checkpoints,models}` wymagają **schematu** `raw` z trzema Volume'ami, a
  dokumentacja mówiła o „volume `raw`” → `sql/bootstrap_uc.sql`. Opis: `docs/poprawki-przed-deployem.md`.
  Obraz przebudowany; ruff czysto, **127/127 testów**, panel działa na nowym obrazie.
- 2026-10-06 — E9 (DEV) — Bootstrap UC na workspace'ie DEV (`adb-7405617820175624`; metastore bez domyślnej lokalizacji
  → `MANAGED LOCATION` w lokalizacji zewnętrznej workspace'u), `bundle validate` + `deploy -t dev` OK. Pierwszy task padł
  przed startem kodu: brak wolnych `Standard_D4ds_v5` w Germany West Central (`CLOUD_PROVIDER_RESOURCE_STOCKOUT`) → oba Joby
  na `Standard_DC4as_v5` (rodzina klastra kursu), Job batch jednomaszynowy, typ maszyny jako zmienna `node_type` per target.
  `sync.paths: [sql]` (deploy usunął 165 zbędnych plików), wheel aplikacji wyjęty z bundla. **`ingest_gios_registry` na DEV
  ✅** (53 stacje, 354 stanowiska, migawka na Volume; 13 min, w tym ~12 min start klastra). Ustalenia autora: DEV na jego
  koncie, PROD na koncie kursu, infrastruktura w osobnym repo Terraform (`smog-cast-terraform`). — Następny krok: Job batch na DEV.
- 2026-10-06 — E9 (DEV, Job batch) — Ścieżka PM na DEV ✅ (archiwum → bronze → czyszczenie → średnie dobowe). Padło
  `ingest_weather_history`: po ~35 plikach Open-Meteo zwróciło treść niebędącą JSON-em, której `fetch` nie ponawiał →
  poprawka (`NotJsonResponse` ponawiane, 6 prób 5–80 s, 2 testy, ingest 25/25), deploy, „repair run” nieudanych i zależnych
  tasków. Repo `smog-cast-terraform`: moduły `unity_catalog`, `secrets`, `identity`, `governance`, środowiska `dev`
  (z importem obiektów z bootstrapu SQL) i `prod` (szkielet); `terraform validate` OK (Terraform 1.16, azurerm 4.81,
  azuread 3.10, databricks 1.136) — **nie uruchamiane** (`apply` po przejściu Joba batch).
- 2026-10-06 — E9 (DEV, Job batch ✅ po naprawie) — Werdykty K1–K4 na DEV takie same jak lokalnie (PM10 ❌ K3, K4a;
  PM2.5 ✅). **Odkrycie: GBT niepowtarzalny między środowiskami.** Regresja logistyczna dała na DEV te same Briery
  walidacyjne co lokalnie co do cyfry (dane identyczne), GBT — różne o do 0,002 (mimo `seed`): Spark losuje progi
  podziałów drzew z próbki zależnej od podziału danych na partycje (Docker ≠ klaster). Dla PM2.5 GBT i logistyczna są
  praktycznie na remis, więc zmienił się model operacyjny (lokalnie GBT 0,07473, na DEV logistic 0,07505). **Poprawka
  powtarzalności** (zgoda autora): `train.fit_input` — dane treningowe w jednej partycji i stałym porządku przed każdym
  `fit` (dobór, `tuned`, `final`); test `test_gbt_is_the_same_whatever_the_partitioning` (bez poprawki nie przechodzi).
  To nie strojenie: reguła wyboru i K1–K4 bez zmian, ale **liczby GBT z E5 trzeba przeliczyć** (lokalnie i na DEV).
- 2026-10-06 — E9 (powtarzalność, ciąg dalszy) — Po `fit_input` lokalnie i na DEV wybrano te same warianty (PM10 GBT gł. 3/100,
  PM2.5 GBT), ale Briery nadal się różniły (np. PM10 GBT gł. 5/100: 0,04049 vs 0,03963), a **werdykt K3 dla PM10 się
  rozjechał** (lokalnie 20,6 p.p. ❌, DEV 12,9 p.p. ✅). Eksperyment na prawdziwych danych: zaburzenie cech o 2e-16
  zmienia Brier walidacyjny GBT PM10 z 0,03970 na 0,03922 (skala różnicy Docker–DEV), po zaokrągleniu cech oba dają
  0,03883 → przyczyna: szum zmiennoprzecinkowy (inna kolejność sumowania średnich), na który drzewa są czułe przez
  wybór progów. **Poprawka:** `prepare` zaokrągla wejścia modelu do 6 miejsc (`ROUND_DECIMALS`) + test. Wniosek do
  write-upu: **K3 dla PM10 jest kruche** — leży blisko progu 15 p.p. i zależy od przedziałów z ok. 30 dniami.
- 2026-10-06 — E9 (powtarzalność ✅) — Z zaokrągleniem cech laptop i DEV dają **identyczne** wyniki (wszystkie 16
  Brierów walidacyjnych, wybór modeli, K1–K4). Wyniki obowiązujące (E5, ramka „Aktualizacja”): PM10 GBT gł. 3/100 —
  Brier 0,0434, AUC 0,95, POD 44%, FAR 32%, K3 8,5 p.p. ✅ (kruche), **K4a ❌ → PM10 nadal nie spełnia kryteriów**;
  PM2.5 GBT gł. 5/50 — Brier 0,0751, K1–K3 ✅. Zaktualizowane: plan, HANDOFF, ARCHITECTURE, panel (strona Model),
  baza wiedzy (`docs/rag/smogcast-project.md`, indeks przebudowany), `docs/ulepszenia-pm10.md`.
- 2026-10-06 — E9 (DEV ✅) — **Pełny Job na żywo na DEV**: 11/11 tasków, 12,2 min (z czego ok. 7–8 min start klastra).
  Strumień: 4488 unikalnych odczytów, 288 w kwarantannie (6,4%, puste wartości), 0 spóźnionych, 4200 przyjętych;
  `silver.features_live` 20/20 użytecznych; `gold.forecast_tomorrow` 20/20 (wydanie 2026-10-06 → 2026-10-07),
  4 ostrzeżenia: Kraków PM10 i PM2.5 (0,60), Poznań PM2.5 (0,57), Wrocław PM10 (0,54). **Walidacja potoku w chmurze
  zakończona**: Job batch (historia → model) i Job na żywo działają na Databricks z tymi samymi wynikami co lokalnie.
  Obserwacja kosztowa: przy Jobie co godzinę start klastra dominuje czas (argument za odpytywaniem API poza Databricks,
  wariant „Azure Function → pliki”). — Następny krok: nowe konto Azure (koniec triala ok. 2026-10-10), Terraform, E8/E9.
- 2026-10-06 — E8 🚧 — Zakres z autorem: tryb offline tylko jako dane do testów integracyjnych w CI. Usunięty `legacy/`.
  Generator `smogcast.ingest.synthetic` (pliki w formatach źródeł, wartości deterministyczne), `conf/synthetic.yaml`,
  nagłówki plików GIOŚ jako kontrakt w `smogcast.core.schema`. Test integracyjny całego potoku 6/6 (~3 min); wykrył
  błąd parsera metadanych w bronze (krótsze wiersze xlsx — poprawiony). Scenariusz `docs/demo-jakosc-danych.md`.
  **Pełny zestaw: ruff czysto, 137/137 testów** (w tym integracyjny). HANDOFF przepisany pod kolejnego agenta.
  — Następny krok: zielone CI na GitHub Actions, README / HOWTOREAD / przygotowanie-danych, potem nowe konto Azure i E9.
- 2026-10-06 — decyzja — **E5b porzucony** (decyzja autora): bez kalibracji PM10, bez zmiany progu ostrzeżenia, bez nowych
  cech — zostają obecne modele i werdykt (PM10 ❌; po poprawce powtarzalności: K4a, K3 na granicy). Potencjał poprawy PM10 (prognoza CAMS jako cecha, suchość
  nawierzchni, inwersja, kalendarz zdarzeń, typ stacji; kalibracja; ocena tylko na niewidzianym okresie 2026) opisany
  do wypowiedzi na demo w `docs/ulepszenia-pm10.md`.
  — Następny krok: autor wpisuje adres DEV, instaluje Databricks CLI, uruchamia bootstrap → próbny deploy na DEV.
- 2026-10-06 — E8 (dokumentacja, CI) — Pełny `README.md`, nowe `docs/HOWTOREAD.md` i `docs/przygotowanie-danych.md`,
  `docs/ARCHITECTURE.md` uzupełniony o testy/CI i środowiska. CI: PR i push na `main` + `workflow_dispatch` (push każdej gałęzi dublował przebiegi z PR — poprawione po
  pierwszym pushu autora), osobne kroki
  testów jednostkowych (131) i integracyjnego (6), jawna ścieżka pamięci podręcznej pip (bez `requirements.txt` krok
  `setup-python` mógłby się wywalić). **Symulacja CI w czystym kontenerze** (tylko pliki widoczne dla gita — wyłapałaby
  plik potrzebny testom, a ignorowany przez `.gitignore`): ruff czysto, **137/137 w 4,5 min**, 9 wheeli. Uwaga: w
  lokalnym repo nic ze smogcast nie jest zacommitowane (ostatni commit = radplume), więc CI na GitHubie zobaczy kod
  dopiero po commitach autora. — Następny krok: autor pushuje → link do przebiegu CI; nowe konto Azure i kolejność E9.
