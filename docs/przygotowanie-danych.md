# Przygotowanie danych: skąd, w jakim formacie i na co uważać

> **Dla kogo:** każdy, kto uruchamia smogcast lokalnie albo na Databricks, i każdy, kto będzie zmieniał kroki `ingest`
> i `bronze`. **Stan na:** 2026-10-06. Wszystkie formaty i pułapki niżej zostały sprawdzone na prawdziwych plikach
> (szczegóły i daty w [`PLAN_SMOG.md`](../PLAN_SMOG.md), sekcja 4 i dziennik).
> Poprzednia wersja tego dokumentu (radplume) leży w [`archive-radplume/`](archive-radplume/przygotowanie-danych.md).

**Najważniejsze:** niczego nie pobierasz ręcznie. Każde źródło ma swój krok `ingest`, który pobiera dane, sprawdza je
i zapisuje oryginał w strefie *landing*. Do testów i CI nie trzeba nawet internetu — generator syntetyczny zapisuje pliki
w tych samych formatach (sekcja 8).

---

## 1. Przegląd źródeł

| # | Dane | Po co | Krok, który pobiera | Landing | Ścieżka potoku |
|---|---|---|---|---|---|
| A | **Archiwum GIOŚ** — pomiary 1-godzinne PM10/PM2.5, roczne zip z xlsx (2021–2025) | historia: trening i backtest | `ingest gios-archive` | `gios_archive/raw/<rok>.zip` | A (batch) |
| B | **Metadane GIOŚ** — stacje i stanowiska (xlsx), także zamknięte | przypisanie stacji do miast, stare kody | `ingest gios-archive` | `gios_archive/raw/metadata.xlsx` | A |
| C | **Rejestr stacji z API GIOŚ** (`station/findAll` + `station/sensors`) | aktualne stanowiska do odpytywania; CDC/SCD2 | `ingest gios-registry` | `gios_registry/snapshot_<ts>.json` | C (CDC) |
| D | **Pomiary na żywo z API GIOŚ** (`data/getData`) | cechy dnia dzisiejszego do prognozy na jutro | `ingest gios-live` | `gios_live/batch_<ts>.json` | B (strumień) |
| E | **Archiwa prognoz pogody Open-Meteo** — `short_range` (od 2021) i `day_ahead` (od 2024-01-19) | cechy pogodowe „na jutro” w historii | `ingest weather-forecast-history` | `weather/<źródło>/<miasto>/<rok>.json` | A |
| F | **Prognoza pogody na żywo** (Open-Meteo) | cechy pogodowe do prognozy na jutro | `ingest weather-forecast-live` | `weather/live/<miasto>/<ts>.json` | B |
| G | **Odtworzenie z usterkami** — kopia prawdziwych pomiarów z `bronze.pm_hourly` + usterki | demo czyszczenia danych | `ingest fault-replay` | `fault_replay/`, `fault_replay_registry/` | B (osobny tor) |
| H | **Dokumenty do bazy wiedzy** (rozporządzenie, dyrektywy UE, GIOŚ, WHO) | asystent (RAG) | `ingest rag-documents` | `rag/_zrodlo/` | aplikacja |

Adresy, identyfikatory plików i listy zmiennych są w konfiguracji, nie w kodzie:
[`gios.yaml`](../packages/core/src/smogcast/core/conf/gios.yaml), [`weather.yaml`](../packages/core/src/smogcast/core/conf/weather.yaml),
[`cities.yaml`](../packages/core/src/smogcast/core/conf/cities.yaml), [`fault_injection.yaml`](../packages/core/src/smogcast/core/conf/fault_injection.yaml),
[`app.yaml`](../packages/core/src/smogcast/core/conf/app.yaml) (źródła RAG).

## 2. Gdzie lądują pliki

Lokalnie wszystko jest w `data/` w katalogu repozytorium (w Dockerze to ten sam folder — repozytorium jest zamontowane).
`data/` i `data-offline/` **nie trafiają do gita** (duże pliki, dane źródłowe mają własne zasady użycia).
Na Databricks te same podfoldery leżą na Volume'ach Unity Catalog — kod się nie zmienia, zmienia się tylko korzeń ścieżki
w `conf/<env>.yaml`.

```
data/                                   (Databricks: /Volumes/smogcast_<env>/raw/…)
├── landing/                            → …/raw/landing      surowe pliki ze źródeł, nigdy nie edytowane
│   ├── gios_archive/raw/               (A, B) <rok>.zip, metadata.xlsx — oryginały z GIOŚ
│   ├── gios_archive/long/              pamięć podręczna: <rok>_<PM10|PM25>_1g.csv.gz (format długi, sekcja 3)
│   ├── gios_registry/                  (C) migawki rejestru
│   ├── gios_live/                      (D) paczki odpytań API
│   ├── weather/{short_range,day_ahead,live}/<miasto>/   (E, F)
│   ├── fault_replay/, fault_replay_registry/            (G)
│   └── rag/_zrodlo/                    (H) oryginały dokumentów
├── delta/<warstwa>/<tabela>/           → tabele smogcast_<env>.<warstwa>.<tabela>   NIE edytuj ręcznie
├── checkpoints/                        → …/raw/checkpoints  stan strumieni           NIE edytuj ręcznie
└── models/                             → …/raw/models       zapisane modele MLlib, indeks RAG
```

Zasada z `CLAUDE.md`: jeśli kiedyś trzeba coś pobrać ręcznie, oryginał trafia do podfolderu `_zrodlo/`, a program czyta
dopiero plik przygotowany obok.

## 3. Archiwum GIOŚ (A) i metadane (B)

**Skąd:** https://powietrze.gios.gov.pl/pjp/archives → „Przygotowane dane do pobrania”, pobieranie
`…/pjp/archives/downloadFile/<id>`. Identyfikatory w `gios.yaml` (`yearly_file_ids`, `metadata_file_id`).

| Rok | id | | Plik | id |
|---|---|---|---|---|
| 2021 | 486 | | Metadane — stacje i stanowiska | 643 |
| 2022 | 524 | | | |
| 2023 | 564 | | | |
| 2024 | 582 | | | |
| 2025 | 644 | | | |

**Pułapka 1 — etykiety na stronie są przesunięte względem linków.** Link podpisany „2024” może prowadzić do innego
roku. Identyfikatory sprawdzono po nazwie pliku z nagłówka `Content-Disposition`, a krok `gios-archive` **sprawdza ją
przy każdym pobraniu** (w nazwie musi być żądany rok albo „Metadane”) i przerywa, zamiast zapisać zły plik. Przy
dodawaniu kolejnego roku: `curl -sI <adres>` i sprawdź `Content-Disposition`.

**Zawartość zip:** m.in. `2025_PM10_1g.xlsx`, `2025_PM25_1g.xlsx` (automatyczne, godzinowe) oraz `…_24g.xlsx`
(manualne, dobowe). **Używamy tylko `_1g`** — średnie dobowe liczymy sami (reguła 18 h). Uwaga na nazwę: w plikach
archiwum `PM25`, w API `PM2.5` (`archive_token` vs `gios_code` w `cities.yaml`).

**Format arkusza `<rok>_PM10_1g.xlsx` — szeroki:**

| Wiersz | Zawartość |
|---|---|
| 1–6 | nagłówek: `Nr`, `Kod stacji`, `Wskaźnik`, `Czas uśredniania`, `Jednostka` (`ug/m3`), `Kod stanowiska` |
| 7… | `datetime` + po jednej kolumnie na stanowisko (~185 kolumn PM10), ~8760 wierszy (8784 w roku przestępnym) |

- **Nagłówek znajdujemy po etykiecie** „Kod stanowiska”, nie po numerze wiersza. Polskie etykiety nagłówków to
  **kontrakt** w [`smogcast.core.schema`](../packages/core/src/smogcast/core/schema.py) — porównywane znak po znaku,
  więc **nie tłumaczyć ich** na angielski (z tego samego kontraktu korzysta generator syntetyczny).
- **Znacznik czasu = KONIEC godziny w CET** (UTC+1 przez cały rok, bez czasu letniego): `2025-01-01 01:00` to godzina
  00:00–01:00 CET.
- **Szum w znacznikach:** Excel dokłada ok. +5 ms na wiersz (do ~44 s pod koniec roku, np. `03:00:00.005`) →
  zaokrąglamy do **najbliższej** pełnej godziny (`timeutil.round_to_hour`).
- **Puste komórki = brak pomiaru** — pomijane i liczone w metrykach; przecinek dziesiętny i śmieci w komórkach obsłużone.
- **Pamięć:** arkusz ma ~1,6 mln komórek. Czytamy go strumieniowo (`openpyxl`, `read_only=True`, wiersz po wierszu)
  i zapisujemy do `gios_archive/long/<rok>_<PM>_1g.csv.gz` (`position_code,time_cet_end,value`) — dalej czyta to już Spark.
  Ten CSV to pamięć podręczna: usunięcie go wymusza ponowną konwersję z xlsx.

**Metadane (`metadata.xlsx`):** arkusze stacji i stanowisk. Zawierają **także stacje zamknięte** (miejscowość,
współrzędne, daty działania), których API już nie zna — bez nich nie przypisalibyśmy do miast starszych pomiarów.

- **Stare kody stacji:** stacje zmieniały kody; archiwum z danego roku używa kodu z tamtego czasu. Kolumna
  „Stary Kod stacji” może mieć **kilka kodów** naraz (rozdzielane przecinkiem lub średnikiem) — każdy dostaje własny
  wiersz w `silver.station_city`, wskazujący dzisiejszy kod.
- **Puste napisy zamiast dat** w arkuszu stanowisk → `try_cast` + pusty napis jako NULL.
- **Wiersze krótsze niż nagłówek** (puste końcowe komórki xlsx nie są zapisywane) — brakujące kolumny parser traktuje
  jako puste (błąd wykryty w E8 testem integracyjnym).
- **8 kodów z pomiarów nie ma w metadanych** — to stacje mobilne (`…MOB`) i małe miejscowości spoza 10 miast; bez wpływu
  na wynik.

**Skala:** 2021–2025 = **11,4 mln** wartości godzinowych (wszystkie stacje Polski), ok. 320 MB zip. Po zawężeniu do
10 miast: 149 stacji (+53 stare kody), 2,1 mln godzin. Archiwum jest zweryfikowane przez GIOŚ i bardzo czyste
(0 wartości ujemnych, 3 powyżej 1000 µg/m³, ok. 8 tys. zer) — stąd osobne demo usterek (G).

## 4. API GIOŚ v1 (C, D)

**Baza:** `https://api.gios.gov.pl/pjp-api/v1/rest` (dokumentacja: https://api.gios.gov.pl/pjp-api/swagger-ui/).
**Stare API** `/pjp-api/rest/...` zwraca **410 Gone** — w starszych poradnikach w internecie jest właśnie ono.

| Endpoint | Użycie | Uwagi |
|---|---|---|
| `GET /station/findAll?size=500` | rejestr stacji (C) | JSON-LD z **polskimi nazwami pól** (`Kod stacji`, `Nazwa miasta`, `WGS84 φ N`…), stronicowanie `page`/`size`; współrzędne jako **tekst** |
| `GET /station/sensors/{stationId}` | stanowiska stacji (C) | `Wskaźnik - kod` = `PM10` / `PM2.5` |
| `GET /data/getData/{idSensor}?size=500` | pomiary na żywo (D) | `Data` = **koniec godziny w czasie lokalnym** (Europe/Warsaw, **z czasem letnim**); wartości bywają `null` |
| `GET /archivalData/getDataBySensor/{idSensor}` | zapas dla archiwum | ta sama konwencja czasu co `getData` (sprawdzone: zimą zwraca dokładnie wartości i znaczniki archiwum) |
| `GET /levels/getPermissible` | weryfikacja progów | PM10 `standardValue` 50.5, `acceptNumber` 35 |

**Format paczki na żywo** (`gios_live/batch_<ts>.json`) — po jednej na odpytanie:

```json
{"source": "live", "fetched_at": "2026-10-01T14:25:26+00:00", "previous_fetched_at": "2026-10-01T14:19:12+00:00",
 "records": [{"position_code": "DsWrocWybCon-PM2.5-1g", "station_code": "DsWrocWybCon", "pollutant": "PM2.5",
              "sensor_id": 670, "time_local_end": "2026-10-01 16:00:00", "value": 16.4,
              "sent_at": "2026-10-01T14:25:26+00:00"}, …]}
```

Pułapki API:
- **`getData` zwraca ok. 3 ostatnie doby przy każdym odpytaniu.** Duplikaty są normą, a nie błędem — deduplikacja po
  (źródło, stanowisko, godzina). „Spóźniony” odczyt to nie każdy stary odczyt, tylko taki, który **powinien był
  przyjść już przy poprzednim odpytaniu** (stąd `previous_fetched_at` w paczce) i ma opóźnienie > 3 h.
- **API nie służy do historii** — trzyma za mało dni. Historia (Job batch) pochodzi z archiwum xlsx.
- **Puste wartości** w ostatnich godzinach są częste (4–6% odczytów na żywo) → kwarantanna `missing_value`, a przy
  kolejnym odpytaniu wartość uzupełniona przez GIOŚ jest oceniana ponownie.
- **Stanowiska manualne** (33 z 134 w 10 miastach) nie mają danych godzinowych na żywo — pomijane.
- **Zbiór stacji na żywo ≠ historia:** rejestr API zna nowe stacje (np. Warszawa 8 vs 6 stacji PM10). Przy definicji
  „miasto = najgorsza stacja” cechy na żywo mogą być nieco wyższe niż w treningu (ograniczenie opisane w ARCHITECTURE).

## 5. Pogoda — Open-Meteo (E, F)

Model musi widzieć **prognozę** pogody na jutro, nigdy pogodę, która faktycznie wystąpiła (decyzja D7) — inaczej
backtest byłby zawyżony. Stąd trzy źródła (`weather.yaml`):

| Źródło | API | Od | Użycie | Charakter |
|---|---|---|---|---|
| `short_range` | `historical-forecast-api.open-meteo.com/v1/forecast` | 2021 | **trening** 2021–2023 | sklejane najświeższe prognozy (kilka godzin wyprzedzenia) — trochę lepsze niż prawdziwa prognoza „na jutro” |
| `day_ahead` | `previous-runs-api.open-meteo.com/v1/forecast`, zmienne `<nazwa>_previous_day1` | **2024-01-19 12:00 UTC** | **walidacja 2024 i test 2025** | prawdziwe prognozy wydane dzień wcześniej; wcześniejsze godziny wracają jako `null` |
| `live` | `api.open-meteo.com/v1/forecast` | — | prognoza na jutro | najnowsza dostępna prognoza |

Zmienne godzinowe (te same we wszystkich źródłach): `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`,
`wind_direction_10m`, `precipitation`, `surface_pressure`, `cloud_cover`, `shortwave_radiation`.

Pułapki:
- **Jednostki i strefa wymuszane w żądaniu** (`wind_speed_unit=ms`, `timezone=GMT`) i **sprawdzane w odpowiedzi** —
  domyślnie Open-Meteo podaje wiatr w km/h. Sprawdzane są też długości tablic.
- **`boundary_layer_height` jest pusta** w archiwum prognoz — nie używamy (dlatego inwersja w
  `docs/ulepszenia-pm10.md` jest liczona z temperatur).
- Przyrostek `_previous_day1` jest zdejmowany w bronze → jeden schemat `bronze.weather_forecast` dla wszystkich źródeł.
- **Odpowiedź, która nie jest JSON-em** (zdarza się przy limicie zapytań — wyszło dopiero na Databricks, gdzie wszystkie
  pliki pobierały się od zera w jednym przebiegu; lokalnie leżały już w pamięci podręcznej): traktowana jak błąd
  przejściowy (`NotJsonResponse`), 6 prób z przerwami 5–80 s.
- **Pamięć podręczna:** zakończone lata nie są pobierane drugi raz; bieżący rok — za każdym razem.
- Różnica jakości źródeł w tych samych dniach jest mała (MAE temperatury dobowej 0,40°C, wiatru 0,21 m/s), więc trening na
  `short_range` niewiele zawyża.
- Dane Open-Meteo są na licencji **CC BY 4.0** — przy publikacji wyników trzeba podać źródło.

## 6. Czas — jedna konwencja w tabelach

Każde źródło stempluje godzinę inaczej. Konwersje są **tylko** w
[`smogcast.core.timeutil`](../packages/core/src/smogcast/core/timeutil.py) (z testami); w tabelach zawsze
`time_utc` = **początek** godziny w UTC, a doba średniej dobowej to `day_cet` (dzień kalendarzowy w CET — tak liczy GIOŚ).

| Źródło | Znacznik w źródle | Przykład → `time_utc` |
|---|---|---|
| archiwum GIOŚ | koniec godziny, **CET** (UTC+1 cały rok) | `2025-01-01 01:00` → `2024-12-31 23:00` |
| API GIOŚ (`getData`, `archivalData`) | koniec godziny, **czas lokalny** (lato: UTC+2) | `2026-10-01 16:00` (CEST) → `2026-10-01 13:00` |
| Open-Meteo | początek godziny, UTC (`timezone=GMT`) | bez zmian |

- **Jesienna zmiana czasu:** jedna godzina lokalna występuje dwa razy; `zoneinfo` bierze pierwsze wystąpienie — jedna
  niejednoznaczna godzina w roku, zaakceptowane.
- Dzień wydania prognozy „dziś” to dzień w CET (`timeutil.issue_day_cet`); zmienna `SMOGCAST_ISSUE_DATE` pozwala
  odtworzyć wybrany dzień w testach i na demo.
- Procesy działają w strefie UTC (`TZ=UTC` w Dockerze i CI, sesja Sparka w UTC) — tak samo jak na klastrze.

## 7. Stacje → miasta

Nie ma listy stacji na sztywno. Stacja należy do miasta, gdy jej miejscowość z metadanych GIOŚ (B) albo `Nazwa miasta`
z rejestru API (C) jest równa `gios_city_name` z `cities.yaml`. Nowe stacje pojawiają się więc same. Każde z 10 miast
ma kod województwa (`jurisdiction_code`, np. `PL-12`) — to klucz pod RLS (decyzja D12).

## 8. Dane syntetyczne (testy, CI, `--offline`)

[`smogcast.ingest.synthetic`](../packages/ingest/src/smogcast/ingest/synthetic.py) zapisuje **te same pliki co prawdziwe
źródła** (zip z arkuszami xlsx GIOŚ, metadane, migawki rejestru, paczki `getData`, JSON-y Open-Meteo dla trzech źródeł),
więc kroki `bronze` i dalsze nie wiedzą, że dane są sztuczne. Włączane przez `sources: synthetic` (konfiguracja testów)
albo `smogcast --offline …` (osobny katalog `data-offline/`, 2 miasta, małe siatki modelu).

- Wartości są czystymi funkcjami (ziarno, stacja, czas) — każdy przebieg daje to samo.
- Jedna ukryta „prawdziwa pogoda” na miasto; PM od niej zależy, prognozy = prawda + błąd (`forecast_error` w
  [`synthetic.yaml`](../packages/core/src/smogcast/core/conf/synthetic.yaml)) — więc model ma czego się nauczyć.
- Generowane są tylko miesiące sezonu grzewczego (żeby każdy podział miał dni z przekroczeniem) i 1% pustych wartości
  (jak w prawdziwym GIOŚ).
- **Nigdy nie mieszają się z prawdziwymi danymi** i nie służą do oceny modelu — tylko do sprawdzenia, że potok działa.

## 9. Odtworzenie z usterkami (G)

Kopia **prawdziwych** pomiarów z `bronze.pm_hourly` dla okna z `fault_injection.yaml` (Kraków, 17–21.01.2025 — w tym
prawdziwy epizod smogowy 20.01), zapisana w formacie paczek API (D) i zepsuta według harmonogramu: awaria łączności,
duplikat paczki, wartość ujemna i > 1000, pusta wartość, zamrożony odczyt, skok, dryf, zmiana rejestru. Osobny folder,
`source = 'fault_replay'`, osobne tabele strumienia — **nigdy nie trafia do danych modelu** (decyzja D17).
Wymaga wcześniej wypełnionego `bronze.pm_hourly` (Job batch). Scenariusz: [`demo-jakosc-danych.md`](demo-jakosc-danych.md).

## 10. Dokumenty bazy wiedzy (H)

Lista źródeł z uzasadnieniem: [`rag-dokumenty.md`](rag-dokumenty.md). W skrócie: oryginały pobierane do
`rag/_zrodlo/`, a każde pobranie sprawdza format (portale potrafią odpowiedzieć stroną HTML z captchą pod adresem
`.pdf` — taki plik jest odrzucany). EUR-Lex blokuje automaty, więc akty UE pochodzą z repozytorium Urzędu Publikacji
(Cellar); pełnych wytycznych WHO 2021 nie da się pobrać automatycznie. Polskie źródła weszły do indeksu jako
nieoficjalne tłumaczenia fragmentów (`docs/rag/`).

## 11. Lista kontrolna przy zmianie źródła

- [ ] Nowy rok archiwum: id z `Content-Disposition`, dopisany w `gios.yaml` i `run.archive_years`.
- [ ] Nowe źródło lub plik: oryginał bez zmian w landing, przetworzona wersja obok.
- [ ] Znaczniki czasu: koniec czy początek godziny, CET czy czas lokalny — konwersja tylko w `timeutil`, z testem.
- [ ] Jednostki wymuszone w żądaniu **i** sprawdzone w odpowiedzi.
- [ ] Nagłówki plików GIOŚ: jeśli GIOŚ je zmieni — zmiana w `smogcast.core.schema` (generator syntetyczny i bronze
      korzystają z tego samego kontraktu, test integracyjny to wyłapie).
- [ ] Cechy modelu: tylko informacja dostępna w chwili prognozy (D, 12:00 CET) — test braku wycieku w
      `silver_transform/tests`.
