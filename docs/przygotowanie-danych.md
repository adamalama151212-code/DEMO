# Przygotowanie danych: co pobrać, jak przygotować i gdzie wrzucić

Ten dokument opisuje wszystkie dane wejściowe projektu. Dla każdego zbioru znajdziesz:
- czy pobiera się sam, czy trzeba go pobrać ręcznie,
- skąd go wziąć,
- do jakiego folderu go wrzucić,
- jaki ma mieć format.

**Najważniejsze:** do podstawowego przebiegu (`radplume run-all`) **nie musisz
niczego przygotowywać**. Pogodę i listę miast program pobiera sam. Ręcznie pobierasz
tylko dane do **walidacji modelu na Fukushimie**, czyli do sprawdzenia, czy model
wskazuje, gdzie i kiedy przeszła prawdziwa chmura.

---

## 1. Przegląd

| # | Dane | Po co | Kto pobiera | Folder | Status w kodzie |
|---|---|---|---|---|---|
| A | Pogoda godzinowa (ERA5) | wiatr, opad, stabilność → smuga | **automatycznie** (`ingest-meteo`) | `data/landing/meteo/` | ✅ działa |
| B | Miasta z populacją (GeoNames) | pytanie „czy miasto Y…” | **automatycznie** (`ingest-cities`) | `data/landing/cities/` | ✅ działa |
| C | **Depozycja Cs-137 na gruncie** (JAEA) | walidacja: *gdzie* spadło skażenie | **Ty, ręcznie** | `data/landing/validation/deposition/` | ✅ działa (`radplume validation`) |
| D | **Godzinowe stężenia Cs-137 w powietrzu** (SPM, Oura 2015) | walidacja: *kiedy i którędy* przeszła chmura | **Ty, ręcznie** | `data/landing/validation/air_concentration/` | 🔜 format ustalony, ingest w kolejnym kroku |
| E | **Przebieg uwolnienia w czasie** (Katata 2015 / Terada 2020) | realny rozkład emisji zamiast równomiernego | **Ty, ręcznie** | `data/landing/source_term/` | ✅ działa (`radplume run-batch`) |
| F | Pomiary wiatru ze stacji AMeDAS (opcjonalnie) | sprawdzenie, ile błędu wnosi ERA5 | **Ty, ręcznie** | `data/landing/meteo_obs/amedas/` | 🔜 opcjonalne |
| G | Dokładne współrzędne Lubiatowa-Kopalina | poprawna lokalizacja źródła w PL | **Ty**, edycja pliku | `src/radplume/conf/sites.yaml` | ✅ wystarczy podmienić liczby |
| H | Dokumenty do RAG (raporty, normy) | aplikacja AI: wyjaśnianie metodologii | **Ty, ręcznie** | `data/landing/documents/` | 🔜 blok 7 planu (Databricks) |

**Kolejność, jeśli chcesz walidować model:** najpierw **E** (bez niego walidacja testuje
głównie założenie o równomiernej emisji), potem **C** (główna metryka), potem **D**
(test kierunku i czasu), a na końcu opcjonalnie **F**.

---

## 2. Struktura folderów

Folder `data/` powstaje w katalogu repozytorium przy pierwszym uruchomieniu i **nie
trafia do gita** (jest w `.gitignore`: dane są duże i mają własne licencje).
W Dockerze to ten sam folder na Twoim dysku, bo repozytorium jest zamontowane w kontenerze.

```
data/
├── landing/                      ← strefa „surowa”: TU wrzucasz pliki
│   ├── meteo/                    (A) automatycznie
│   ├── cities/                   (B) automatycznie
│   ├── validation/
│   │   ├── deposition/           (C) *.csv — depozycja Cs-137
│   │   │   └── _zrodlo/          oryginalne pliki pobrane z JAEA (program ich nie czyta)
│   │   └── air_concentration/    (D) *.csv — stężenia godzinowe SPM
│   │       └── _zrodlo/
│   ├── source_term/              (E) *.csv — przebieg uwolnienia
│   │   └── _zrodlo/
│   ├── meteo_obs/amedas/         (F) *.csv — pomiary wiatru
│   └── documents/                (H) PDF-y do RAG
├── delta/                        ← tabele Delta (bronze/silver/gold/ops) — NIE edytuj ręcznie
└── checkpoints/                  ← stan strumieni — NIE edytuj ręcznie
```

Utworzenie folderów w PowerShellu (w katalogu repozytorium):

```powershell
$dirs = "validation/deposition/_zrodlo", "validation/air_concentration/_zrodlo",
        "source_term/_zrodlo", "meteo_obs/amedas", "documents"
foreach ($d in $dirs) { New-Item -ItemType Directory -Force -Path "data/landing/$d" | Out-Null }
```

**Zasada `_zrodlo/`:** oryginalny plik zawsze zostaw nietknięty w podfolderze `_zrodlo/`,
a obok zapisz przekształcony CSV w formacie z tego dokumentu. Program czyta tylko pliki
`*.csv` leżące bezpośrednio w folderze, więc nie weźmie oryginału. Zawsze da się wtedy
sprawdzić, skąd wzięła się liczba (plan: „każdy wiersz da się prześledzić do źródła”).

Tryb `--offline` używa osobnego katalogu `data-offline/`. Dane walidacyjne wrzucaj do
`data/`, bo walidacja ma sens tylko z prawdziwą pogodą.

---

## 3. Wspólne zasady formatu CSV (ważne przy Excelu z polskimi ustawieniami)

Każdy plik, który przygotowujesz, musi spełniać te zasady:

| Zasada | Dlaczego |
|---|---|
| separator kolumn: **przecinek** `,` | polski Excel domyślnie zapisuje średnik `;`, więc kolumny się skleją |
| separator dziesiętny: **kropka** `1234.5` | polski Excel zapisuje `1234,5`, a Spark wczyta to jako tekst albo NULL |
| pierwszy wiersz: **nagłówek** z nazwami kolumn jak w tym dokumencie | kod szuka kolumn po nazwach |
| kodowanie: **UTF-8** | japońskie nazwy stacji i polskie znaki |
| czas: **UTC**, format `2011-03-15T06:00:00` | źródła japońskie podają czas **JST = UTC+9**; trzeba **odjąć 9 godzin** |
| współrzędne: stopnie dziesiętne WGS84 (`37.4213`, `141.0325`) | nie stopnie-minuty-sekundy |
| jednostki: dokładnie te z opisu kolumny | np. kBq/m², a nie Bq/m² |

Jak zapisać poprawny CSV z Excela: *Plik → Zapisz jako → „CSV UTF-8 (rozdzielany przecinkami)”*.
Wcześniej ustaw w Windows (*Ustawienia regionalne → Dodatkowe ustawienia*) separator
dziesiętny na kropkę i separator listy na przecinek. Prostsza alternatywa to przekształcenie
w Pythonie: `pandas.read_excel(...)` → `.to_csv(..., index=False)`.

Najczęstszy błąd w danych z Japonii to czas. Pomiar z „2011-03-15 15:00 JST” to
`2011-03-15T06:00:00` UTC. Pomyłka o 9 godzin przypisze chmurę do zupełnie innego
wiatru, a walidacja wyjdzie fatalna bez winy modelu.

---

## 4. Szczegóły dla każdego zbioru

### A. Pogoda (ERA5 przez Open-Meteo): automatycznie

- **Nic nie robisz.** `radplume ingest-meteo` pobiera dane sam, a pobrane lata zostają w cache.
- To **prawdziwa** pogoda: reanaliza ERA5, czyli pomiary połączone modelem pogody w siatkę ok. 25 km.
- Zakres jest w `src/radplume/conf/local.yaml` (`climatology.meteo_range`) i w `sites.yaml`
  (`validation.meteo_range` = 11–20 marca 2011 dla Fukushimy).
- Chcesz pobrać dane od nowa? Usuń plik z `data/landing/meteo/<lokalizacja>/`.

### B. Miasta (GeoNames): automatycznie

- **Nic nie robisz.** `radplume ingest-cities` pobiera `cities5000.zip`
  (miejscowości ≥ 5000 mieszkańców, licencja CC BY 4.0).

---

### C. Depozycja Cs-137 na gruncie (JAEA EMDB) ✅

**Po co:** główny test modelu. Porównuje, **ile cezu spadło w danym miejscu**, z tym, co
przewiduje model dla epizodu walidacyjnego `validation_2011`. Wynik to FAC2, FAC5 i pokrycie przedziału P5–P95.

**Skąd:** [JAEA — Database for Radioactive Substance Monitoring Data (EMDB)](https://emdb.jaea.go.jp/emdb/)
- pomiary lotnicze (Airborne Monitoring, MEXT/DOE): mapa depozycji Cs-137, najlepsze pokrycie terenu,
- [pomiary gleby z 2200 punktów](https://emdb.jaea.go.jp/emdb_old/en/portals/1020101001/) (MEXT, czerwiec 2011): dokładniejsze punktowo.

**Na co uważać:**
- **Data korekty rozpadu.** JAEA przelicza wartości na konkretny dzień, np. 14.06.2011 dla gleby
  albo 11.03.2013 dla części pomiarów lotniczych. Używaj **tylko Cs-137**: przez 2 lata ubywa go
  ok. 4,5%, co wobec niepewności modelu (×5) nie ma znaczenia. **Nie używaj Cs-134** (połowa
  rozpada się w 2 lata) ani sumy „Cs-134+137”.
- **Jednostki.** JAEA podaje zwykle Bq/m² albo kBq/m². Kolumna `measured_kbq_m2` musi być w **kBq/m²**
  (Bq/m² ÷ 1000).
- **Zasięg.** Model liczy ±100 km od elektrowni (lokalnie siatka 41 × 5 km). Punkty dalej są pomijane.
- **Liczba punktów.** Wystarczy kilkaset do kilku tysięcy. Nie ma sensu wrzucać milionów pikseli mapy lotniczej.

**Gdzie:** `data/landing/validation/deposition/jaea_cs137.csv` (nazwa dowolna, rozszerzenie `.csv`)

**Format:**

| Kolumna | Typ | Opis |
|---|---|---|
| `site_id` | tekst | zawsze `fukushima_daiichi` |
| `lat` | liczba | szerokość geograficzna punktu pomiaru (WGS84) |
| `lon` | liczba | długość geograficzna |
| `nuclide` | tekst | zawsze `Cs-137` |
| `measured_kbq_m2` | liczba | depozycja w kBq/m² |
| `source` | tekst | skąd pomiar, np. `JAEA-soil-2011-06` albo `JAEA-airborne-4th` |

```csv
site_id,lat,lon,nuclide,measured_kbq_m2,source
fukushima_daiichi,37.6680,140.7290,Cs-137,512.3,JAEA-soil-2011-06
```
*(wiersz ilustruje format, to nie jest prawdziwy pomiar)*

**Uruchomienie:** `radplume validation`, a wynik w `radplume show gold validation`.

---

### D. Godzinowe stężenia Cs-137 w powietrzu (stacje SPM) 🔜

**Po co:** sprawdza, **kiedy i którędy** przeszła chmura. W Japonii działa ponad 400 stacji
monitoringu pyłu zawieszonego (SPM). Ich taśmy filtracyjne wymieniano co godzinę i przechowano,
a naukowcy zmierzyli na nich cez z 99 stacji za każdą godzinę 12–23 marca 2011.
To najlepszy zbiór do sprawdzenia, czy model wysyła smugę we właściwą stronę o właściwej godzinie.

**Skąd:**
- baza: [Oura i in. 2015, „A Database of Hourly Atmospheric Concentrations of Radiocesium…”, J. Nucl. Radiochem. Sci.](https://www.jstage.jst.go.jp/article/jnrs/15/2/15_2_1/_article). Dane są w materiałach uzupełniających artykułu,
- metoda i opis zdarzeń: [Tsuruta i in. 2014, Scientific Reports](https://pmc.ncbi.nlm.nih.gov/articles/PMC5381196/).

**Na co uważać:**
- czas w źródle jest w **JST**, więc odejmij 9 h,
- wartości poniżej progu detekcji oznacz w `below_detection = true`, nie wpisuj zera,
- stężenia są w **Bq/m³**.

**Gdzie:** `data/landing/validation/air_concentration/spm_oura2015.csv`

**Format:**

| Kolumna | Typ | Opis |
|---|---|---|
| `station_id` | tekst | identyfikator stacji ze źródła |
| `station_name` | tekst | nazwa (może być po japońsku) |
| `lat`, `lon` | liczba | położenie stacji (WGS84) |
| `time_start_utc` | czas | początek godziny poboru, **UTC** |
| `time_end_utc` | czas | koniec godziny poboru, **UTC** |
| `nuclide` | tekst | `Cs-137` |
| `concentration_bq_m3` | liczba | stężenie w Bq/m³ (puste, jeśli poniżej detekcji) |
| `below_detection` | `true`/`false` | czy poniżej progu wykrywalności |
| `source` | tekst | np. `Oura2015` |

```csv
station_id,station_name,lat,lon,time_start_utc,time_end_utc,nuclide,concentration_bq_m3,below_detection,source
FKS-001,Futaba,37.4500,141.0120,2011-03-15T00:00:00,2011-03-15T01:00:00,Cs-137,123.4,false,Oura2015
```
*(wiersz ilustruje format, to nie jest prawdziwy pomiar)*

**Status:** format jest ustalony. Kod zapisujący godzinowe stężenia modelu w punktach stacji
i tabela `gold.validation_air` powstaną w kolejnym kroku. Model obłoków liczy już czas przejścia
chmury wzdłuż trajektorii, więc brakuje tylko zapisu stężeń w punktach stacji. Możesz już przygotować plik.

---

### E. Przebieg uwolnienia w czasie (source term) ✅

**Po co:** bez tego pliku epizod walidacyjny zakłada, że przez 96 h uwalniało się równo tyle samo
co godzinę. W rzeczywistości emisja szła w kilku krótkich zrzutach (m.in. popołudnie 12 marca,
noc 14/15, poranek i noc 15, poranek 16 marca), a mapa skażenia zależy od tego, na jaki wiatr
i deszcz trafił każdy zrzut. **Bez tego pliku walidacja C i D będzie zaniżona z powodu
założenia, a nie z powodu fizyki.**

**Skąd:**
- [Katata i in. 2015, Atmos. Chem. Phys. 15, 1029–1070](https://acp.copernicus.org/articles/15/1029/2015/): tabela tempa uwolnienia Cs-137 i I-131 w czasie (artykuł open access),
- alternatywnie: [Terada i in. 2020, J. Environ. Radioact.](https://www.sciencedirect.com/science/article/pii/S0265931X19304473), nowsza wersja szacunków.

**Na co uważać:**
- sprawdź w tabeli jednostkę (zwykle Bq/h) i strefę czasową (zwykle **JST**, więc odejmij 9 h),
- przedziały czasu mogą mieć różną długość (np. 3 h, 30 min). Wpisz je tak, jak są w źródle,
  bo kod rozłoży je na godziny,
- kolumnę `release_height_m` możesz wypełnić dla porządku, ale model jej jeszcze nie używa:
  wysokość uwolnienia pochodzi z wariantów w `sites.yaml` (`release_height_m: [min, centralna, max]`),
- okno epizodu to `validation.episode_start` + `episode_hours` w `sites.yaml`
  (12.03.2011 06:00 UTC + 96 h). Uwolnienie poza oknem jest pomijane, a w logu pojawia się
  ostrzeżenie z odsetkiem pominiętej ilości. Chcesz uwzględnić więcej? Wydłuż okno w `sites.yaml`
  i poszerz `validation.meteo_range`.

**Gdzie:** `data/landing/source_term/fukushima_2011_katata2015.csv`

**Format:**

| Kolumna | Typ | Opis |
|---|---|---|
| `site_id` | tekst | `fukushima_daiichi` |
| `nuclide` | tekst | `Cs-137` albo `I-131` |
| `time_start_utc` | czas | początek przedziału, **UTC** |
| `time_end_utc` | czas | koniec przedziału, **UTC** |
| `release_rate_bq_h` | liczba | tempo uwolnienia w Bq/h |
| `release_height_m` | liczba (opcjonalnie) | wysokość uwolnienia |
| `source` | tekst | np. `Katata2015-Table` |

```csv
site_id,nuclide,time_start_utc,time_end_utc,release_rate_bq_h,release_height_m,source
fukushima_daiichi,Cs-137,2011-03-12T06:00:00,2011-03-12T07:00:00,1.0e+13,120,Katata2015
```
*(wiersz ilustruje format, to nie są wartości z publikacji)*

**Co robi program z tym plikiem** (`radplume run-batch`, kroki `bronze` i `silver`):
1. `bronze.source_term`: plik wczytany bez zmian.
2. Przedziały są rozkładane na pełne godziny okna epizodu, proporcjonalnie do czasu nakładania się.
3. Całkowita ilość w oknie staje się medianą ilości dla `validation_2011` (niepewność
   `validation.source_term_gsd`, domyślnie ×/÷ 2). Ułamki godzinowe trafiają do `silver.release_schedule`.
4. Nuklid, którego nie ma w pliku (np. podasz tylko Cs-137), dostaje ilość z `sites.yaml`
   i ten sam profil czasowy.

Sprawdzenie: `radplume show silver release_schedule` i `radplume show silver source_terms`.
Kolumna `source = file` oznacza dane z pliku, a `config_uniform` oznacza, że pliku nie znaleziono.

---

### F. Pomiary wiatru ze stacji AMeDAS (opcjonalnie) 🔜

**Po co:** porównanie wiatru z ERA5 (siatka 25 km) z pomiarami ze stacji w terenie. Pokazuje,
ile błędu wnosi samo wejście meteo. Przydaje się do write-upu („ograniczenia”).

**Skąd:** [JMA AMeDAS](https://www.jma.go.jp/jma/en/Activities/amedas/amedas.html). Dane historyczne
są w serwisie „過去の気象データ・ダウンロード” (Past Weather Data Download) na stronie JMA.
Istotne stacje to m.in. Namie (浪江) i Iitate (飯舘), zakres 11–20.03.2011, dane godzinowe.

**Na co uważać:**
- czas w **JST**, więc odejmij 9 h,
- kierunek wiatru jest zapisany **słownie po japońsku w 16 kierunkach** (np. 北北西 = NNW),
  a trzeba go zamienić na stopnie (N = 0°, NNE = 22,5°, …, NNW = 337,5°),
- to kierunek, **z którego** wieje wiatr (tak samo jak w ERA5), więc nie dodawaj 180°.

**Gdzie:** `data/landing/meteo_obs/amedas/amedas_2011-03.csv`

**Format:** `station_id, station_name, lat, lon, time_utc, wind_speed_ms, wind_from_deg, precip_mm_h, source`

---

### G. Dokładne współrzędne Lubiatowa-Kopalina ✅

To nie jest plik do wrzucenia, tylko **edycja konfiguracji**. Obecne współrzędne są przybliżone,
bo test API zwrócił `elevation: 0.0`, czyli linię brzegową (plan, P21).

1. Znajdź współrzędne terenu elektrowni w dokumentach Polskich Elektrowni Jądrowych
   (decyzja środowiskowa / raport OOŚ dla lokalizacji Lubiatowo-Kopalino).
2. Otwórz `src/radplume/conf/sites.yaml` i w sekcji `lubiatowo_kopalino` podmień `lat` i `lon`.
3. Usuń stare dane pogodowe dla tej lokalizacji: folder `data/landing/meteo/lubiatowo_kopalino/`.
4. Uruchom `radplume run-batch`. W logu kroku `silver` nie powinno być ostrzeżenia
   `grid_elevation ≤ 0`.

---

### H. Dokumenty do RAG 🔜

Na etap aplikacji AI na Databricks (plan 4.8, blok 7). Nie jest potrzebny do lokalnego PoC.
Do `data/landing/documents/` wrzucaj PDF-y, na które aplikacja ma się powoływać:
- raport WMO Task Team (2013) — [„Evaluation of Meteorological Analyses for the Radionuclide Dispersion and Deposition from the Fukushima Daiichi NPP Accident”](https://www.researchgate.net/publication/259970914_Evaluation_of_Meteorological_Analyses_for_the_Radionuclide_Dispersion_and_Deposition_from_the_Fukushima_Daiichi_Nuclear_Power_Plant_Accident),
- [UNSCEAR 2020/2021, tom II, Aneks B](https://www.unscear.org/unscear/en/publications/2020_2021_2.html), m.in. [załącznik A-10](https://www.unscear.org/unscear/uploads/documents/publications/UNSCEAR_2020_21_Annex-B_Attach_A-10.pdf) o modelowaniu transportu,
- Katata i in. 2015 (jak w E),
- dokumenty IAEA/ICRP o progach skażenia i dawek.

Te raporty warto przeczytać także do write-upu. Raport WMO porównuje 5 profesjonalnych modeli
z pomiarami i daje punkt odniesienia dla Twojego wyniku.

---

## 5. Lista kontrolna

- [ ] (G) podmieniłem współrzędne Lubiatowa w `sites.yaml` i usunąłem stare meteo tej lokalizacji
- [ ] (E) przebieg uwolnienia z Katata 2015 → `data/landing/source_term/*.csv`, czas w UTC
- [ ] (C) depozycja Cs-137 z JAEA → `data/landing/validation/deposition/*.csv`, kBq/m², tylko Cs-137
- [ ] (D) stężenia godzinowe SPM (Oura 2015) → `data/landing/validation/air_concentration/*.csv`, czas w UTC
- [ ] (F, opcjonalnie) wiatr AMeDAS → `data/landing/meteo_obs/amedas/*.csv`, kierunek w stopniach
- [ ] oryginały zostawione w `_zrodlo/`
- [ ] CSV: przecinek jako separator, kropka dziesiętna, UTF-8, nagłówek zgodny z tabelą
- [ ] `radplume run-batch` i `radplume validation` bez błędów

Jeśli plik źródłowy ma inną strukturę niż opisana (np. wiele arkuszy Excela albo japońskie
nagłówki), wrzuć oryginał do `_zrodlo/` i pokaż fragment. Można wtedy dopisać parser
bezpośrednio pod oryginalny format zamiast przekształcać go ręcznie.

---

## 6. Licencje i dobre praktyki

- Dane z `data/` **nie trafiają do repozytorium** (`.gitignore`). Każdy zbiór ma własną licencję:
  Open-Meteo CC BY 4.0, GeoNames CC BY 4.0, Safecast CC0, JAEA wg warunków serwisu.
  Źródła wymień w README/write-upie.
- **Nie wpisuj ręcznie „przykładowych” wartości udających pomiary.** Walidacja na
  zmyślonych liczbach jest gorsza niż jej brak. Przykładowe wiersze w tym dokumencie służą
  wyłącznie do pokazania formatu.
- Zmiana pliku w `landing/` → uruchom ponownie odpowiedni krok. Kroki są idempotentne,
  więc ponowne uruchomienie nie dubluje danych.
