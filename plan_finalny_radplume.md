# Projekt finalny — platforma do modelowania dyspersji radionuklidów

**Dokument roboczy dla agenta implementującego.**
Zawiera: ocenę zgodności z wymaganiami kursu, architekturę docelową, plan PoC lokalnego
oraz plan wdrożenia na Databricks (DEV → PROD przez CI/CD).

### Rejestr poprawek (rewizja 2 — po przeglądzie zgodności ze specyfikacją)

| # | Problem w rewizji 1 | Poprawka | Sekcje |
|---|---|---|---|
| P1 | `withWatermark` po cichu gubi spóźnione rekordy — nie da się ich skierować do `late_rejected`, a wynik zależy od podziału na mikro-batche (niedeterministyczne testy) | spóźnienie liczone jawnie z pola `sent_at` (`lag = sent_at - event_time`), podział bezstanowym filtrem; watermark tylko do ograniczenia stanu deduplikacji | 3.5.1, 3.5.2, 3.5.3, 4.4 |
| P2 | rolling z-score / frozen reading jako okna po wierszach — nieobsługiwane w Structured Streaming; `flatMapGroupsWithState` nie istnieje w PySpark | detekcja dryfu i zamrożenia w materialized view nad `silver_sensor_clean`; stan urządzenia w tabeli Delta aktualizowanej w `foreachBatch` (`MERGE`) | 3.5.3, 3.6, 4.4 |
| P3 | generator wysyła stałe tło dawki niezwiązane ze smugą → join z P5–P95 nie ma sensu | dawka w generatorze wyprowadzana z wyniku modelu dla komórki czujnika + szum + wstrzyknięte odchylenia | 3.5.3, 4.4, VIII |
| P4 | sieć „radiacyjna”, a wszystkie anomalie dotyczą wiatru | urządzenia = stacje meteo-radiacyjne (jak część stacji EURDEP); anomalie dla obu pomiarów | 3.5.1, 3.5.2 |
| P5 | RLS po `country_code` — wszystkie lokalizacje to `JP`, filtr niczego nie filtruje | RLS po `jurisdiction_code` (prefektura/operator) + jedna lokalizacja europejska | 1.3, 4.6 |
| P6 | brak opisu, jak RLS/CLS i granty trafiają do PROD | plik `sql/governance.sql` jako task w Jobie deployowanym bundlem; jednorazowy bootstrap opisany jawnie | 4.1, 4.5, 4.6 |
| P7 | fine-tuning Qwena + benchmark Spider — osobny projekt, ryzyko na demo, koszt GPU | text-to-SQL na Foundation Model API (pay-per-token); fine-tuning jako opcjonalny blok na koniec | 2.2, 4.8, V, VI |
| P8 | „REST API automation” w kursie najpewniej = automatyzacja Databricksa przez REST API, nie ingest z Open-Meteo | zdolność zaawansowana: **CDC** (`AUTO CDC` na rejestrze urządzeń) jako pewna; Zerobus jako opcjonalna | 1.2, 4.4 |
| P9 | Zerobus zapisuje do tabeli o stałym schemacie — `addNewColumns` tam nie działa | główny ingest streamingu przez Auto Loader (tu żyje schema evolution); Zerobus jako opcjonalna równoległa ścieżka | 2.2, 4.4, 4.4a |
| P10 | guardrails text-to-SQL mylone z guardrails AI-assisted development wymaganymi w write-upie | dwie osobne sekcje w write-upie | VII |
| P11 | brak scenariusza 20–30 min demo | dodana Część X | X |

---

## CZĘŚĆ I — Ocena zgodności z wymaganiami kursu

### 1.1 Werdykt

Projekt wpisuje się w wymagania **dobrze, ale nie automatycznie**. Rdzeń (medalion, jakość danych,
idempotencja, dashboard, RAG, testy) wynika z projektu naturalnie. Trzy elementy trzeba
**zaprojektować świadomie**, bo same z siebie się nie pojawią: streaming, RLS/CLS oraz PROD.

### 1.2 Mapowanie wymagań

| Wymaganie kursu | Pokrycie | Status |
|---|---|---|
| Dwa środowiska DEV/PROD, nic ręcznie w PROD | Asset Bundles, dwa targety | ✅ naturalne |
| Pełny medalion bronze→silver→gold | rdzeń projektu | ✅ naturalne |
| **Ingest batch** | Open-Meteo (REST), pomiary walidacyjne | ✅ naturalne |
| **Ingest streaming** | symulator sieci monitoringu → Auto Loader (opcjonalnie równolegle Zerobus) | ⚠️ **do zaprojektowania** |
| Unity Catalog (katalogi/schematy/volumes) | naturalne | ✅ |
| **RLS / CLS** | wielopodmiotowość wg jurysdykcji (prefektura/operator), lokalizacje w JP i w UE | ⚠️ **wymaga uzasadnienia** |
| Secrets w Key Vault | klucze API, token modelu | ✅ naturalne |
| Schema evolution | Auto Loader `schemaEvolutionMode = addNewColumns` na surowym JSON — firmware v2 dodaje kolumnę | ✅ naturalne (tylko ścieżka Auto Loader, nie Zerobus) |
| Data quality / expectations | expectations w Lakeflow: zakresy fizyczne + **czyszczenie brudnych danych czujników** | ✅ **bardzo naturalne, teraz główny akcent projektu** |
| Idempotencja, re-runnable | seedowany MC + `MERGE` po kluczu | ✅ naturalne |
| ≥1 pipeline deklaratywny (Lakeflow) | medalion jako pipeline deklaratywny | ✅ |
| ≥1 Lakeflow Job | orkiestracja: ingest → pipeline → gold → walidacja | ✅ |
| Testy jednostkowe + DQ w CI | testy fizyki (wartości analityczne!) + testy na spóźnione/anomalne dane | ✅ **mocna strona** |
| Asset Bundles + CI/CD | GitHub Actions | ✅ |
| Dashboard na gold | mapa ryzyka, ranking lokalizacji | ✅ |
| Aplikacja AI (RAG) | router: text-to-SQL (Foundation Model API) + RAG dokumentowy | ✅ **mocna strona** |
| ≥1 zdolność zaawansowana | **CDC** — `AUTO CDC` na rejestrze urządzeń (SCD2) + opcjonalnie Zerobus | ✅ |

> **Uwaga do P8:** ingest z Open-Meteo przez REST **nie jest** liczony jako „REST API automation" —
> w kontekście kursu to najpewniej automatyzacja samego Databricksa przez REST API. Nie opieraj
> na tym zaliczenia wymogu; ewentualnie dopytaj prowadzącego.

### 1.3 Trzy luki i jak je domknąć

**Luka 1 — streaming.** Symulacja dyspersji jest z natury wsadowa. Nie doklejaj streamingu
sztucznie; wbuduj go jako **drugą, równoległą ścieżkę danych**: symulowana sieć monitoringu
radiacyjnego (wzorowana na EURDEP/Safecast) wysyła odczyty w czasie rzeczywistym.
Ta ścieżka ma własny sens: porównuje **pomiar na żywo z przewidywaniem modelu** i generuje alert,
gdy odczyt wypada poza przedział P5–P95 z warstwy Gold. To jest realny wzorzec z systemów
kryzysowych, a nie ozdobnik. **To właśnie tutaj żyje teraz główny ciężar projektu: czujniki mają
realistycznie brudne, spóźnione i anomalne dane, a pipeline musi je świadomie czyścić** — patrz
Część IV.4a.

**Luka 2 — RLS/CLS.** Dane o skażeniu same z siebie nie mają wrażliwych kolumn. Uzasadnienie:
platforma jest **wielopodmiotowa** — korzystają z niej regionalne i krajowe organy dozoru.
- **RLS:** analityk widzi tylko lokalizacje i czujniki w swojej jurysdykcji (`jurisdiction_code`,
  np. `JP-07` Fukushima, `JP-08` Ibaraki, `EU-xx` lokalizacja europejska). Filtr po samym
  `country_code` byłby pusty w praktyce — Daiichi i Daini to oba `JP`. Dlatego: podział
  na prefektury/operatorów **oraz** jedna lokalizacja europejska (Open-Meteo działa globalnie,
  kod się nie zmienia — to jednocześnie test architektury z etapu 9)
- **CLS:** dokładne parametry źródła uwolnienia (`source_term_bq`, dokładne współrzędne reaktora)
  maskowane dla roli `public_analyst`, widoczne dla `regulator`
- dodatkowo: surowe odczyty z sieci monitoringu zawierają `device_id` i dokładną lokalizację
  właściciela czujnika — **to jest autentyczny problem prywatności w Safecast** i najlepsze
  uzasadnienie maskowania w całym projekcie

**Luka 3 — PROD.** Free Edition i trial nie wystarczą. PROD musi być płatnym workspace.
Patrz sekcja kosztów — to zmienia wcześniejsze szacunki.

### 1.4 Gdzie projekt wygrywa z typowym projektem kursowym

- **testy jednostkowe mają sens** — fizyka daje sprawdzalne wartości analityczne
  i bilans masy; większość projektów testuje trywialne transformacje
- **expectations są naprawdę merytoryczne** — stężenie ≥ 0, bilans masy, prędkość wiatru
  w zakresie fizycznym, monotoniczność rozpadu
- **realistyczna obsługa brudnych danych z czujników** — buforowanie/spóźnione paczki,
  anomalie sprzętowe — to jest coś, czego praktycznie żaden projekt kursowy nie ma i co
  najbardziej liczy się w ocenie inżynierskiej jakości (feedback od praktyka data engineeringu)
- **walidacja na niezależnych danych pomiarowych** — prawie nikt tego nie robi
- **uczciwa sekcja ograniczeń** — wprost punktowana w rubryce ("honest discussion of trade-offs")
- **własny zestaw ewaluacyjny text-to-SQL** na tabelach Gold (20–30 pytań z oczekiwanym wynikiem)
  — tani, a mierzalny; dostrojony model tylko jako opcjonalny dodatek (blok 10)

---

## CZĘŚĆ II — Architektura docelowa

### 2.1 Chmura

**Azure** — wymuszone przez Key Vault i Azure DevOps w opisie kursu.
Azure Databricks + ADLS Gen2 + Key Vault. (GitHub Actions jest dopuszczone jako alternatywa CI.)

### 2.2 Przepływ danych

```
ŚCIEŻKA A — BATCH (symulacja)
Open-Meteo REST ──┐
Safecast / MEXT ──┼──► landing (volume) ──► bronze ──► silver ──► gold_risk_map
sites, grid    ───┘                                              gold_validation
scenarios (MC) ───┘                                              gold_site_ranking

ŚCIEŻKA B — STREAMING (monitoring, celowo brudne dane)
gold_risk_map (eksport dawki oczekiwanej per komórka/godzina) ──► symulator czujników
symulator czujników ──► volume landing/sensor ──► Auto Loader ──► bronze_sensor_raw (append-only, z sent_at)
  (buforowanie, anomalie,                                           │
   firmware v2 = nowa kolumna)                    lag = sent_at - event_time
                                                   ├── lag > 15 min ──► ops.sensor_late_rejected
                                                   └── lag ≤ 15 min ──► dedup (watermark) + expectations
                                                                          ├── drop/quarantine ──► ops.sensor_quarantine
                                                                          └── silver_sensor_clean
                                                  silver_sensor_quality (MV: dryf z-score, frozen reading)
                                                  ──► gold_live_alerts
                                                        ▲
                                          (join z gold_risk_map: pomiar vs przedział P5–P95)
  opcjonalnie: ten sam symulator ──► Zerobus ──► bronze_sensor_zerobus (stały schemat, bez schema evolution)

ŚCIEŻKA C — CDC (zdolność zaawansowana)
rejestr urządzeń (zmiany: firmware, jurysdykcja, wycofanie) ──► landing/device_cdc
      ──► AUTO CDC (SCD typ 2) ──► silver_devices ──► join w silver_sensor_clean / RLS czujników

WARSTWA SERWUJĄCA
gold_* ──► dashboard AI/BI
gold_* ──► text-to-SQL (Foundation Model API, pay-per-token)   ──┐
dokumenty ──► Vector Search ──► RAG                            ──┴──► Databricks App
```

### 2.3 Unity Catalog

```
radplume_dev / radplume_prod        (katalogi — jeden na środowisko)
├── raw        (schema)   + volume `landing`
├── bronze
├── silver
├── gold
└── ops                   (audyt, metryki jakości, wyniki walidacji, odrzucone rekordy)
```

Nazwa katalogu wstrzykiwana przez zmienną bundla — **ten sam kod w obu środowiskach**.

---

## CZĘŚĆ III — Faza 0: PoC lokalny (5–6 dni)

Cel: zweryfikować fizykę i koszt **zanim** wyda się pieniądze na chmurę.
Szczegóły w osobnym dokumencie; poniżej to, co krytyczne dla wersji finalnej.

### 3.1 Zasady przenaszalności

**Zakazane w `silver/` i `gold/`:** `pandas`, `numpy`, `.collect()`, `.toPandas()`,
pętle po siatce/scenariuszach, Python UDF, `.coalesce(1)`, ścieżki literalne.

**Wymagane od pierwszego dnia:** Delta lokalnie (`delta-spark`), partycjonowanie zdefiniowane
od razu, `shuffle.partitions` z configu, seedowany RNG, ingest odseparowany od transformacji.

### 3.2 Struktura repozytorium

```
radplume/
├── databricks.yml              # Asset Bundle
├── resources/
│   ├── pipeline_batch.yml      # Lakeflow declarative
│   ├── pipeline_stream.yml
│   ├── job_orchestration.yml
│   ├── app.yml                 # Databricks App
│   └── dashboard.yml
├── sql/
│   └── governance.sql          # ROW FILTER, COLUMN MASK, granty — wykonywane jako task Joba
├── config/
│   ├── local.yaml  dev.yaml  prod.yaml
│   ├── physics.yaml
│   └── sensor_dq.yaml
├── src/radplume/
│   ├── session.py  paths.py  config.py
│   ├── ingest/     meteo.py  measurements.py  scenarios.py  sensor_sim.py  device_registry.py
│   ├── bronze/  silver/  gold/  validation/
│   │   silver/     sensor_clean.py  sensor_quality.py  devices_cdc.py
│   └── app/        router.py  text_to_sql.py  rag.py
├── eval/
│   └── text_to_sql_eval.yaml   # 20–30 pytań + oczekiwany wynik na Gold
├── tests/
│   ├── test_physics.py         # wartości analityczne + bilans masy
│   ├── test_schemas.py
│   ├── test_idempotency.py
│   ├── test_sensor_dq.py       # podział po lag, spike, dryf, frozen, quarantine
│   └── test_sensor_sim.py      # generator: seed → identyczny wynik, dawka zgodna z modelem
└── .github/workflows/ci.yml
```

### 3.3 Etapy z kryteriami ukończenia

| # | Etap | Gotowe gdy |
|---|---|---|
| 1 | Szkielet, sesja, config, Delta | pusty przebieg tworzy tabelę Delta |
| 2 | Ingest meteo (Open-Meteo, Fukushima 03/2011) | `bronze_meteo` z `ingest_timestamp`, `source_file` |
| 3 | Siatka + scenariusze MC | odtwarzalne przy tym samym seedzie; **CRS zapisany w schemacie** |
| 4 | Silver: meteo, klasa Pasquilla | rozkład klas sensowny (noc = stabilne) |
| 5 | **Fizyka na jednym wierszu** ⚠️ | 3 testy: oś smugi, punkt boczny, **bilans masy** |
| 6 | Silver: pełna dyspersja | ~10 mln wierszy < 5 min; mapa smugi zgodna z kierunkiem wiatru |
| 7 | Gold | `percentile_approx`, mapa prawdopodobieństwa czytelna |
| 8 | **Walidacja** ⚠️ | znasz coverage, FAC2/FAC5, błąd kierunkowy |
| 9 | Druga lokalizacja (Daini) | **bez zmian w kodzie** — test architektury |
| 10 | Próba skalowania lokalnie | wiesz, co pęka pierwsze |
| 11 | **Symulator czujników + czyszczenie** ⚠️ | patrz 3.5 — działa lokalnie na streamie plikowym |

**Etapu 5 nie wolno pominąć.** Błąd fizyki wykryty na etapie 8 = przeliczenie wszystkiego.
**Etap 11 jest teraz priorytetem demo** — to on ma pokazać umiejętność czyszczenia brudnych danych.

### 3.4 Konfiguracja — dwa wymiary niepewności

```yaml
run:
  mode: validation            # validation | climatology
  episode_hours: 72           # długość jednego uwolnienia
  n_episodes: 1               # ile losowych momentów startu (climatology: 50-200)
  meteo_range: [2011-03-11, 2011-03-16]
  n_param_scenarios: 30       # niepewność parametrów na epizod
  grid_size: 60
  cell_km: 4
```

- **niepewność meteorologiczna** — nie wiadomo, *kiedy* nastąpi awaria → losowanie daty z historii
- **niepewność parametryczna** — nie wiadomo, *ile* się uwolni i jaka będzie stabilność

Iloczyn tych wymiarów to przestrzeń Monte Carlo. **Uwaga na eksplozję:** przy trybie
klimatologicznym nie utrwalaj Silvera godzinowego — agreguj do poziomu epizodu w tym samym jobie.
Godzinowy Silver tylko dla epizodu walidacyjnego.

### 3.5 Symulator brudnych danych — założenia i lokalne testowanie ⭐ NOWE

To jest teraz **główny obszar do popisania się** (feedback od praktyka data engineeringu):
pokazanie, że umiesz transformować i czyścić brudne dane sensoryczne do wartości analitycznych,
a nie tylko przepuszczać czyste dane przez pipeline.

#### 3.5.1 Dwa zjawiska do zasymulowania

**A. Buforowanie / spóźnione paczki (late-arriving data)**

Scenariusz: czujnik traci łączność (np. padła karta WiFi) na losowy czas, dane lokalnie
buforuje, a po przywróceniu łączności wysyła całą zaległą paczkę na raz z historycznymi
znacznikami czasu.

Parametry symulacji:
- prawdopodobieństwo utraty łączności: ok. 2–5% szans na start awarii w każdym "tick-u" na
  urządzenie (np. co 1 min symulacji)
- czas trwania awarii: losowy, np. `uniform(5, 45)` minut
- po powrocie: urządzenie wysyła wszystkie zbuforowane odczyty jednym batchem, z oryginalnymi
  (przeszłymi) `event_time`, ale jednym **`sent_at`** = moment wysyłki (pole w payloadzie,
  ustawiane przez urządzenie w chwili nadania — nie przez pipeline)

Reguła biznesowa do zaimplementowania (zgodnie z tym, co powiedział szwagier):
- **przyjmujemy odczyty opóźnione maksymalnie o 15 minut**: `lag = sent_at - event_time`
- odczyty z `lag > 15 min` trafiają do `ops.sensor_late_rejected` (nie są gubione —
  są świadomie odseparowane, żeby pokazać, że wiesz co robisz, a nie że tracisz dane
  przez przypadek)

**Dlaczego nie sam `withWatermark` (poprawka P1):** watermark w Structured Streaming odrzuca
spóźnione rekordy **po cichu** w operatorach stanowych — nie da się ich przechwycić i zapisać
do osobnej tabeli. Do tego watermark przesuwa się dopiero po zakończeniu mikro-batcha, więc
to, czy rekord spóźniony o 16 min przepadnie, zależy od podziału danych na batche →
testy byłyby niedeterministyczne. Dlatego:
1. **klasyfikacja spóźnienia** = bezstanowy filtr po jawnie policzonym `lag` →
   deterministyczna, testowalna, z jawną tabelą odrzuconych
2. **`withWatermark("event_time", "15 minutes")` + `dropDuplicatesWithinWatermark`** po
   `(device_id, event_time)` — **tylko** do ograniczenia stanu deduplikacji, już na strumieniu
   przefiltrowanym w kroku 1. Watermark ma tę samą wartość co reguła biznesowa, więc nie
   powinien niczego dodatkowo gubić; licznik `numRowsDroppedByWatermark` z metryk strumienia
   trafia do `ops` jako kontrola, że faktycznie jest ~0

**B. Anomalie czujnika**

Urządzenia to **stacje meteo-radiacyjne** (poprawka P4) — mierzą moc dawki `dose_rate_usv_h`
**oraz** prędkość wiatru, jak część stacji sieci EURDEP. Dawka jest sygnałem głównym
(to ona jest porównywana z modelem), wiatr — pomocniczym. Anomalie dotyczą obu pomiarów.

Scenariusz: sporadyczny błąd sprzętowy/firmware powoduje nierealną wartość pomiaru.

Parametry symulacji:
- prawdopodobieństwo anomalii: ok. 0.5–1% odczytów
- typy anomalii do zasymulowania (miej różne, żeby pokazać różne strategie czyszczenia):
  1. **spike fizycznie niemożliwy** — wiatr 10 → 80 m/s w jednym odczycie albo dawka
     przekraczająca zakres detektora → twardy błąd czujnika → `expect_or_drop`
     (rekord zapisany w `ops.sensor_quarantine` z powodem, żeby był widoczny na dashboardzie)
  2. **dryf sensora dawki** — stopniowe, powolne przesunięcie odczytu w górę przez np. 2h
     (typowe dla rozkalibrowanego detektora) → wykrywane przez rolling z-score, nie przez
     twardy próg → trafia do kwarantanny, nie do drop
  3. **zamrożony odczyt** — czujnik zwraca dokładnie tę samą wartość przez N minut z rzędu
     (typowy objaw zawieszonego firmware) → flaga (`expect`), nie drop, bo teoretycznie
     mogłaby to być realna cisza
  4. **wartość zerowa/null przy pracującym urządzeniu** (`battery > 0`) → osobna reguła

Typy 2 i 3 wymagają historii odczytów urządzenia, więc **nie** są liczone w samym strumieniu —
patrz krok 3b w 3.5.3 (poprawka P2).

#### 3.5.2 Progi jakości danych (do `physics.yaml` / `sensor_dq.yaml`)

```yaml
sensor_dq:
  late_arrival:
    max_lag_minutes: 15          # reguła szwagra: lag = sent_at - event_time
    reject_table: ops.sensor_late_rejected
  dedup:
    watermark_minutes: 15        # tylko ograniczenie stanu deduplikacji, nie reguła biznesowa
    keys: [device_id, event_time]

  wind_speed_ms:
    hard_min: 0
    hard_max: 45                 # powyżej = fizycznie niemożliwe na anemometrze -> drop
    max_delta_per_reading: 25    # zmiana między kolejnymi odczytami tego czujnika -> quarantine
  dose_rate_usv_h:
    hard_min: 0
    hard_max: 1000                # zakres detektora; kontekstowo dobrać wg skali projektu

  drift_detection:               # liczone w MV silver_sensor_quality, nie w strumieniu
    window_minutes: 120
    z_score_threshold: 3.0
    warmup_minutes: 10           # nowe urządzenie bez baseline'u nie podlega regule

  frozen_reading:
    repeat_count_threshold: 10    # ta sama wartość N razy z rzędu = podejrzane
```

#### 3.5.3 Jak zasymulować to lokalnie (przed chmurą)

Cel fazy PoC: **ten sam kod** (moduł `sensor_sim.py` + `silver/sensor_clean.py`) działa lokalnie
na Spark Structured Streaming czytającym z katalogu plików, a docelowo na Databricks czyta
z Auto Loadera (`cloudFiles`) na volume. Zmienia się tylko źródło, nie logika czyszczenia.

**Krok 1 — generator w Pythonie (`src/radplume/ingest/sensor_sim.py`)**

Prosty proces, który co tick symulacji dopisuje nowe pliki JSON do katalogu
lokalnego `data/raw/sensor_stream/`, symulując wiele urządzeń jednocześnie. Każde urządzenie ma
własny wewnętrzny stan: łączność, bufor, aktywna anomalia (dryf / zamrożenie).

**Dawka musi pochodzić z modelu (poprawka P3).** Gdyby generator losował stałe tło
(`gauss(0.1, 0.02)`), porównanie z przedziałem P5–P95 z `gold_risk_map` nie miałoby sensu —
alerty byłyby wszędzie albo nigdzie. Dlatego generator czyta eksport oczekiwanej dawki
(mediana z `gold_risk_map` per `cell_id` × godzina) i generuje odczyt jako:
`dawka = tło + mediana_modelu(cell, godzina) × lognormal(0, σ)`, gdzie σ dobrane tak, żeby
~90% odczytów mieściło się w P5–P95. Wtedy alerty `outside_model_band` powstają głównie tam,
gdzie **celowo** wstrzyknięto odchylenie (scenariusz demo), a nie przypadkiem.

Szkic logiki (do rozwinięcia przez agenta):

```python
import json, random, time
from pathlib import Path
from datetime import datetime, timedelta

OUT_DIR = Path("data/raw/sensor_stream")
BACKGROUND_USV_H = 0.05

class DeviceState:
    def __init__(self, device_id, cell_id, firmware="v1"):
        self.device_id, self.cell_id, self.firmware = device_id, cell_id, firmware
        self.connected = True
        self.buffer = []
        self.outage_left = 0
        self.drift_left, self.drift_offset = 0, 0.0      # (2) dryf detektora dawki
        self.frozen_left, self.frozen_value = 0, None    # (3) zamrożony odczyt

    def tick(self, sim_time, rng, expected_dose):
        # 1. Losowa utrata łączności
        if self.connected and rng.random() < 0.03:
            self.connected = False
            self.outage_left = int(rng.uniform(5, 45))

        reading = self._make_reading(sim_time, rng, expected_dose)

        if not self.connected:
            self.buffer.append(reading)
            self.outage_left -= 1
            if self.outage_left > 0:
                return []                      # nic nie wysyłamy podczas awarii
            self.connected = True
            flushed, self.buffer = self.buffer, []
        else:
            flushed = [reading]

        # sent_at ustawiane w chwili wysyłki — dla paczki z bufora jest wspólne i późniejsze
        # niż event_time, stąd jawny lag = sent_at - event_time (poprawka P1)
        for r in flushed:
            r["sent_at"] = sim_time.isoformat()
        return flushed

    def _make_reading(self, sim_time, rng, expected_dose):
        median = expected_dose.get((self.cell_id, sim_time.replace(minute=0)), 0.0)
        dose = BACKGROUND_USV_H + median * rng.lognormvariate(0, 0.35)
        wind = max(rng.gauss(8, 3), 0)

        roll = rng.random()
        if roll < 0.004:
            wind = rng.uniform(60, 90)                   # (1) spike wiatru -> drop
        elif roll < 0.006:
            dose = rng.uniform(5_000, 20_000)            # (1) dawka poza zakresem detektora -> drop
        elif roll < 0.007 and self.drift_left == 0:
            self.drift_left = 120                        # (2) start dryfu na 2h
        elif roll < 0.008 and self.frozen_left == 0:
            self.frozen_left, self.frozen_value = 15, round(dose, 4)  # (3) zawieszony firmware

        if self.drift_left > 0:
            self.drift_offset += 0.01 * max(median, BACKGROUND_USV_H)   # +1%/min -> po 2h ~ +120%
            dose += self.drift_offset
            self.drift_left -= 1
            if self.drift_left == 0:
                self.drift_offset = 0.0
        if self.frozen_left > 0:
            dose = self.frozen_value
            self.frozen_left -= 1

        reading = {
            "device_id": self.device_id,
            "event_time": sim_time.isoformat(),
            "dose_rate_usv_h": round(dose, 4),
            "wind_speed_ms": round(wind, 2),
            "battery": round(rng.uniform(20, 100), 1),
        }
        if self.firmware == "v2":
            reading["detector_temp_c"] = round(rng.gauss(18, 4), 1)  # nowa kolumna -> schema evolution
        return reading

def run_simulation(devices, expected_dose, minutes=180, seed=42, start=datetime(2011, 3, 14)):
    rng = random.Random(seed)                          # seed -> identyczny wynik, testowalne
    sim_time = start
    for _ in range(minutes):
        batch = []
        for d in devices:
            batch.extend(d.tick(sim_time, rng, expected_dose))
        if batch:
            fname = OUT_DIR / f"batch_{sim_time.strftime('%Y%m%dT%H%M%S')}.json"
            fname.write_text("\n".join(json.dumps(r) for r in batch))
        sim_time += timedelta(minutes=1)
        time.sleep(0.05)   # opcjonalnie spowolnij, żeby "wyglądało" na żywo na demo
```

Ten generator jest **odseparowany od logiki czyszczenia** — dokładnie zgodnie z zasadą
przenaszalności z sekcji 3.1. `expected_dose` to słownik wczytany z eksportu `gold_risk_map`
(plik w volume), a lista urządzeń (z `cell_id`, `jurisdiction_code`, `firmware`) pochodzi
z rejestru urządzeń — tego samego, który zasila ścieżkę CDC (4.4). Na Databricks generator
działa jako task Joba zapisujący pliki do volume `landing/sensor`.

**Scenariusz demo:** osobna funkcja `run_demo_scenario()` z ręcznie ustawionym momentem awarii
łączności (np. 20 min → paczka odrzucona), jednym spike'iem, jednym dryfem i jednym **realnym**
odchyleniem dawki ×5 względem modelu w konkretnej komórce — żeby na demo każdy przypadek
pojawił się przewidywalnie.

**Krok 2 — czytanie strumienia lokalnie (Spark Structured Streaming, file source)**

```python
raw_stream = (
    spark.readStream
    .format("json")
    .schema(sensor_schema)                 # jawny schemat, nie inferSchema na streamie
    .option("maxFilesPerTrigger", 1)
    .load("data/raw/sensor_stream/")
)
```

To jest lokalny odpowiednik Auto Loadera — ten sam wzorzec kodu, inne źródło. Migracja do
`cloudFiles` na Databricks to zmiana jednej linijki configu, nie logiki. Lokalnie schemat jest
jawny; na Databricks Auto Loader z `schemaEvolutionMode = addNewColumns` przyjmuje kolumnę
`detector_temp_c` z firmware v2 (schema evolution pokazane na realnym przypadku).

**Krok 3a — czyszczenie w strumieniu, tylko reguły bezstanowe + deduplikacja
(`silver/sensor_clean.py`)**

Logika w czystych funkcjach `DataFrame -> DataFrame`, żeby te same funkcje testować w pytest
i wywoływać z pipeline'u Lakeflow:

```python
def with_lag(df, max_lag_min):
    lag_min = (F.col("sent_at").cast("long") - F.col("event_time").cast("long")) / 60
    return (df.withColumn("lag_minutes", lag_min)
              .withColumn("is_late", F.col("lag_minutes") > max_lag_min))

def split_late(df, max_lag_min):
    df = with_lag(df, max_lag_min)
    return df.filter(~F.col("is_late")), df.filter(F.col("is_late"))   # (on_time, late)

def dedup(df, watermark_min):
    # stan deduplikacji ograniczony watermarkiem; dane już przefiltrowane po lag
    return (df.withWatermark("event_time", f"{watermark_min} minutes")
              .dropDuplicatesWithinWatermark(["device_id", "event_time"]))

HARD_RULES = {                     # -> expect_or_drop, powód zapisany w ops.sensor_quarantine
    "wind_physical": "wind_speed_ms BETWEEN 0 AND 45",
    "dose_in_detector_range": "dose_rate_usv_h BETWEEN 0 AND 1000",
    "no_null_when_powered": "NOT (battery > 0 AND dose_rate_usv_h IS NULL)",
}
```

W Lakeflow: `bronze_sensor_raw` (streaming table, Auto Loader) → dwa flowy:
`ops.sensor_late_rejected` (filtr `is_late`) oraz `silver_sensor_clean`
(`~is_late` → `dedup` → `@dlt.expect_all_or_drop(HARD_RULES)`). Rekordy odrzucone przez
reguły twarde trafiają do `ops.sensor_quarantine` osobnym flowem z odwróconym warunkiem
(wzorzec „quarantine table" — expectations same w sobie nie zapisują odrzuconych wierszy).

**Krok 3b — reguły wymagające historii urządzenia (`silver/sensor_quality.py`) (poprawka P2)**

Rolling z-score, zamrożony odczyt i `max_delta_per_reading` potrzebują poprzednich odczytów
tego samego urządzenia. Funkcje okna po wierszach (`lag`, `rows between`) **nie są obsługiwane**
w Structured Streaming, więc te reguły liczone są w **materialized view**
`silver_sensor_quality` nad `silver_sensor_clean` (przeliczanym przyrostowo przez Lakeflow):

```python
w = Window.partitionBy("device_id").orderBy("event_time")
w_roll = (Window.partitionBy("device_id")
          .orderBy(F.col("event_time").cast("long"))
          .rangeBetween(-120 * 60, -1))     # 120 min wstecz, bez bieżącego odczytu

quality = (clean
    .withColumn("roll_mean", F.avg("dose_rate_usv_h").over(w_roll))
    .withColumn("roll_std",  F.stddev("dose_rate_usv_h").over(w_roll))
    .withColumn("z_score", (F.col("dose_rate_usv_h") - F.col("roll_mean")) / F.col("roll_std"))
    .withColumn("same_as_prev", F.col("dose_rate_usv_h") == F.lag("dose_rate_usv_h").over(w))
    # długość serii identycznych wartości -> frozen_reading
    ...)
```

Wynik: kolumna `quality_status ∈ {clean, drift_suspect, frozen, warmup}` — dryf trafia do
kwarantanny (nie jest usuwany), frozen jest tylko flagą. Na demo i do alertów to wystarcza —
opóźnienie MV (minuty) jest akceptowalne, bo alert i tak porównuje z przedziałem godzinowym.

**Krok 4 — testy (`tests/test_sensor_dq.py`)**

Testy na *zasymulowanych* przypadkach, nie na losowym streamie — deterministyczne, z ustalonym
seedem. Dzięki temu, że spóźnienie jest liczone z `sent_at` (a nie przez watermark), testy
wywołują czyste funkcje na statycznych DataFrame'ach i nie zależą od podziału na mikro-batche:
- odczyt z `lag = 10 min` → **akceptowany**
- odczyt z `lag = 20 min` → **trafia do `sensor_late_rejected`**
- ten sam `(device_id, event_time)` dwa razy → **jeden rekord w silver**
- wiatr 80 m/s / dawka poza zakresem → **odrzucone, obecne w `sensor_quarantine` z powodem**
- powolny dryf +1%/min przez 2h → **`drift_suspect` (z-score), kwarantanna, nie drop**
- ta sama wartość 12 razy z rzędu → **oflagowana jako `frozen`**
- pierwsze 10 min nowego urządzenia → **`warmup`, bez reguły dryfu**

Osobno `tests/test_sensor_sim.py`: ten sam seed → identyczne pliki; mediana dawki z generatora
mieści się w P5–P95 modelu dla ~90% odczytów bez wstrzykniętych odchyleń.

#### 3.5.4 Co to daje na demo

- **żywy pokaz na PoC lokalnym**: uruchamiasz generator, widać w konsoli/Sparku, że część danych
  przychodzi z opóźnieniem i jest odrzucana albo kwarantannowana w czasie rzeczywistym
- ta sama demonstracja **bez zmian w logice** działa na Databricks, tylko źródłem jest
  Auto Loader zamiast katalogu plików — dokładnie to pokazuje dojrzałość architektury
- naturalnie domyka też wymóg kursu "schema evolution and data quality handled" mocniejszym
  przykładem niż suche `expectations` na czystych danych

### 3.6 Propozycje dodatkowych transformacji i anomalii (do rozważenia, nieobowiązkowe) ⭐ NOWE

Poniższe rozszerzają 3.5 o dodatkowe mechanizmy czyszczenia — każdy to osobny, konkretny wzorzec
inżynierski, dobry pod rubrykę "engineering quality". Nie trzeba implementować wszystkich;
warto wybrać 2–3, które dają najwięcej różnorodności bez rozdmuchania zakresu.

**Jakość pojedynczego odczytu**
- **Cross-sensor validation** — porównanie odczytu wiatru z jednego czujnika ze średnią
  sąsiadów w promieniu np. 5 km w tym samym momencie; odchylenie >3σ od sąsiadów = podejrzenie
  błędu lokalnego czujnika, a nie realnego zjawiska pogodowego
- **Spójność wewnętrzna rekordu** — np. `battery = 0`, a urządzenie mimo to wysyła dane =
  sprzeczność, osobna reguła walidacyjna
- **Błędna jednostka po "aktualizacji firmware"** — zasymulowany czujnik, który w pewnym
  momencie zaczyna wysyłać dane w złej jednostce (np. km/h zamiast m/s); wykrywane po nagłej
  zmianie rzędu wielkości całej serii czasowej, nie pojedynczego odczytu

**Duplikaty i niespójność źródeł**
- **Duplikaty z równoległych ścieżek ingestu** — ten sam czujnik przy niestabilnym łączu wysyła
  czasem przez WiFi, czasem przez zapasowe LTE → dwa rekordy z bardzo bliskim `event_time`,
  różnym `ingestion_path` → dedup z regułą "zachowaj rekord z mniejszym opóźnieniem"
- **Out-of-order w obrębie jednego urządzenia** — nie tylko całe spóźnione paczki, ale odczyty
  w złej kolejności wewnątrz jednej paczki (rozjeżdżający się zegar urządzenia) →
  sortowanie + walidacja monotoniczności `event_time` per urządzenie

**Metadane i cykl życia urządzenia**
- **Nowe urządzenie bez historii** — pierwsze ~10 min danych z nowego `device_id` nie ma
  jeszcze baseline'u do wykrywania dryfu → osobna ścieżka "warm-up", nieobjęta regułami
  dryfu/z-score dopóki baseline nie powstanie
- **Firmware version drift** — część floty ma symulowany starszy firmware z inną precyzją
  albo inną jednostką → schema evolution + reguła DQ uzależniona od `firmware_version`

**Na poziomie agregacji (silver → gold)**
- **Gap filling z jawnym oznaczeniem** — gdy czujnik milczy dłużej niż próg, interpolacja
  z sąsiadów przestrzennych do celów dashboardu, ale zawsze z kolumną `is_imputed = true` —
  pokazuje świadome rozróżnienie pomiaru od szacunku, bez ukrywania tego przed użytkownikiem
- **Confidence score per agregat** — `gold_live_alerts` z dodatkową kolumną: jaki % czujników
  w danym oknie czasowym było `clean` vs `quarantined` — dashboard pokazuje nie tylko wynik,
  ale i jego wiarygodność

**Najmocniejszy pojedynczy dodatek (rekomendowany, jeśli starczy czasu)**
- **Stanowy auto-recovery czujnika** — urządzenie oznaczone jako `quarantined` po serii złych
  odczytów wraca do stanu `healthy` dopiero po np. 5 kolejnych "zdrowych" odczytach z rzędu.
  Wymaga logiki **stanowej**, nie tylko bezstanowego filtrowania — solidny techniczny wyróżnik
  i naturalny temat do Q&A na demo. Implementacja (poprawka P2):
  - **rekomendowane:** tabela stanu `ops.device_health` (Delta, klucz `device_id`) aktualizowana
    w `foreachBatch` przez `MERGE` — czysty Spark SQL, zgodne z zakazem pandas/UDF z 3.1,
    łatwe do przetestowania
  - alternatywa: `transformWithStateInPandas` / `applyInPandasWithState` — w PySpark
    **nie ma** `flatMapGroupsWithState` (tylko Scala/Java); wymaga świadomego wyjątku od zakazu
    pandas z 3.1, opisanego w write-upie

---

## CZĘŚĆ IV — Faza 1: Wdrożenie na Databricks

### 4.1 Infrastruktura (dzień 1)

1. Azure: resource group, ADLS Gen2, Key Vault, dwa workspace (DEV, PROD)
2. Unity Catalog: metastore, katalogi `radplume_dev` / `radplume_prod`, external location, volumes
3. Key Vault: sekrety — token API, poświadczenia modelu, connection stringi;
   podpięte przez secret scope (backed by Key Vault, **nie** Databricks-backed)
4. Service principal dla CI z uprawnieniami tylko do deployu
5. **Nic w PROD nie tworzone ręcznie poza jednorazowym bootstrapem** — reszta z bundla (poprawka P6)

**Jednorazowy bootstrap (jawnie wymieniony w README, najlepiej jako skrypt/Terraform w repo):**
metastore i przypięcie workspace'ów, storage credential + external location, service principal
CI, grupy kontowe `regulator` / `public_analyst`, secret scope oparty o Key Vault (jeśli API
nie pozwala utworzyć go jako SP — utworzyć raz skryptem jako admin i opisać to w write-upie
jako świadomy wyjątek).

**Wszystko inne idzie przez bundle/CI:** schematy, volumes, pipeline'y, joby, dashboard, app,
a także **RLS/CLS i granty** — `sql/governance.sql` uruchamiany jako task Joba (4.5).
Nikt nie klika `ALTER TABLE ... SET ROW FILTER` w SQL editorze na PROD.

### 4.2 Asset Bundle

```yaml
targets:
  dev:
    mode: development
    variables: {catalog: radplume_dev, grid_size: 60, n_param_scenarios: 30}
  prod:
    mode: production
    variables: {catalog: radplume_prod, grid_size: 200, n_param_scenarios: 500}
    permissions: [...]
```

Ta sama różnica DEV/PROD co lokalnie: **tylko zmienne**.

### 4.3 Pipeline deklaratywny (Lakeflow) — ścieżka batch

Warstwy jako tabele streamingowe / zmaterializowane widoki, z expectations:

```python
@dlt.expect_or_drop("wind_physical", "wind_speed_ms BETWEEN 0 AND 120")
@dlt.expect_or_fail("no_negative_concentration", "concentration >= 0")
@dlt.expect("mass_balance", "abs(total_mass - released_mass) / released_mass < 0.05")
```

Trzy poziomy reakcji (`expect` / `expect_or_drop` / `expect_or_fail`) świadomie zróżnicowane —
to punkt do omówienia na demo.

### 4.4 Pipeline streamingowy — ścieżka monitoringu

**Symulator** (task Joba) generuje odczyty czujników:
`[device_id, event_time, sent_at, dose_rate_usv_h, wind_speed_ms, battery, (detector_temp_c)]`,
z dawką wyprowadzoną z modelu (3.5.3, poprawka P3) i celowo wstrzykiwanymi anomaliami.
Pozycja, `cell_id`, `jurisdiction_code` i `firmware` urządzenia **nie** jadą w każdym odczycie —
pochodzą z rejestru urządzeń (ścieżka CDC poniżej).

**Ingest (poprawka P9):** Auto Loader na volume `landing/sensor` z
`schemaEvolutionMode = addNewColumns` — tu żyje schema evolution (firmware v2 dodaje
`detector_temp_c`). Zerobus zapisuje bezpośrednio do istniejącej tabeli Delta o **stałym**
schemacie, więc nie zastępuje Auto Loadera w tej roli — jest **opcjonalną** równoległą ścieżką
(`bronze_sensor_zerobus`), jeśli jest dostępny w regionie workspace'u.

**Bronze → Silver:** podział po `lag` (late → `ops.sensor_late_rejected`), deduplikacja po
`(device_id, event_time)` z watermarkiem ograniczającym stan, reguły twarde z kwarantanną,
join z `silver_devices` (aktualna wersja SCD2) — szczegóły w 3.5.3.
Maskowanie `device_id` i dokładnych współrzędnych robi `COLUMN MASK` w UC (4.6), nie
transformacja — dane w silver pozostają pełne dla roli `regulator`.

**Ścieżka CDC — rejestr urządzeń (zdolność zaawansowana, poprawka P8):**
`device_registry.py` generuje feed zmian (`INSERT` nowego urządzenia, `UPDATE` firmware
v1 → v2, zmiana jurysdykcji, `DELETE` = wycofanie) z kolumną `op` i `seq`. W Lakeflow:

```python
dlt.create_streaming_table("silver_devices")
dlt.create_auto_cdc_flow(            # dawniej apply_changes
    target="silver_devices",
    source="bronze_device_cdc",
    keys=["device_id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("op = 'DELETE'"),
    stored_as_scd_type=2,
)
```

Daje to: historię wersji firmware (która reguła DQ obowiązywała w danym momencie),
poprawne wycofanie urządzenia, idempotentną obsługę powtórzonych zmian — i domyka wymóg
„advanced capability” niezależnie od dostępności Zerobusa.

**Gold (`gold_live_alerts`):** materialized view — join `silver_sensor_quality` z `gold_risk_map`
po `(cell_id, godzina)`; alert, gdy pomiar wypada poza przedział P5–P95 modelu. (Skoro
`silver_sensor_quality` jest MV, alerty też są MV, a nie stream-static joinem — odświeżane
przy każdym przebiegu pipeline'u, na demo w trybie triggered co kilka minut.)
**To jest najmocniejszy punkt demo:** pokazuje, że obie ścieżki się spotykają. Ma sens tylko
dlatego, że generator wyprowadza dawkę z modelu (P3) — patrz też ograniczenie w Części VIII.

### 4.4a Symulacja brudnych danych na Databricks ⭐ NOWE

Ten sam generator co lokalnie (3.5.3), uruchomiony jako task Joba, zapisuje pliki do volume
`landing/sensor` zamiast do katalogu lokalnego. Logika czyszczenia w `silver_sensor_clean`
jest **identyczna** z tą przetestowaną lokalnie — to jest właśnie dowód, że zasady
przenaszalności z 3.1 się opłaciły.

Dodatkowo w chmurze:
- `ops.sensor_late_rejected`, `ops.sensor_quarantine` i `ops.device_health` jako osobne tabele
  Delta, widoczne na dashboardzie (punkt 5 w 4.7)
- `gold_live_alerts.alert_type` rozróżnia: `outside_model_band` (realny sygnał — pomiar
  czysty, ale poza P5–P95) od `sensor_fault` (odczyt w kwarantannie — DQ, nie sygnał fizyczny).
  Alert `outside_model_band` nie może powstać z odczytu o statusie innym niż `clean` — to
  rozróżnienie samo w sobie jest dobrym tematem do omówienia w Q&A na demo

### 4.5 Lakeflow Job — orkiestracja

```
apply_governance (sql/governance.sql) ──► ingest_meteo (REST) ──► generate_scenarios ──► pipeline_batch
                                                                                            │
                                                                                      gold_validation
                                                                                            │
                                                                        export_expected_dose (dla symulatora)
                                                                                            │
                                                                                refresh_vector_index ──► notify

job_sensor_demo:  device_registry (CDC) ──► sensor_sim ──► pipeline_stream (triggered)
```

`apply_governance` to task typu SQL (`sql_task` z plikiem) — idempotentny
(`CREATE OR REPLACE FUNCTION`, `GRANT`, oraz `ALTER TABLE ... SET ROW FILTER / SET MASK`
dla zwykłych tabel Delta zapisywanych `MERGE`-em). Dla tabel zarządzanych przez pipeline
Lakeflow (streaming tables / MV) filtr i maskę deklaruje się **w definicji tabeli**
(`@dlt.table(row_filter=..., schema="... MASK ...")`), odwołując się do funkcji z
`governance.sql` — dlatego `apply_governance` (tworzący funkcje) musi się wykonać przed
pierwszym przebiegiem pipeline'u. Składnię zweryfikować w aktualnej dokumentacji.

Idempotencja:
- batch: `MERGE` po kluczu naturalnym (`site_id, episode_id, scenario_id, cell_id, nuclide, ts`),
  seedowany RNG, brak `append` bez klucza
- streaming: bronze append-only (niezmienny), idempotencję w silver zapewnia checkpoint +
  deduplikacja po `(device_id, event_time)`; rejestr urządzeń przez `AUTO CDC` z `sequence_by`
- test `test_idempotency.py` uruchamia pipeline dwa razy i porównuje sumy kontrolne

### 4.6 Governance

- **RLS (poprawka P5):** `ROW FILTER` na `gold_risk_map`, `gold_live_alerts` i
  `silver_sensor_clean` po `jurisdiction_code` vs grupa użytkownika
  (`is_account_group_member('jur_JP-07')` itd.; `regulator_national` widzi wszystko).
  Lokalizacja europejska sprawia, że filtr jest widoczny na demo: ten sam dashboard,
  dwóch użytkowników, różne wiersze
- **CLS:** `COLUMN MASK` na `source_term_bq` i na `device_id` / dokładnych współrzędnych czujników
  (dla `public_analyst`: hash `device_id`, współrzędne zaokrąglone do komórki siatki)
- role: `regulator` (pełny dostęp), `public_analyst` (maskowany), `service_principal_ci`
- funkcje filtrów/masek i granty w `sql/governance.sql`, wdrażane Jobem; podpięcie do tabel
  pipeline'u w ich definicji w kodzie — w obu przypadkach z repo, nigdy ręcznie (poprawka P6)
- lineage z Unity Catalog jako element prezentacji architektury

### 4.7 Dashboard (AI/BI)

Pytania, na które odpowiada:
1. Mapa prawdopodobieństwa przekroczenia progu — wybór lokalizacji, nuklidu, okna czasowego
2. Ranking lokalizacji wg oczekiwanej liczby narażonych mieszkańców
3. Róża ryzyka — sektor × miesiąc
4. **Jakość modelu:** coverage, FAC2/FAC5, rozbicie błędu wg opadu
5. Stan sieci monitoringu, aktywne alerty **oraz metryki jakości danych** (% odczytów
   spóźnionych, % odrzuconych jako anomalia, liczba czujników w kwarantannie) — dobre,
   namacalne info dla oceniającego, że DQ jest realnie mierzone, nie tylko zaimplementowane

Punkt 4 jest nietypowy i warty podkreślenia: dashboard raportuje **ograniczenia własnego modelu**.

### 4.8 Aplikacja AI

**Router** klasyfikuje intencję:
- **liczby** → text-to-SQL na tabelach Gold — model z **Foundation Model API** (pay-per-token)
  z promptem zawierającym schemat Gold i przykładowe zapytania (poprawka P7)
- **metodologia / fakty** → RAG wektorowy (Vector Search) na dokumentach: IAEA, UNSCEAR, ICRP,
  opisy modelu gaussowskiego i klas Pasquilla, **własna dokumentacja projektu i wyniki walidacji**
- **mieszane** → oba konteksty

**Zabezpieczenia text-to-SQL (do opisania w write-upie jako guardrails):**
- whitelista tabel i kolumn wymuszana przez parser (sqlglot), nie przez prompt
- tylko `SELECT`, wymuszony `LIMIT`, timeout
- walidacja składni przed wykonaniem + jedna pętla retry z komunikatem błędu
- wygenerowany SQL pokazywany użytkownikowi
- zapytania wykonywane z tożsamością użytkownika (lub SP z uprawnieniami `public_analyst`),
  więc RLS/CLS obowiązuje także w aplikacji AI

**Hosting modelu (poprawka P7):** Foundation Model API w trybie pay-per-token — brak
utrzymywanego endpointu GPU, koszt rzędu kilku dolarów za cały projekt, zero ryzyka
„cold startu” na demo. Alternatywa do rozważenia: Genie space na tabelach Gold jako
backend części liczbowej. Decyzja kosztowa do write-upu.

**Do README:** własny zestaw ewaluacyjny `eval/text_to_sql_eval.yaml` (20–30 pytań do Golda
z oczekiwanym wynikiem) i odsetek poprawnych odpowiedzi (execution accuracy) — tani, mierzalny
i dotyczy Twoich tabel, a nie ogólnego benchmarku.

**Opcjonalnie, tylko jeśli zostanie czas (blok 10):** dostrojony Qwen serwowany jako
external model + benchmark Spider z rozbiciem na poziomy trudności, porównany na tym samym
zestawie ewaluacyjnym z modelem z Foundation Model API. Nie jest wymagany przez specyfikację
i nie może blokować żadnego innego bloku.

### 4.9 CI/CD (GitHub Actions)

```
PR:    lint (ruff) → testy jednostkowe (pytest + chispa) → bundle validate → deploy do DEV → smoke test
main:  wszystko powyżej → bundle deploy -t prod → job weryfikacyjny na próbce
```

Uwierzytelnienie: OIDC do Azure, service principal, zero sekretów w repo.

---

## CZĘŚĆ V — Koszty

**Wcześniejsze szacunki nie obowiązują** — wymóg płatnego PROD zmienia obraz.

| Pozycja | Szacunek |
|---|---|
| PoC lokalny | 0 |
| DEV (trial $400, 14 dni) | 0, jeśli zmieścisz się w oknie |
| PROD — przebiegi wsadowe | 10–40 $ |
| PROD — klaster streamingowy | **największe ryzyko** — 20–60 $/tydz. jeśli chodzi ciągle |
| Storage ADLS (100–300 GB) | 3–8 $/mies. |
| Foundation Model API (pay-per-token) + Vector Search | 5–30 $ (Vector Search endpoint wyłączany poza pracą/demo) |
| *opcjonalnie:* serwowanie dostrojonego modelu (blok 10) | 20–80 $ — tylko jeśli blok 10 jest realizowany |
| Key Vault, sieć | < 5 $ |

**Realnie: 60–200 $** przy dyscyplinie. Bez dyscypliny — wielokrotnie więcej.

**Pięć zasad oszczędzania:**
1. Klaster streamingowy **tylko na czas demo i testów**; `Trigger.AvailableNow` zamiast ciągłego
   `processingTime` w codziennej pracy
2. Auto-termination 10 min wszędzie; jobs compute zamiast all-purpose do pipeline'ów
3. DEV pracuje na `grid_size: 60` — pełna skala tylko w PROD i tylko wtedy, gdy kod działa
4. Jeden pełny przebieg PROD, nie dziesięć; zrzuty ekranu Spark UI robione za pierwszym razem
5. Pay-per-token zamiast własnego endpointu; Vector Search i ewentualny endpoint modelu
   wyłączane/skalowane do zera poza demo

**Sanity check skali:** nie celuj w miliardy wierszy. `grid 200×200 × 72h × 500 scenariuszy ×
3 nuklidy` z agregacją epizodową to kilkaset milionów wierszy — w zupełności wystarczy,
by uzasadnić Sparka i pokazać partycjonowanie. Kilka miliardów nie doda punktów, doda rachunek.

---

## CZĘŚĆ VI — Kolejność prac

| Blok | Zakres | Czas |
|---|---|---|
| 0 | PoC lokalny (etapy 1–11, w tym symulator brudnych danych) | 6–7 dni |
| 1 | Infra Azure + UC + Key Vault + szkielet bundla | 1 dzień |
| 2 | Migracja batch: pipeline deklaratywny + expectations | 1–2 dni |
| 3 | CI/CD: testy w GitHub Actions, deploy do DEV | 1 dzień |
| 4 | Ścieżka streamingowa: symulator (dawka z modelu) + Auto Loader + czyszczenie + MV jakości + alerty | 2–3 dni |
| 4b | Ścieżka CDC: rejestr urządzeń + `AUTO CDC` SCD2 | 0.5 dnia |
| 5 | Governance: `sql/governance.sql` jako task Joba, RLS/CLS, role, lineage | 0.5–1 dnia |
| 6 | Dashboard (w tym metryki jakości danych) | 1 dzień |
| 7 | Aplikacja AI: router, text-to-SQL (Foundation Model API), Vector Search, zestaw ewaluacyjny | 2 dni |
| 8 | Deploy do PROD, pełny przebieg, strojenie | 1 dzień |
| 9 | README, diagram architektury, write-up, próba demo (Część X) | 1–2 dni |
| 10 | *Opcjonalnie:* dostrojony Qwen + benchmark Spider | tylko nadwyżka czasu |

**Ścieżka krytyczna:** blok 0 → 2 → 3. Jeśli czasu zabraknie, tnij najpierw blok 10, potem
skalę (mniejsza siatka, mniej scenariuszy, jedna lokalizacja JP + jedna UE) — **nie** komponenty
wymagane przez specyfikację. Bloku 4b nie tnij: to on gwarantuje „advanced capability”.
**Symulatora brudnych danych (3.5) nie tnij** — to teraz najsilniejszy element projektu pod
kątem oceny inżynierskiej.

**Uwaga do fizyki:** rubryka nie punktuje wyrafinowania modelu dyspersji, tylko testy i jakość
danych. Model gaussowski z klasami Pasquilla–Gifforda + walidacja FAC2/FAC5 wystarczą —
nie rozbudowuj fizyki kosztem bloków 4–7.

---

## CZĘŚĆ VII — Checklista pod rubrykę oceny

**End-to-end correctness**
- [ ] pipeline uruchomiony dwukrotnie daje identyczne sumy kontrolne (`test_idempotency`)
- [ ] `MERGE` po kluczu naturalnym w batchu, żaden ślepy `append` poza append-only bronze
- [ ] streaming: dedup po `(device_id, event_time)`, spóźnione rekordy w jawnej tabeli (nie gubione przez watermark)
- [ ] Bronze niezmienny — Silver i Gold odtwarzalne od zera

**Governance & security**
- [ ] RLS po `jurisdiction_code` (widoczny efekt: JP vs UE), CLS na `source_term_bq` i `device_id`
- [ ] RLS/CLS i granty wdrażane z `sql/governance.sql` przez Job — nie ręcznie
- [ ] secret scope backed by Key Vault, zero sekretów w repo
- [ ] CI działa jako service principal z minimalnymi uprawnieniami
- [ ] jednorazowy bootstrap wypisany w README

**Engineering quality**
- [ ] testy fizyki z wartościami analitycznymi + bilans masy
- [ ] testy schematów, testy idempotencji
- [ ] **testy DQ czujników: lag 10/20 min, duplikat, spike, dryf, frozen, warm-up**
- [ ] test generatora: seed → identyczny wynik, dawka zgodna z pasmem modelu
- [ ] logika w modułach, notebooki tylko jako cienka warstwa wywołań
- [ ] czysta historia gita, konwencjonalne commity, PR-y

**Automation**
- [ ] `databricks bundle deploy -t prod` to jedyny sposób na zmianę w PROD
- [ ] zielony pipeline CI widoczny w repo

**Advanced capability**
- [ ] CDC: `AUTO CDC` SCD2 na rejestrze urządzeń, działa w PROD
- [ ] *(opcjonalnie)* Zerobus jako równoległa ścieżka ingestu

**AI & analytics**
- [ ] dashboard odpowiada na 5 pytań z sekcji 4.7, w tym metryki jakości danych
- [ ] aplikacja AI działa na żywo, pokazuje wygenerowany SQL i cytuje źródła, respektuje RLS
- [ ] wynik zestawu ewaluacyjnego text-to-SQL w README

**Communication**
- [ ] diagram architektury (ścieżki batch, streaming i CDC)
- [ ] write-up: decyzje projektowe i kompromisy kosztowe/wydajnościowe
- [ ] write-up — **dwie osobne sekcje** (poprawka P10):
  - *AI-assisted development:* gdzie asystent AI pomógł (np. szkielet bundla, testy, SQL
    governance) i jakie guardrails stosowano w pracy z nim: review każdej zmiany, testy przed
    merge, zakaz sekretów w promptach i kodzie, CI jako bramka, weryfikacja API w aktualnej
    dokumentacji
  - *Guardrails aplikacji AI:* whitelista sqlglot, tylko `SELECT`, `LIMIT`, timeout, RLS
    w zapytaniach, widoczny SQL
- [ ] **sekcja ograniczeń modelu** — punktowana jako "honest discussion of trade-offs"

---

## CZĘŚĆ VIII — Ograniczenia modelu (do README, pisać od początku)

- model gaussowski zakłada **płaski teren** — rejon Fukushimy jest górzysty
- zakłada **jednorodny wiatr** w domenie — mitygowane segmentacją godzinową, nie usunięte
- **uproszczona depozycja mokra** — a to opad zdecydował o faktycznym rozkładzie skażenia
- **niepewność źródła rzędu ×10** — szacunki w literaturze różnią się kilkukrotnie
- Safecast mierzy **moc dawki, nie depozycję** — konwersja wnosi dodatkowy błąd
- brak chemii atmosferycznej i przemian form chemicznych jodu
- dane ze ścieżki streamingowej, w tym awarie łączności i anomalie czujników, są **symulowane**
  na podstawie realistycznych wzorców, nie są to dane z prawdziwej sieci monitoringu
- **dawka w symulatorze jest wyprowadzona z samego modelu** (P3) — porównanie „pomiar na żywo
  vs model” w `gold_live_alerts` weryfikuje więc **działanie pipeline'u i logiki alertów**,
  a nie trafność modelu. Trafność modelu ocenia wyłącznie walidacja na niezależnych danych
  (Safecast/MEXT, `gold_validation`). Napisać to wprost — to typowe pytanie na Q&A

**Wniosek do zapisania wprost:** narzędzie służy do porównywania scenariuszy i szacowania
kierunków ryzyka, **nie do przewidywania stężeń**. Nie jest systemem wspomagania decyzji
kryzysowych ani produktem regulacyjnym.

---

## CZĘŚĆ IX — Ryzyka

| Ryzyko | Mitygacja |
|---|---|
| Walidacja wypadnie słabo | to nadal wynik — opisać dlaczego; rubryka nagradza uczciwość |
| Klaster streamingowy przepala budżet | `Trigger.AvailableNow`, włączany tylko na demo |
| Rozjazd CRS między siatką a rastrem ludności | wspólny EPSG ustalony na starcie, zapisany w schemacie |
| Text-to-SQL halucynuje na demo | ograniczony schemat, przygotowane pytania zapasowe, widoczny SQL |
| Trial DEV wygasa w trakcie | trial dopiero po ukończeniu PoC lokalnego |
| Eksplozja objętości w trybie klimatologicznym | brak utrwalania Silvera godzinowego, grubsza siatka |
| Zerobus/Lakeflow API zmienione od czasu materiałów kursu | zweryfikować w aktualnej dokumentacji przed implementacją |
| Symulator brudnych danych za bardzo losowy — trudno pokazać deterministyczny przypadek na demo | seedowany RNG w generatorze; przygotowany osobny "scenariusz demo" z ręcznie wywołaną awarią i anomalią w konkretnym momencie |
| Zerobus niedostępny w regionie Azure / w preview | ścieżka główna to Auto Loader; zdolność zaawansowana zapewniona przez CDC — Zerobus tylko bonus |
| „REST API automation” niezaliczone, bo to ingest danych, a nie automatyzacja Databricksa | nie opierać na tym wymogu; CDC jako zdolność zaawansowana; ewentualnie dopytać prowadzącego |
| Secret scope Key Vault nie da się utworzyć jako SP | jednorazowy bootstrap skryptem, opisany w README jako świadomy wyjątek |
| Pełny przebieg PROD trwa za długo na żywo | przebieg pełnej skali wykonany przed demo; na żywo tylko deploy + krótki scenariusz strumieniowy (Część X) |
| Row filter / mask zdjęty lub niemożliwy do nałożenia `ALTER`-em na tabelę Lakeflow | filtry i maski dla tabel pipeline'u w definicji tabeli; `apply_governance` (funkcje, granty, zwykłe tabele Delta) idempotentny, jako pierwszy task Joba |

---

## CZĘŚĆ X — Scenariusz demo (20–30 min) ⭐ NOWE

Specyfikacja wymaga pokazania: architektury, **żywego deployu DEV → PROD przez CI/CD**,
przebiegu pipeline'u z jakością danych, dashboardu i aplikacji AI. Pełna skala PROD liczona
**przed** demo — na żywo tylko to, co trwa minuty.

| Min | Blok | Co pokazać | Przygotowane wcześniej |
|---|---|---|---|
| 0–4 | Architektura | diagram (batch + streaming + CDC), UC: katalogi DEV/PROD, lineage | diagram, zakładka lineage |
| 4–9 | CI/CD na żywo | merge małego PR (np. zmiana progu w `sensor_dq.yaml`) → GitHub Actions: lint, testy, `bundle validate`, deploy DEV → deploy PROD | PR otwarty, CI zielone na DEV; w tle zdjęcie poprzedniego przebiegu na wypadek awarii |
| 9–16 | Pipeline + DQ | `job_sensor_demo` z `run_demo_scenario()`: paczka po 20 min awarii → `sensor_late_rejected`, spike → `sensor_quarantine`, dryf → `drift_suspect`, firmware v2 → nowa kolumna (schema evolution), zmiana w rejestrze → SCD2 w `silver_devices`; widok expectations w UI pipeline'u | seed i momenty anomalii ustalone; pipeline rozgrzany |
| 16–21 | Dashboard | mapa ryzyka, ranking, jakość modelu (FAC2/FAC5), metryki DQ, alert `outside_model_band` vs `sensor_fault`; **RLS**: ten sam dashboard jako użytkownik JP i UE | dwa konta testowe w różnych grupach |
| 21–26 | Aplikacja AI | 1 pytanie liczbowe (widoczny SQL), 1 metodologiczne (cytaty ze źródeł), 1 mieszane | lista pytań sprawdzonych na zestawie ewaluacyjnym + pytania zapasowe |
| 26–30 | Trade-offs | koszty, ograniczenia modelu (w tym: symulowane czujniki ≠ walidacja modelu), rola AI w developmencie | slajd z Części VIII i V |

**Zasada:** każdy krok na żywo ma zrzut ekranu/nagranie jako plan B. Q&A: przygotować
odpowiedzi na „dlaczego nie sam watermark?”, „czy alerty dowodzą trafności modelu?”,
„co jest ręcznie w PROD?”.
