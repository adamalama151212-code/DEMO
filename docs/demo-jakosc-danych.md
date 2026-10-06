# Scenariusz demo: jak potok łapie i czyści popsute dane

> **Dla kogo:** autor projektu, na Demo Day (wymóg kursu: „a pipeline run with data quality”).
> **Stan na:** 2026-10-06. Demo działa lokalnie (`smogcast run-fault-demo`); Job na Databricks (`smogcast-fault-demo`)
> jest zaplanowany w E9 — zapytania SQL niżej działają w obu miejscach (lokalnie przez `smogcast show …`, na
> Databricks w SQL Editorze z katalogiem `smogcast_dev`).
> Liczby — z prawdziwego przebiegu (`PLAN_SMOG.md`, dziennik E6, weryfikacja końcowa); każdy przebieg daje to samo.

## O co chodzi (1 zdanie na start)

Bierzemy **prawdziwe** godzinowe pomiary PM10 z 6 stacji w Krakowie z 17–21 stycznia 2025 (w tym prawdziwy epizod
smogowy 20.01), odtwarzamy je jako strumień — tak, jak przychodziłyby z API — i **celowo psujemy transport i pojedyncze
odczyty** według harmonogramu. Potok musi każdą usterkę odłożyć we właściwe miejsce, a prawdziwego smogu nie ruszyć.

Dlaczego wstrzykujemy: archiwum GIOŚ jest bardzo czyste (2021–2025: 0 odczytów do kwarantanny na 11,4 mln), więc bez
tego nie byłoby czego pokazać. Popsute dane idą **osobnym torem** (`source = 'fault_replay'`) i nigdy nie trafiają do
danych modelu (decyzja D17).

## Krok 1 — pokaż „zamówienie”: harmonogram usterek

Plik [`packages/core/src/smogcast/core/conf/fault_injection.yaml`](../packages/core/src/smogcast/core/conf/fault_injection.yaml):

| Usterka (godzina okna) | Co symuluje | Gdzie ma wylądować |
|---|---|---|
| `outage` (h 6, 8 h) | stacja traci łączność, potem wysyła zaległe odczyty | `ops.pm_late_rejected` |
| `duplicate_batch` (h 20) | ta sama paczka wysłana dwa razy | **jeden** rekord w `silver.pm_stream` |
| `negative` (h 26) | wartość ujemna | `ops.pm_quarantine`, `below_min` |
| `out_of_range` (h 27, 1500) | wartość fizycznie niemożliwa (> 1000 µg/m³) | `ops.pm_quarantine`, `above_max` |
| `missing_value` (h 28) | stacja działa, wartość pusta | `ops.pm_quarantine`, `missing_value` |
| `frozen` (h 32, 9 h) | czujnik „zawiesił się” na jednej wartości | flaga `frozen` |
| `spike` (h 44, ×6) | pojedyncza godzina 6× za wysoka | flaga `spike` |
| `drift` (h 50, 40 h, +4%/h) | czujnik powoli się rozkalibrowuje | flaga `drift` |
| `registry_change` (h 60) | stacja znika z rejestru, inna dostaje nowy czujnik | CDC → historia SCD2 stacji |

**Co powiedzieć:** „Najpierw zapisujemy, czego oczekujemy — dopiero potem uruchamiamy. To test, nie pokaz slajdów.”

## Krok 2 — uruchom

```powershell
docker compose run --rm smogcast smogcast run-fault-demo
```

Kolejność (6 kroków, ok. 2–3 min): reset tabel demo → odtworzenie z usterkami → migawki rejestru → SCD2 stacji →
czyszczenie strumienia → podsumowanie jakości. **Zaczyna od resetu**, więc każdy przebieg daje ten sam wynik — można
uruchomić na żywo bez ryzyka. Źródło `live` (prawdziwe API) nie jest dotykane.

## Krok 3 — pokaż wynik, usterka po usterce

Lokalnie: `smogcast show <warstwa> <tabela>`. Na Databricks: SQL niżej (katalog `smogcast_dev`).

### Spóźnione odczyty — 4 odrzucone

```sql
SELECT station_code, time_utc, value, ROUND(lag_hours, 1) AS lag_hours
FROM smogcast_dev.ops.pm_late_rejected
WHERE source = 'fault_replay' ORDER BY time_utc;
```
**Co powiedzieć:** „Zaległe odczyty przyszły 4–7 godzin po czasie. Spóźniony to nie każdy stary odczyt — API GIOŚ
przy każdym odpytaniu wysyła ostatnie 3 doby, więc powtórka to norma. Spóźniony jest odczyt, który **powinien był
przyjść już przy poprzednim odpytaniu**, a nie przyszedł.”

### Kwarantanna z powodem

```sql
SELECT quarantine_reason, COUNT(*) AS readings, MIN(value) AS min_value, MAX(value) AS max_value
FROM smogcast_dev.ops.pm_quarantine
WHERE source = 'fault_replay' GROUP BY quarantine_reason;
```
Oczekiwane: `below_min` (wartość ujemna), `above_max` (1500 µg/m³), `missing_value` (pusta wartość).
**Co powiedzieć:** „Nic nie znika po cichu: każdy odrzucony odczyt jest zachowany z powodem — można go przejrzeć,
policzyć i wyjaśnić.”

### Duplikat paczki — jeden rekord

```sql
SELECT station_code, time_utc, COUNT(*) AS rows
FROM smogcast_dev.silver.pm_stream
WHERE source = 'fault_replay' GROUP BY station_code, time_utc HAVING COUNT(*) > 1;
```
Oczekiwane: **0 wierszy** — paczka wysłana dwa razy dała jeden rekord (deduplikacja po stacji i godzinie, zapis MERGE).

### Flagi: zamrożony czujnik, skok, dryf

```sql
SELECT dq_flag, station_code, COUNT(*) AS hours, MIN(time_utc) AS first_hour, MAX(time_utc) AS last_hour
FROM smogcast_dev.silver.pm_stream
WHERE source = 'fault_replay' AND dq_flag <> 'ok'
GROUP BY dq_flag, station_code ORDER BY first_hour;
```
Oczekiwane: `frozen` — 9 godzin, `spike` — 1 godzina, `drift` — 26 godzin, **tylko na stacji z dryfem**.
**Co powiedzieć:** „Te odczyty nie są usuwane, tylko oznaczone — `is_valid = false` wyklucza je z prognozy, ale zostają
do analizy. Dryf wykrywamy, porównując stację z **medianą pozostałych stacji miasta**: zepsuty czujnik oddala się od
sąsiadów. Wykrycie zajęło ok. 25 godzin — dryf rośnie powoli, a próg jest ustawiony tak, żeby nie alarmować przy
zwykłych różnicach między stacjami.”

### Najmocniejszy moment: prawdziwy smog bez flag

```sql
SELECT DATE(time_utc) AS day, ROUND(PERCENTILE(value, 0.5), 0) AS median_ug_m3, MAX(value) AS max_ug_m3,
       SUM(CASE WHEN dq_flag <> 'ok' THEN 1 ELSE 0 END) AS flagged
FROM smogcast_dev.silver.pm_stream
WHERE source = 'fault_replay' GROUP BY DATE(time_utc) ORDER BY day;
```
20 stycznia mediana ok. **104 µg/m³** (ponad dwukrotność normy dobowej) — i **żadna z tych godzin nie jest oflagowana**
jako usterka. **Co powiedzieć:** „Wysoka wartość to nie błąd. Gdy rośnie **kilka stacji naraz**, to prawdziwy epizod
smogowy; gdy rośnie jedna, a sąsiedzi nie — to podejrzany czujnik. Czyszczenie, które wyrzucałoby wysokie wartości,
wyrzuciłoby właśnie te dni, które model ma przewidywać.”

### Zmiana rejestru stacji (CDC → SCD2)

```sql
SELECT op, COUNT(*) AS changes FROM smogcast_dev.silver.station_changes
WHERE registry_source = 'fault_replay' GROUP BY op;

SELECT station_code, valid_from, valid_to, is_current FROM smogcast_dev.silver.stations
WHERE registry_source = 'fault_replay' ORDER BY station_code, valid_from;
```
Oczekiwane: **9 INSERT + 1 UPDATE + 1 DELETE**. **Co powiedzieć:** „API GIOŚ nie podaje zmian — porównujemy kolejne
migawki rejestru i sami wyliczamy zmiany (CDC), a historia stacji jest w tabeli SCD2: wiemy, jaka stacja istniała
i z jakimi czujnikami w każdym momencie.”

### Podsumowanie jakości — jedna tabela

```sql
SELECT metric, value, pct_of_unique FROM smogcast_dev.gold.dq_summary
WHERE source = 'fault_replay' ORDER BY metric;
```
To samo pokazuje panel: strona **Data quality** → przełącznik **„Fault demo”**.

## Krok 4 — domknięcie (30 sekund)

- Każda usterka z harmonogramu wylądowała we właściwym miejscu; prawdziwy smog przeszedł bez flag.
- **Te same reguły czyszczą prawdziwy strumień `live`** — na żywo z API: ok. 4–6% pustych wartości (kwarantanna),
  ponowne wysyłki (deduplikacja), pojedyncze skoki.
- **CI sprawdza to automatycznie przy każdej zmianie kodu**: test integracyjny uruchamia to samo demo na danych
  syntetycznych i sprawdza, że usterki kończą w kwarantannie, w odrzuconych i we flagach
  (`packages/cli/tests/test_integration.py`).

## Pytania, które mogą paść

| Pytanie | Odpowiedź |
|---|---|
| „Skąd wiecie, że próg skoku / zamrożenia jest dobry?” | Progi w `conf/pm_dq.yaml` (np. zamrożenie ≥ 6 h tej samej wartości; skok > 3× sąsiednich godzin i > 100 µg/m³). Na archiwum 2021–2025 oflagowały 0,1% godzin — nie wycinają prawdziwych danych. |
| „Co się dzieje z odrzuconymi danymi?” | Zostają w `ops.*` z powodem; nic nie jest kasowane. |
| „Czy powtórne uruchomienie dubluje dane?” | Nie: zapisy idempotentne (`txnAppId` z id zapytania strumieniowego) i MERGE po kluczu; demo zaczyna od resetu. |
| „Czy popsute dane wpływają na model?” | Nie — osobne źródło `fault_replay`, osobne tabele strumienia; cechy dla prognozy biorą tylko `source = 'live'`. |
