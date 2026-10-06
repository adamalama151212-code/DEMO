# smogcast — tomorrow's PM10 / PM2.5 exceedance forecast for the 10 largest Polish cities

This document describes what smogcast is, how its forecast is produced, how good the forecast is and where its
limits are. It is part of the knowledge base of the smogcast assistant.

## 1. The question smogcast answers

> **Will the PM10 or PM2.5 limit be exceeded tomorrow in city Y, and how certain is that?**

The answer is a **probability** (0–100%) per city and pollutant, issued every day for the next day, together with
the model that produced it and the evidence of how reliable such probabilities were in the past.

Cities covered (the 10 largest cities in Poland, each in a different voivodeship): Warszawa, Kraków, Wrocław, Łódź,
Poznań, Gdańsk, Szczecin, Bydgoszcz, Lublin, Białystok. The mix includes cities with frequent smog (Kraków, Łódź)
and cleaner ones (Gdańsk, Szczecin).

## 2. Definitions

| Term | Definition in smogcast | Source |
|---|---|---|
| day | calendar day in **CET** (UTC+1 all year, no daylight saving time) — the way the Chief Inspectorate of Environmental Protection (GIOŚ) computes daily means | GIOŚ archive data are in CET |
| daily station mean | mean of 1-hour results; **valid only with at least 18 of 24 hours** (75%) | EU data-completeness criterion (Directive 2008/50/EC) |
| PM10 exceedance | daily mean **above 50 µg/m³** | limit value for the protection of human health (Poland/EU); 35 exceedance days per year are allowed |
| PM2.5 exceedance | daily mean **above 25 µg/m³** (configurable) | current Polish law has no daily PM2.5 limit; 25 µg/m³ is the daily limit value of the revised EU Directive 2024/2881 (applicable from 2030); alternative: 15 µg/m³ (WHO 2021 guideline) |
| exceedance in a city | **at least one** station of the city has a valid daily mean above the limit | this is how GIOŚ reports exceedances (per measuring position) |
| "how certain" | the **probability** from the model, checked for **calibration**: of all days forecast at "70%", an exceedance should occur on about 70% | backtest |
| "tomorrow" | day D+1; the forecast is issued on day D at 12:00 CET (measurements up to 11:00 + weather forecast for D+1) | |
| warning | a forecast probability of **0.5 or more** | configurable threshold |

All thresholds live in configuration (`conf/thresholds.yaml`), each with its source — not in code.

## 3. Data sources

### 3.1 GIOŚ yearly archive (history of hourly measurements)

- Source: https://powietrze.gios.gov.pl/pjp/archives ("Prepared data for download"), one zip per year.
- Years used: **2021–2025** (five heating seasons). The weather-forecast archive starts in 2021, so earlier PM years
  would have no matching forecast.
- Only the automatic, hourly series (`<year>_PM10_1g.xlsx`, `<year>_PM25_1g.xlsx`) are used; daily means are
  computed by smogcast itself.
- Format: wide sheet (rows = hours, columns = measuring positions); the timestamp is the **end of the hour in CET**
  (`2025-01-01 01:00` covers 00:00–01:00 CET) and carries spreadsheet noise of a few milliseconds, so it is rounded to
  the nearest hour.
- Labels on the GIOŚ web page are shifted against the download links; every download therefore checks the file name
  from the `Content-Disposition` header.
- Size: **11.4 million** hourly values for 2021–2025. The archive is verified by GIOŚ and very clean: no negative
  values, 3 values above 1000 µg/m³ and about 8 thousand zeros.
- Station metadata (a separate xlsx) includes **closed stations** and old station codes, which the current API no
  longer lists — needed to assign historic stations to cities.

### 3.2 GIOŚ API v1 (live measurements and the station registry)

- Base URL `https://api.gios.gov.pl/pjp-api/v1/rest` (the old `/pjp-api/rest/...` returns 410 Gone).
- `station/findAll` — the station registry (station code, name, coordinates as text, city, voivodeship).
- `station/sensors/{stationId}` — measuring positions of a station (e.g. PM10, PM2.5).
- `data/getData/{sensorId}` — roughly the **last three days** of hourly values; the timestamp is the **end of the
  hour in local time** (Europe/Warsaw, with daylight saving time); values may be `null` (not yet published).
- `archivalData/getDataBySensor/{sensorId}` — archived values of one sensor (checked: identical values and stamps
  to the yearly archive).
- Manual measuring positions (daily, laboratory-analysed filters) have no live data: the API returns HTTP 400 for
  them ("results are published after 4–8 weeks"). In the 10 cities about a third of the PM positions are manual.

### 3.3 Weather forecasts (Open-Meteo)

The model must see the **weather forecast** for tomorrow, never the weather that actually happened — otherwise
backtest results would be inflated. Three Open-Meteo sources are used:

| Source | API | Available from | Used for |
|---|---|---|---|
| `short_range` | Historical Forecast API — continuously stitched freshest forecasts (a few hours of lead time) | 2021 | model **training** (the only option before 2024) |
| `day_ahead` | Previous Runs API, variables with the suffix `_previous_day1` — the forecast issued one day earlier | 2024-01-19 12:00 UTC | **validation and test** (real day-ahead quality) |
| `live` | Forecast API — the current forecast (today + 2 days) | now | **live forecasts** |

Hourly variables: temperature at 2 m, relative humidity, wind speed and direction at 10 m, precipitation, surface
pressure, cloud cover, shortwave radiation; always in m/s and UTC. `boundary_layer_height` is empty in these APIs and
is not used. On the same days the two archives differ little: mean absolute difference of the daily mean temperature
0.40 °C and of the mean wind speed 0.21 m/s — training weather is only slightly better than real day-ahead forecasts.

## 4. Time conventions

- Tables store `time_utc` = the **start** of the measurement hour in UTC.
- The GIOŚ archive stamps the **end** of the hour in **CET**; the GIOŚ API stamps the **end** of the hour in
  **local time** (with daylight saving time); Open-Meteo works in UTC.
- The "day" of a daily mean is the calendar day in CET (`day_cet`).
- Example: the archive value "2025-01-01 01:00" is the hour 00:00–01:00 CET, i.e. `time_utc = 2024-12-31 23:00`.
- All conversions happen in one module (`smogcast.core.timeutil`) and are covered by tests (New Year's Eve, daylight
  saving changes, spreadsheet noise).

## 5. How the forecast is produced

### 5.1 Cleaning hourly measurements

- **Hard rules** → quarantine with the reason: value below 0 µg/m³ (`below_min`), above 1000 µg/m³ (`above_max`),
  missing value (`missing_value`), unknown station (`unknown_station`).
- **Flags** (the row stays, but is not valid for daily means):
  - `frozen` — the same value in 6 or more consecutive hours (a PM monitor does not read exactly the same value for
    hours); the whole run is flagged, a gap splits the run;
  - `spike` — one hour above 3× the highest of its two neighbours on each side and above 100 µg/m³ (a real smog
    episode rises over several hours, so its neighbours are high too);
  - `drift` (live stream only) — the station's recent deviation from the **median of the other stations in the same
    city** rises above its own earlier level (24-hour window vs the 72 hours before it, logarithmic residual increase
    above 0.5, at least 3 stations in the city). If all stations rise together — a real smog episode — the deviation
    stays flat and nothing is flagged; a traffic station that always reads higher is not flagged either.
- Station → city assignment uses the city name from the station metadata (no hard-coded list) and maps old station
  codes to today's code. For the live stream the archive metadata is combined with the current API registry, because
  new stations (e.g. Lublin ul. Okopowa, Poznań ul. Hetmańska) are not in the metadata file.

### 5.2 Daily means

- A station-day is valid with **at least 18 valid hours**.
- A city's daily value is the **maximum** over its valid stations (and, for information, the mean over stations).
  A city day without any valid station is unknown (NULL), not "no exceedance".
- The "morning" of day D = hours starting 00:00–10:00 CET, which are already published at the 12:00 issue time; a
  morning mean needs at least 8 hours.
- Exceedance frequency 2021–2025 (share of days): PM10 from 3.8% (Szczecin) to 17.1% (Kraków); PM2.5 from 10.5%
  (Szczecin) to 24.8% (Kraków). The highest mean PM10 concentration 2021–2025 was in Kraków (27.4 µg/m³).

### 5.3 Features (no leakage of the future)

One row per city, pollutant and issue day D, predicting day D+1. A feature may only use what is known on day D at
12:00 CET:
- PM of day D-1 (city maximum and mean, whether D-1 exceeded the limit) and the maximum of day D-2,
- the morning of day D (maximum and mean so far),
- the **weather forecast for D+1** aggregated per CET day: mean/min/max temperature and range, humidity, mean/min/max
  wind speed, **calm hours** (wind below 1.5 m/s), mean wind direction (vector mean, so 350° and 10° average to 0°,
  not 180°), precipitation sum and hours, pressure, cloud cover, radiation sum; a forecast day is complete with at
  least 20 hours,
- the calendar of D+1: month, day of week, weekend, heating season (October–April).

The label is whether D+1 exceeded the limit — the only column that looks at D+1 measurements. A test checks the
absence of leakage: changing D+1 data changes only the label. A row with missing weather or without D-1 data is kept
but marked unusable — never back-filled.

Before the models, fixed transformations are applied (no statistics learned from data, hence no leakage):
logarithm of concentrations (smog is multiplicative), sine/cosine of wind direction and month, a missing morning
mean filled with yesterday's mean plus a 0/1 "missing" indicator.

### 5.4 Models

- Supervised machine learning, **binary classification with a probability**, in Spark MLlib:
  **logistic regression** (well calibrated, explainable) and **gradient-boosted trees (GBT)**. One model per
  pollutant; the city is a one-hot feature, so cities share what they have in common (weather → smog) while keeping
  their own base level.
- **Chronological split** (never random — neighbouring days are alike): training 2021–2023, validation 2024,
  test 2025.
- Procedure fixed **before** seeing any result: every hyper-parameter grid point is fitted on the training years and
  scored with the Brier score on the validation year; the operational model of a pollutant is the model type with
  the **lowest validation Brier score**; it is then refit on 2021–2024 (the "final" model) and evaluated **once** on
  the test year. The test year never influences any choice.
- Two simple reference forecasts the model must beat:
  - **persistence** — "tomorrow like today": probability 1 if D-1 exceeded the limit, else 0;
  - **climatology** — the share of exceedance days in the same city and calendar month in the training years only.

### 5.5 Metrics in plain words

- **Brier score** — mean squared error of the probability (0 = perfect, lower is better).
- **BSS (Brier skill score)** — how much better than climatology on the same days (> 0 = the model adds value).
- **POD (probability of detection)** — of the days with an exceedance, on how many the model warned.
- **FAR (false alarm ratio)** — of the warnings, how many were false.
- **CSI** — hits / (hits + misses + false alarms); ignores the many quiet days.
- **AUC** — how well probabilities rank exceedance days above quiet days (0.5 = random, 1 = perfect).
- **Calibration (reliability)** — per probability bin, the forecast probability vs how often the exceedance really
  happened: does "70%" mean 70%?

### 5.6 Acceptance criteria (fixed before training)

The model counts as reliable only if **all** criteria hold on the test year (2025), with real day-ahead weather
forecasts:

| # | Criterion | In plain words |
|---|---|---|
| K1 | Brier skill score vs climatology > 0 | probabilities better than the calendar average |
| K2 | Brier score lower than persistence | the model adds something beyond "tomorrow like today" |
| K3 | calibration: in every probability bin with at least 30 days, \|forecast − observed frequency\| ≤ 15 percentage points | "70%" means about 70% |
| K4 | PM10 only: POD ≥ 60% and FAR ≤ 50% (warning at P ≥ 0.5) | catches most smog days without a false alarm every other time |

The thresholds are reasonable starting values, not an industry standard, and were not changed after seeing results.

## 6. How good the forecast is (test year 2025)

10 cities, forecast for the next day with the real weather forecast issued one day earlier.

| | PM10 (GBT model, depth 3, 100 trees) | PM2.5 (GBT model, depth 5, 50 trees) |
|---|---|---|
| days / days with an exceedance | 3613 / 292 (8.1%) | 3627 / 697 (19.2%) |
| Brier: model / climatology / persistence | **0.0434** / 0.0615 / 0.0933 | **0.0751** / 0.117 / 0.172 |
| BSS vs climatology | 0.30 | 0.36 |
| AUC | 0.95 | 0.94 |
| POD / FAR (P ≥ 0.5) | 44% / 32% | 70% / 27% |
| K1 BSS > 0 | passed | passed |
| K2 better than persistence | passed | passed |
| K3 calibration ≤ 15 p.p. | passed, but **borderline**: 8.5 p.p. (bin 0.4–0.5: forecast 45%, observed 53%) | passed (7.2 p.p.) |
| K4a POD ≥ 60% | **failed**: 44% | not applicable |
| K4b FAR ≤ 50% | passed (32%) | not applicable |
| **Verdict** | **does not meet the criteria** (K4a) | **meets the criteria** |

Observations:
- The results are **reproducible**: the same numbers come out on a laptop (Docker) and on Databricks. Gradient-boosted
  trees were at first sensitive to floating-point noise and to how the data was split into partitions, so model inputs
  are rounded to 6 decimals and each model is trained on the data in one ordered partition.
- **The PM10 calibration result is fragile.** In model versions that differed only by numerical noise, the largest
  calibration gap was between 8.5 and 21 percentage points, so the K3 verdict changed in both directions. The high
  PM10 forecast bins hold only about 30 days each (e.g. 0.7–0.8: 29 days, below the 30-day minimum for judging a bin).
  Honest reading: PM10 calibration is at the edge of the criterion, not clearly met.
- GBT was chosen for PM10 on the validation year (Brier 0.0385 vs 0.0396 for logistic regression). On the test year
  logistic regression did better (Brier 0.040, BSS 0.35, FAR 26%), but switching models after looking at the test year
  would be selection on the test set, so it was not done. Confirming it requires a new, unseen period.
- 2025 was smoggier than 2024 (PM10 exceedances on 8.1% vs 5.3% of days). The model still beat both simple rules in
  9 of 10 cities (exception: PM10 in Bydgoszcz, worse than climatology).
- Persistence has a negative BSS because its 0/1 forecasts are punished hard by the Brier score — yet it detects 42%
  of exceedance days, so "tomorrow like today" is not a silly rule.

### Why PM10 is harder to forecast than PM2.5

- PM2.5 is mostly **combustion** dust (household stoves, traffic) and depends almost entirely on the **weather the
  model knows from the forecast**: cold means more heating, calm wind keeps the smoke above the city.
- PM10 = PM2.5 **plus coarse dust**: raised from dry streets by traffic, from construction sites, soil, sand after
  winter. These causes are **not in the model** (no data on road condition, construction, gritting) — the model
  sees only part of the causes.
- PM10 exceedances are **rarer** (about 8% of days vs 17–19% for PM2.5): fewer training examples (about 900 vs 1800
  days) and more room for false confidence; the 50 µg/m³ limit needs a **strong** episode, so the model must predict
  "how much", not only "whether there will be smog".
- More positions measure PM10, also next to busy streets (Kraków: 9 PM10 vs 2 PM2.5) — a local jump at one station
  is enough for a city exceedance, and such jumps are not visible in the weather forecast.
- Conclusion: the PM10 model **ranks** days well from least to most dangerous (AUC 0.95), but misses more than half
  of the exceedance days and its calibration is at the edge of the criterion. Possible improvements: features describing the road surface
  (precipitation, days without rain), the gritting season, working days near traffic stations; recalibration of PM10
  probabilities fitted on the validation year.

## 7. Live operation

- Every hour the live path polls the GIOŚ API for all PM10/PM2.5 sensors of the 10 cities (about 100 sensors; about
  a third are manual and have no live data), takes a snapshot of the station registry and downloads the current
  weather forecast.
- **Stream cleaning** (Spark Structured Streaming): the API re-sends about three days on every poll, so most records
  are duplicates by design. Each reading (station, pollutant, hour) is classified as:
  - duplicate — the same value is already stored,
  - correction — the key is stored with a different value (GIOŚ re-publishes verified values),
  - late — a new reading that should have been available at the previous poll already and arrives more than 3 hours
    after its hour ended → rejected and logged; on the first poll ever old hours are a backfill, not late,
  - new — goes through the hard rules and the flags.
  A quarantined empty value is judged again when GIOŚ fills it in later. When several polls are processed together,
  a reading is judged by its first arrival and keeps its newest value.
- **Station registry changes** (stations opened or closed, new sensors) are detected by comparing consecutive
  registry snapshots (change data capture) and kept as a slowly changing dimension (type 2) with validity periods.
- **Tomorrow's forecast** is computed daily from the cleaned live stream (the same daily-mean rules as the history)
  and the live weather forecast, with each pollutant's operational model in its "final" version. A city without valid
  PM data for yesterday or without a complete weather forecast for tomorrow gets an explicit "no forecast" row with
  the reason — never a guessed value.
- **Measured data quality** per source: unique readings, re-sent duplicates, late readings, quarantined readings by
  reason, flags, share of valid readings. In live data the typical quarantine reason is an empty value for the most
  recent hours (not yet published).

### Fault-injection demonstration

The GIOŚ archive is too clean to demonstrate data cleaning, so a **copy** of real measurements (Kraków, 17–21 January
2025) is replayed as a stream with deliberately injected faults. It never touches the model's history.

| Injected fault | Expected result |
|---|---|
| connection outage → a delayed batch of backlogged readings | late readings rejected |
| the same batch sent twice | one record (deduplication) |
| negative value / value above 1000 µg/m³ | quarantine with the rule as the reason |
| missing value from a working station | quarantine |
| frozen reading (the same value for many hours) | `frozen` flag |
| slowly growing error of one station vs the other stations | `drift` flag |
| one-hour spike | `spike` flag |
| a **real smog episode** at several stations at once (20 January 2025, city median about 104 µg/m³) | **no flag** — a real signal, not a fault |

The real demonstration produced each result at the expected place; drift was detected about 25 hours after it began,
only on the drifting station.

## 8. Known limitations

- PM10 does not meet the calibration and detection criteria (section 6); its probabilities around 50–90% are too high.
- The live station set is slightly larger than the historic one (the API registry contains newer stations, e.g.
  Warszawa 8 vs 6 PM10 stations). With "city = worst station", more stations systematically raise the maximum, so
  live features may be slightly higher than in training.
- The forecast uses only measurements and the weather forecast — no emission inventories, traffic, road condition or
  long-range transport data.
- Training weather (short-range forecasts) is slightly better than the weather available in real operation
  (day-ahead forecasts); the evaluation uses real day-ahead forecasts, so the reported scores are not inflated by this.
- One hour per year is ambiguous at the autumn clock change; it is resolved to the first occurrence.
- PM2.5 uses the future EU daily limit (25 µg/m³, from 2030), because Polish law has no daily PM2.5 limit today.

## 9. Platform

- Medallion architecture: **bronze** (raw data as received), **silver** (cleaned, daily means, features), **gold**
  (forecasts, backtest metrics, calibration, acceptance verdict, data-quality summary), plus **ops** (quarantine,
  late readings, data-quality metrics log).
- Every pipeline step is a separate Python wheel with its own entry point; shared code (configuration, Spark session,
  storage, time conventions, table contracts) lives in one core package. Locally everything runs in Docker with Delta
  Lake tables; on Azure Databricks the same code uses Unity Catalog tables and volumes, Lakeflow Jobs run one step per
  task, and an Asset Bundle with CI/CD deploys DEV and PROD.
- Writes are idempotent (MERGE on natural keys, replace-where per slice, transactional appends in the stream), so any
  step can be re-run without duplicating data.
- Access control is planned per voivodeship (row-level security on `jurisdiction_code`) with column masking for
  technical columns.
- The AI assistant answers numeric questions only from gold tables through SQL guarded by a parser (one SELECT
  statement, whitelisted gold tables, enforced LIMIT) and answers knowledge questions from this document, the
  architecture description and official air-quality documents — always showing the SQL or the sources it used.
