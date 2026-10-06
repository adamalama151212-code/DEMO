# smogcast — data platform architecture

This document describes how data flows through smogcast, which components exist and which tables they produce.
It is part of the knowledge base of the smogcast assistant. Definitions, methodology and model results are
described in the companion document "smogcast — tomorrow's PM10 / PM2.5 exceedance forecast".

## 1. Data flow

Three paths meet in the gold layer and in the assistant.

### Path A — history and model (batch)

1. The GIOŚ yearly archive (xlsx, 2021–2025) is downloaded to the landing zone and converted to a long format
   → `bronze.pm_hourly`. GIOŚ station and position metadata → `bronze.gios_stations`, `bronze.gios_positions`.
2. Hourly measurements of the stations of the 10 cities are cleaned (hard rules, `frozen` and `spike` flags)
   → `silver.pm_hourly`; quarantined rows → `ops.pm_quarantine`.
3. Daily means per station and per city → `silver.pm_daily_station`, `silver.pm_daily_city`.
4. Open-Meteo forecast archives (`short_range` from 2021, `day_ahead` from 2024-01-19) → `bronze.weather_forecast`
   → daily aggregates per CET day → `silver.weather_daily`.
5. Leakage-free features (one row per city, pollutant and issue day) → `silver.features`.
6. The model step trains logistic regression and gradient-boosted trees, selects the operational model on the
   validation year and saves the models; the backtest step writes `gold.backtest_predictions`,
   `gold.backtest_metrics`, `gold.reliability`, `gold.acceptance` and `gold.model_selection`.

### Path B — live data (streaming)

1. Every hour the GIOŚ API is polled for all PM sensors of the 10 cities; each poll is a JSON batch file in the
   landing zone. A fault-injection replay (a copy of real archive measurements with deliberately injected faults)
   writes batch files of the same format to its own landing folder.
2. Spark Structured Streaming reads new batch files (trigger "available now") and, per micro-batch:
   appends the raw rows to `bronze.pm_stream`; classifies each reading as duplicate, correction, late or new;
   writes late readings to `ops.pm_late_rejected` and hard-rule violations to `ops.pm_quarantine`; merges accepted
   readings into `silver.pm_stream` and recomputes the `frozen`, `spike` and `drift` flags on a window of the city's
   recent history.
3. The current weather forecast (Open-Meteo, source `live`) goes through the same bronze and silver weather steps.
4. Features for today's issue day from the live stream and the live weather forecast → `silver.features_live`.
5. The saved operational model computes tomorrow's probability → `gold.forecast_tomorrow`.
6. A data-quality summary of the stream per source → `gold.dq_summary`.

### Path C — station registry (change data capture)

Snapshots of the GIOŚ station registry → `bronze.station_snapshots` → differences between consecutive snapshots
(INSERT / UPDATE / DELETE) → `silver.station_changes` → slowly changing dimension type 2 → `silver.stations`.
The registry also tells the live stream which city a newly opened station belongs to.

### Consumers

- The assistant (question answering with guarded SQL over gold tables and retrieval over documents).
- The dashboard on gold tables: tomorrow in the cities, exceedance days in the season, model quality vs simple
  reference forecasts, calibration, data quality.

## 2. Why it is built this way

- **The model is evaluated where it is used**: the backtest covers 2021–2025 in the same 10 cities.
- **Features only use information available at forecast time** (day D, 12:00 CET), and tomorrow's weather comes from
  a **forecast archive**, never from the weather that actually happened — otherwise backtest results would be
  inflated. Training uses the `short_range` archive; validation and test use **real forecasts issued one day earlier**
  (`day_ahead`, from 2024-01-19).
- **The acceptance criteria were fixed before training** and not changed after seeing the results.
- **Data corruption for demonstrations stays separate**: the fault-injection replay uses a copy of real measurements
  in its own landing folder and its own source label; the model's history never reads it. The GIOŚ archive is verified
  and clean (no quarantined rows, 0.1% flags), so without injected faults a cleaning demonstration would have nothing
  to show.

## 3. Packages (one wheel per pipeline step)

| Package | Python namespace | Steps |
|---|---|---|
| `smogcast-core` | `smogcast.core` | library only: configuration, Spark session, storage (local paths or Unity Catalog), data-quality metrics, step runner, CET/UTC time helpers, shared table contracts |
| `smogcast-ingest` | `smogcast.ingest` | `gios-archive`, `weather-forecast-history`, `gios-registry`, `gios-live`, `weather-forecast-live`, `fault-replay` |
| `smogcast-bronze` | `smogcast.bronze` | `station-meta`, `pm-hourly`, `weather-forecast`, `station-snapshots` |
| `smogcast-silver-clean` | `smogcast.silver_clean` | `pm-hourly`, `pm-stream`, `stations-scd2`, `fault-reset`, `live-reset` |
| `smogcast-silver-transform` | `smogcast.silver_transform` | `pm-daily`, `weather-daily`, `features`, `features-live` |
| `smogcast-model` | `smogcast.model` | `train`, `backtest`, `forecast` |
| `smogcast-gold` | `smogcast.gold` | `dq-summary` |
| `smogcast-app` | `smogcast.app` | the assistant and its SQL guardrails |
| `smogcast-cli` | `smogcast.cli` | local command line only: runs steps and ordered step lists |

Rules:
- Step packages never import each other — only the core package. Table contracts shared by a producer and a consumer
  live in the core package. This is why tomorrow's forecast is a step of the model package (it needs the saved model
  and the model's feature preparation), not of the gold package.
- All packages share one version.
- Every step package has one console entry point taking `--step <name> --env <local|dev|prod>`; one Lakeflow Job task
  runs one step of one wheel.
- Trade-off: more configuration files, lock-step versioning and a slower CI build, in exchange for independent
  deployment and clear responsibility boundaries between steps.

| Where | Who runs the steps | How |
|---|---|---|
| locally (Docker) | the CLI | one step, or an ordered list: `run-batch` (history → model), `run-live` (live path), `run-fault-demo` (cleaning demonstration from scratch) |
| Azure Databricks | two Lakeflow Jobs: batch (history → model) and live (hourly) | each task is a Python wheel task with the step's wheel plus the core wheel, parameters `--step … --env dev/prod` |

The ordered step lists of the CLI and the task graphs of the Jobs are kept identical; a test checks it.

### Job task graph — batch path

`ingest gios-archive` → `bronze station-meta` and `bronze pm-hourly` → `silver-clean pm-hourly` →
`silver-transform pm-daily`; in parallel `ingest weather-forecast-history` → `bronze weather-forecast` →
`silver-transform weather-daily`; both → `silver-transform features` → `model train` → `model backtest`.

### Job task graph — live path (every hour)

`ingest gios-registry` → `bronze station-snapshots` → `silver-clean stations-scd2`;
`ingest gios-registry` → `ingest gios-live`; `gios-live` and `stations-scd2` → `silver-clean pm-stream`;
`ingest weather-forecast-live` → `bronze weather-forecast` → `silver-transform weather-daily`;
`pm-stream` and `weather-daily` → `silver-transform features-live` → `model forecast`;
`pm-stream` → `gold dq-summary`.

The cleaning demonstration runs: `fault-reset` → `fault-replay` → `station-snapshots` → `stations-scd2` →
`pm-stream` → `dq-summary` — every run replays the same faults from scratch and never touches the live source.

## 4. Tables

Locally: `data/delta/<layer>/<table>/`; on Databricks: `smogcast_<env>.<layer>.<table>`.

| Table | Key / partition | Write | Description |
|---|---|---|---|
| `bronze.gios_stations` | station_code | overwrite | stations from GIOŚ metadata, **including closed ones**: town, coordinates, operating dates, old codes |
| `bronze.gios_positions` | position_code | overwrite | measuring positions (e.g. `MpKrakAlKras-PM10-1g`): pollutant, averaging time, measurement type |
| `bronze.pm_hourly` | station_code, pollutant, time_utc / partition `source_year` | replace per year | all stations, PM10 and PM2.5, 1-hour values as published; `time_utc` = start of the hour; 2021–2025: 11.4 million rows |
| `bronze.weather_forecast` | city_id, source, time_utc | overwrite | hourly forecast; `source` = `short_range` / `day_ahead` / `live` |
| `bronze.station_snapshots` | snapshot_ts, station_id | overwrite | snapshots of the GIOŚ API station registry |
| `bronze.pm_stream` | — | idempotent append | raw stream readings; `source` = `live` / `fault_replay` |
| `silver.station_city` | station_code | overwrite | station → city (by town from the metadata), also old codes → today's code |
| `silver.pm_hourly` | station_code, pollutant, time_utc / partition `source_year` | overwrite | stations of the 10 cities only; `dq_flag` (`ok` / `frozen` / `spike`), `is_valid`, `day_cet`, `hour_cet`; 2.1 million rows |
| `silver.pm_daily_station` | station_code, pollutant, day_cet | overwrite | daily mean (valid with at least 18 hours), morning mean (00–10 CET) |
| `silver.pm_daily_city` | city_id, pollutant, day_cet | overwrite | maximum and mean over stations, `exceeded` (NULL = no data, not "no exceedance") |
| `silver.weather_daily` | city_id, source, day_cet | overwrite | forecast aggregates per CET day: temperature, wind, calm hours, precipitation, … |
| `silver.features` | city_id, pollutant, issue_day | overwrite | features from D-1, D-2, the morning of D, the weather forecast for D+1 and the calendar of D+1; label `exceeded_next_day`; `split`, `is_usable` |
| `silver.station_changes` | station_id, snapshot_ts | overwrite | change feed: differences between consecutive registry snapshots |
| `silver.stations` | station_id, valid_from | overwrite | registry history (SCD type 2: `is_current`, `valid_to`) |
| `silver.pm_stream` | source, station_code, pollutant, time_utc | MERGE | cleaned stream: `dq_flag` (`ok` / `frozen` / `spike` / `drift`), `status`, `lag_hours`; kept separate from the model's history |
| `silver.features_live` | city_id, pollutant, issue_day | MERGE | features of today's issue day from the live stream and the live weather forecast; `is_usable`, `unusable_reason` |
| `gold.model_selection` | pollutant, model, params | overwrite | hyper-parameter grid with validation Brier score; `operational` = the model chosen on the validation year |
| `gold.backtest_predictions` | model, split, city_id, pollutant, issue_day | overwrite | probability vs outcome (validation 2024, test 2025), also persistence and climatology |
| `gold.backtest_metrics` | model, split, pollutant, city_id (+ `ALL`) | overwrite | Brier, BSS, POD, FAR, CSI, AUC |
| `gold.reliability` | model, split, pollutant, bin | overwrite | calibration: forecast probability vs observed frequency |
| `gold.acceptance` | pollutant, model, criterion | overwrite | verdict of the acceptance criteria K1–K4 on the test year |
| `gold.forecast_tomorrow` | city_id, pollutant, target_day | MERGE | **the answer to the main question**: `probability`, `warned` (P ≥ 0.5), operational model, `status` (`ok` / `no_forecast` with the reason), explanatory features (yesterday's maximum, this morning's maximum, forecast temperature, wind, calm hours, precipitation), `city_name`, `jurisdiction_code` |
| `gold.dq_summary` | source, metric | overwrite | stream data quality per source: unique readings, re-sent duplicates, late readings, quarantine by reason, flags; share of unique readings |
| `ops.pm_quarantine` | — | replace per source (archive) / append (stream) | readings breaking hard rules or from an unknown station, with the reason; `source` = `archive` / `live` / `fault_replay` |
| `ops.pm_late_rejected` | — | idempotent append | late readings (more than 3 hours after the hour and due at the previous poll already) |
| `ops.dq_metrics` | — | append | log of data-quality metrics from every step |

## 5. Time conventions

| Source | Convention in the source | In tables |
|---|---|---|
| GIOŚ archive | **end** of the hour, **CET** (UTC+1 all year), millisecond noise from the spreadsheet | `time_utc` = **start** of the hour in UTC |
| GIOŚ API (live data, archival data) | **end** of the hour, local time Europe/Warsaw (with daylight saving time) | same |
| Open-Meteo | UTC | same |
| "day" of a daily mean | — | `day_cet` = calendar day in CET |

Example: the archive value "2025-01-01 01:00" is the hour 00:00–01:00 CET, i.e. `time_utc = 2024-12-31 23:00`.

## 6. Idempotency and re-runs

- Whole-table rebuilds (`overwrite`) for small tables recomputed from their sources.
- Replace-where per slice (e.g. one archive year) for large tables — as idempotent as a MERGE, much cheaper.
- MERGE on the natural key for tables updated incrementally (stream, live features, forecasts).
- Inside the stream, appends carry a transaction identifier (application id + micro-batch id), so a replayed
  micro-batch is not written twice; the application id includes the streaming query id, so a reset of the stream
  (new checkpoint, micro-batch ids starting from 0 again) is not mistaken for a replay.
- Data-quality metrics are an append-only event log (every run adds its own entries).

## 7. Environments

Local, DEV and PROD differ only in configuration files: locally Delta tables are directories on disk and raw files
live in a local landing folder; on Databricks tables live in Unity Catalog (`smogcast_dev`, `smogcast_prod`) and raw
files, streaming checkpoints and saved models live in a Unity Catalog volume. The configuration ships inside the core
wheel, so the cluster runs exactly the configuration tested locally. PROD is deployed only by CI/CD through a
Databricks Asset Bundle.
