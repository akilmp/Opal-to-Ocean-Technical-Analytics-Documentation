# Opal to Ocean — Technical & Analytics Documentation

## 1) Executive summary

**Goal.** Build a robust daily-level analytical dataset (“fact_day”) that integrates Sydney public transport reliability, beach water quality, local air quality, weather/rain proxies, and individual activity/transaction logs to quantify trade-offs among **time, cost, and daily wellbeing**. Deliver: (a) clean data marts, (b) an analysis notebook with defensible inference, and (c) a lightweight dashboard for exploration and “what-if” scenarios.

**Why Sydney.** Uses NSW/TfNSW, City of Sydney, and DCCEEW/Beachwatch sources that provide reliable, frequently updated feeds spanning **GTFS/GTFS-Realtime**, **Beachwatch beach quality**, **Air Quality NSW**, and **city pedestrian activity**. ([opendata.transport.nsw.gov.au][1])

**Primary outputs.**

* `fact_day` table (row = day) for analysis & modelling
* Reusable transformations (SQL/DuckDB + Python)
* Streamlit dashboard: “My Sydney Day” + “What-ifs”
* Methods & validation report (this doc)

---

## 2) Success criteria

* **Data completeness:** ≥ 95% of days in study window have all core features (commute, reliability, Beachwatch flag, PM2.5 daily mean).
* **Data quality:** daily automated checks pass (schema, ranges, missingness, duplicates).
* **Reproducibility:** a new machine can rebuild marts and figures with one command.
* **Analytical clarity:** at least three results with uncertainty bands (CIs), and a clear separation of correlation vs. plausible causal insight (with controls).
* **Observability:** logs for each ingestion step with row counts and latency.

---

## 3) Stakeholders & use cases

* **Hiring managers / analytics leads:** evidence of end-to-end data work (ingest→model→viz) and judgement under real-world constraints.
* **Data engineers:** review of schema, naming, partitioning, and pipeline design.
* **Analysts:** reproducible notebook and metrics definitions.

---

## 4) Data inventory

### 4.1 External/open data

| Domain                        | Source                                                | Key fields                                                   | Refresh cadence             | Notes                                                                                         |
| ----------------------------- | ----------------------------------------------------- | ------------------------------------------------------------ | --------------------------- | --------------------------------------------------------------------------------------------- |
| Public transport (static)     | TfNSW “Timetables – Complete GTFS”                    | stops.txt, trips.txt, stop_times.txt, routes.txt, shapes.txt | Periodic                    | Static schedules/routes for joins & geometry. ([opendata.transport.nsw.gov.au][1])            |
| Public transport (real-time)  | TfNSW GTFS-Realtime (trip updates, vehicle positions) | delay, schedule_relationship, trip_id, timestamp             | Real-time                   | Compute delay/reliability measures by day and corridor. ([developer.transport.nsw.gov.au][2]) |
| Beach water quality           | NSW **Beachwatch** (SEED/Data.NSW)                    | site, status (e.g., Good/Poor), enterococci, advisories      | Daily                       | Used for swim suitability and post-rain heuristics. ([data.nsw.gov.au][3])                    |
| Air quality                   | **Air Quality NSW** API & download facility           | pm25, pm10, ozone, station_id, datetime                      | Hourly → aggregate to daily | Official monitoring network; documented API. ([airquality.nsw.gov.au][4])                     |
| Pedestrian activity (context) | City of Sydney Automatic Hourly Pedestrian Count      | site_id, count, datehour                                     | Hourly                      | CBD activity proxy; optional contextual feature. ([data.cityofsydney.nsw.gov.au][5])          |
| Area context (benchmarking)   | ABS Data API (Census)                                 | SA2, commute mode share, rent/income summary                 | Static                      | Use ABS Data API (Beta) for SA2 context. ([Australian Bureau of Statistics][6])               |

> Recent context: NSW’s **State of the Beaches** reporting highlights variable quality across Sydney sites and the recommendation to avoid swimming after rainfall—useful domain framing for Beachwatch-based features. ([The Guardian][7])

### 4.2 First-party data (captured manually or exported)

* Daily journal CSV (commute mode/min, spend, sleep, steps, caffeine, mood 1–5).
* Opal trip history CSV export (date, mode, origin/destination, fare).
* (Optional) Device steps/sleep export; calendar meeting counts per day.

---

## 5) System architecture

**Ingestion.** Python scripts call external APIs (requests/httpx) and save raw pulls as **append-only, date-partitioned** Parquet under `data/raw/<source>/ingest_date=<YYYY-MM-DD>/…`. Large GTFS static zip is cached by version/hash.

**Storage/compute.** **DuckDB** (local, columnar) for transformations; **GeoParquet** for geospatial joins (stops/routes/shapes). Pandas/Polars used for lightweight prep; heavy joins in SQL.

**Transform.** SQL models run in order: `raw → stg_* → dim_* / fact_*`. Orchestrate via `make` or `tox` session to keep it minimal but reproducible.

**App.** Streamlit dashboard reads only from `marts/` (never raw).

**Observability.** Python `structlog` + simple JSON logs; a `quality_report.json` per run.

---

## 6) Directory layout

```
opal-to-ocean/
├─ data/
│  ├─ raw/                # append-only; .gitignore large files
│  ├─ staging/
│  └─ marts/
├─ src/
│  ├─ ingest/             # api clients & pull jobs
│  ├─ transform/          # SQL models & runners
│  ├─ features/           # feature engineering helpers
│  └─ quality/            # validation checks
├─ notebooks/
│  ├─ 01_build_fact_day.ipynb
│  └─ 02_eda_results.ipynb
├─ app/                   # Streamlit
├─ tests/                 # unit + data tests
└─ README.md
```

---

## 7) Data models & schemas

### 7.1 Raw layers (selected)

* `raw_gtfs_static (many files)`

  * `stops(stop_id, stop_name, lat, lon, zone_id)`
  * `trips(route_id, service_id, trip_id)`
  * `stop_times(trip_id, stop_sequence, arrival_time, departure_time, stop_id)`
  * `routes(route_id, route_short_name, route_type)`
  * `shapes(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence)` ([opendata.transport.nsw.gov.au][1])

* `raw_gtfs_rt_trip_updates(ingest_ts, trip_id, delay_s, schedule_relationship)` (protobuf decoded). ([developer.transport.nsw.gov.au][2])

* `raw_beachwatch_daily(ingest_date, site_id, site_name, status, enterococci, rain_24h)` (status categorical). ([data.nsw.gov.au][3])

* `raw_air_quality_hourly(station_id, datetime_utc, pm25, pm10, o3, temp, wind)` (as provided). ([airquality.nsw.gov.au][4])

* `raw_ped_count_hourly(site_id, datehour, count)` (optional). ([data.cityofsydney.nsw.gov.au][5])

* `raw_abs_sa2(sa2_code, metric_name, metric_value, year)` (small extracted slices via ABS Data API). ([Australian Bureau of Statistics][6])

* `raw_personal_journal(date, commute_mode, commute_minutes, spend_transport, steps, sleep_hrs, caffeine, mood_1_5, …)`

* `raw_opal_trips(date, mode, origin_stop, dest_stop, fare_amount)`

### 7.2 Staging

* `stg_commute_day(date, primary_mode, commute_minutes, opal_cost, late_trip_count, total_trip_count, mean_delay_s)`

  * Join Opal trips with GTFS-RT (by line/trip_id & window) to compute **delay** aggregates.

* `stg_env_day(date, nearest_station_id, pm25_mean, pm10_mean, o3_mean, rain_24h, beach_status_primary, beach_status_secondary)`

  * Hourly AQ → daily mean; Beachwatch daily status by chosen sites. ([airquality.nsw.gov.au][4])

### 7.3 Marts

* `dim_site(site_id, site_name, lat, lon, type)` (beach, AQ station, ped counter)
* `dim_sa2(sa2_code, name, region)`
* **`fact_day(date, weekday, month, primary_mode, commute_minutes, mean_delay_s, reliability, opal_cost, pm25_mean, rain_24h, beach_ok, steps, sleep_hrs, caffeine, spend_total, mood_1_5, meetings_count, …)`**

**Derived fields**

* `reliability := 1 - (late_trip_count / nullif(total_trip_count,0))`
* `beach_ok := 1 if beach_status in ('Good','Very good') else 0` (Beachwatch categories). ([data.nsw.gov.au][3])
* `after_rain := 1 if rain_24h >= 10 OR beach_status in ('Poor','Very poor') else 0` (aligns with public guidance to avoid swimming after rain; see news coverage of State of the Beaches). ([The Guardian][7])

---

## 8) Data dictionary (key fields)

**fact_day**

* `date` (DATE) – analysis grain
* `primary_mode` (TEXT) – {train, bus, light_rail, ferry, walk, bike}
* `commute_minutes` (INT) – total home↔work minutes that day
* `mean_delay_s` (INT) – daily mean of GTFS-RT delay on used line(s)
* `reliability` (FLOAT [0,1]) – see definition above
* `opal_cost` (DECIMAL) – total Opal fare that day
* `pm25_mean` (FLOAT µg/m³) – daily mean PM2.5 (nearest station) ([airquality.nsw.gov.au][4])
* `rain_24h` (FLOAT mm) – 24-hour rainfall proxy (from Beachwatch feed if available) ([data.nsw.gov.au][3])
* `beach_ok` (TINYINT) – Beachwatch suitability flag
* `steps`, `sleep_hrs`, `caffeine`, `spend_total`, `mood_1_5`, `meetings_count` – self-logged/exports
* `sa2_code` (TEXT) – for ABS joins; used in benchmarking ([Australian Bureau of Statistics][6])

---

## 9) Ingestion specs

**GTFS static.**

* **Fetch:** download GTFS zip; record content hash; unpack to `raw_gtfs_static/version=…`.
* **Schedule:** ad-hoc (static) with manual refresh monthly.
* **Notes:** Keep shape geometry as GeoParquet for route mapping. ([opendata.transport.nsw.gov.au][1])

**GTFS-Realtime.**

* **Fetch:** poll trip updates every 60s during commuter windows (e.g., 06:00–10:00, 16:00–20:00 local) and every 5 min otherwise; persist snapshots.
* **Parse:** decode protobuf → rows `(timestamp, trip_id, delay_s, route_id)`.
* **Aggregation:** daily per route/line and user’s trips; compute `mean_delay_s`, `late_trip_count`. ([developer.transport.nsw.gov.au][2])

**Beachwatch.**

* **Fetch:** daily status JSON/CSV for selected Sydney sites (e.g., Bondi, Coogee, Manly).
* **Fields:** `status`, `enterococci`, `rain_24h`.
* **Use:** transform status → ordered category; map to `beach_ok`. ([data.nsw.gov.au][3])

**Air Quality NSW.**

* **Fetch:** hourly via API for nearest station(s); backfill gaps with download facility.
* **Transform:** timezone-aware aggregation to daily means. ([airquality.nsw.gov.au][4])

**City of Sydney ped counts (optional).**

* **Fetch:** hourly counts; clip to CBD days; aggregate to daily totals or commuting windows. ([data.cityofsydney.nsw.gov.au][5])

**ABS Data API (optional benchmarking).**

* **Fetch:** once, targeted variables for SA2 of residence/work. ([Australian Bureau of Statistics][6])

---

## 10) Transform & feature engineering

**Normalization.** Z-score continuous metrics by monthly window to reduce seasonality.
**Lag features.** Include lagged PM2.5 (t-1, t-2) and cumulative rain (72h).
**Calendar dummies.** Weekday dummies, public holiday flag (NSW), month, quarter.
**Mode switching.** Construct categorical `mode_switch` comparing usual vs. observed mode; capture elasticity to delays.
**Reliability bands.** Bucket `mean_delay_s` into `{on_time, minor (1–5m), moderate (5–15m), severe (15m+)}`.
**Exposure windows.** For AQ, consider commute-window averages vs. 24-hour mean (sensitivity check).

---

## 11) Analytics plan

**Descriptives.**

* Distributions: commute time by mode; AQ by station; Beachwatch status over time.
* Cross-tabs: reliability bands × mode switching; rain × Beachwatch × attendance.

**Inference (with controls).**

* **OLS/robust regression** of `mood_1_5` on `commute_minutes`, `reliability`, `pm25_mean`, `after_rain`, with weekday & meeting controls.
* **Difference-in-means** for beach attendance on `after_rain` vs. dry days (with CI).
* **Logit** for probability of `mode_switch` as a function of expected delay.
* **Sensitivity**: exclude outlier days; alternate AQ station; alternate rainfall proxy.

**Reporting.**

* Coefficient tables with standard errors; bootstrapped CIs where appropriate.
* Avoid causal claims beyond design; clearly label “association”.

---

## 12) Dashboard functional spec (Streamlit)

**Pages.**

1. **My Sydney Day**

   * Date picker → cards for commute actuals, reliability summary, AQ, Beachwatch status, costs.
   * Map: route shape + beach sites + nearest AQ station.

2. **What-if Simulator**

   * Controls: mode selection, delay scenario, AQ threshold, rain toggle.
   * Outputs: predicted commute time/cost; swim suitability; a brief text summary.

**Non-functional.**

* Uses only `marts/`; loads under 2s for a 90-day window.
* No PII rendered; redact exact home coordinates.

---

## 13) Data quality & validation

**Schema checks.** Great Expectations (or lightweight custom): data types, unique keys (`date`), categorical domains (Beachwatch).
**Range checks.** `pm25_mean` (0–500), `commute_minutes` (0–240), fares ≥ 0.
**Completeness.** Required columns non-null ≥ 95%.
**Freshness.** Echo ingestion timestamps; alert if Beachwatch/AQ > 48h old.
**Reconciliation.** Opal fare totals vs. daily spend_transport tolerance ±$2.

---

## 14) Privacy & ethics

* Raw exports (Opal, personal logs) are **.gitignored** and never pushed.
* Only daily aggregates and hashed location identifiers enter marts.
* Config uses environment variables; secrets not committed.
* Dashboard ships with **synthetic sample** data; instructions provided to rebuild with private data locally.

---

## 15) Setup & reproducibility

**Prereqs.** Python ≥ 3.11; DuckDB; `make`.
**Env.**

```bash
cp .env.example .env  # fill TfNSW key, AQ NSW token if required
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Build.**

```bash
make ingest   # pulls Beachwatch, AQ, GTFS/GTFS-RT snapshots
make marts    # runs SQL to build stg_* and fact_day
make analyze  # runs notebooks to output figures
make app      # launches Streamlit locally
```

**Config.** `config.yml` to select Sydney sites, AQ station preference, commuter lines.

---

## 16) Testing strategy

* **Unit tests (pytest)** for parsers (GTFS-RT decode, Beachwatch status mapping).
* **Data tests** for keys, nulls, ranges on `stg_*` and `fact_day`.
* **Golden file tests** for deterministic transformations on a fixed tiny sample.
* **Snapshot tests** for dashboard outputs (optional).

---

## 17) Logging & observability

* Structured JSON logs per step: `rows_in`, `rows_out`, `duration_ms`, `source_url`, `hash`.
* Write a `run_manifest.json` with data versions (GTFS zip hash, last AQ hourly timestamp) to aid reproducibility.

---

## 18) Performance notes

* Prefer vectorized Pandas/Polars for preprocessing; heavy joins in DuckDB SQL.
* Store normalized keys, precompute `reliability` and `after_rain` to avoid recomputation in the app.
* Partition parquet by `date` month for quick filters.

---

## 19) Risks & mitigations

* **API changes/quotas** (TfNSW, AQ NSW): pin client versions; backfill via download facility when API unavailable. ([airquality.nsw.gov.au][8])
* **Status semantics** (Beachwatch categories): keep mapping table under version control. ([data.nsw.gov.au][3])
* **Clock/timezone drift:** standardize to Australia/Sydney, convert UTC feeds carefully.
* **Data sparsity** for certain lines/times: widen polling windows; fill with static schedule when RT missing (flagged).

---

## 20) Roadmap (stretch)

* Add **ABS SA2 benchmarking** view in dashboard for commute mode/time context. ([Australian Bureau of Statistics][6])
* Incorporate **City of Sydney** pedestrian counts as a congestion proxy in CBD. ([data.cityofsydney.nsw.gov.au][5])
* Persist geospatial layers as **GeoParquet** and add isochrone estimation for first/last mile.

---

## 21) References (key documentation)

* TfNSW developer docs for GTFS / GTFS-Realtime and feed consumers. ([developer.transport.nsw.gov.au][2])
* TfNSW “Timetables – Complete GTFS” dataset page. ([opendata.transport.nsw.gov.au][1])
* NSW Beachwatch dataset hub (program & data access). ([data.nsw.gov.au][3])
* Air Quality NSW API & data download facility. ([airquality.nsw.gov.au][4])
* City of Sydney data hub — pedestrian counters. ([data.cityofsydney.nsw.gov.au][5])
* ABS Data API (Beta) & usage guide. ([Australian Bureau of Statistics][6])
* Recent media coverage summarising **State of the Beaches** outcomes (context for rain/quality guidance). ([The Guardian][7])

---

## 22) Appendix — example SQL (stg → fact)

```sql
-- stg_commute_day
CREATE OR REPLACE TABLE stg_commute_day AS
SELECT
  DATE(trip_start_ts, 'Australia/Sydney') AS date,
  ANY_VALUE(primary_mode) AS primary_mode,
  SUM(trip_minutes) AS commute_minutes,
  SUM(fare_amount)  AS opal_cost,
  SUM(CASE WHEN delay_s > 60 THEN 1 ELSE 0 END) AS late_trip_count,
  COUNT(*) AS total_trip_count,
  AVG(NULLIF(delay_s,0)) AS mean_delay_s
FROM joined_opal_gtfsrt
GROUP BY 1;

-- fact_day
CREATE OR REPLACE TABLE fact_day AS
SELECT
  c.date, dow(c.date) AS weekday, strftime(c.date, '%m') AS month,
  c.primary_mode, c.commute_minutes, c.mean_delay_s,
  1.0 - (c.late_trip_count / NULLIF(c.total_trip_count,0)) AS reliability,
  c.opal_cost,
  e.pm25_mean, e.rain_24h,
  CASE WHEN e.beach_status_primary IN ('Good','Very good') THEN 1 ELSE 0 END AS beach_ok,
  p.steps, p.sleep_hrs, p.caffeine, p.spend_total, p.mood_1_5,
  p.meetings_count, p.sa2_code
FROM stg_commute_day c
LEFT JOIN stg_env_day e USING(date)
LEFT JOIN stg_personal_day p USING(date);
```

---

[1]: https://opendata.transport.nsw.gov.au/dataset/timetables-complete-gtfs?utm_source=chatgpt.com "Timetables Complete GTFS - Transport for NSW"
[2]: https://developer.transport.nsw.gov.au/developers/documentation?utm_source=chatgpt.com "Documentation | Transport Open Data"
[3]: https://data.nsw.gov.au/data/dataset/beachwatch?utm_source=chatgpt.com "Beachwatch | Data.NSW - Data.NSW"
[4]: https://www.airquality.nsw.gov.au/air-quality-data-services/air-quality-api?utm_source=chatgpt.com "Air quality API | Air Quality NSW"
[5]: https://data.cityofsydney.nsw.gov.au/datasets/66421e1dfe264bb19c76179ae92281cf_0/explore?showTable=true&utm_source=chatgpt.com "Automatic Hourly Pedestrian Count | City of Sydney Data hub"
[6]: https://www.abs.gov.au/about/data-services/application-programming-interfaces-apis/data-api-user-guide?utm_source=chatgpt.com "Data API user guide - Australian Bureau of Statistics"
[7]: https://www.theguardian.com/australia-news/2025/oct/22/sydney-beach-most-polluted-swimming-rankings?utm_source=chatgpt.com "Coogee beach among Sydney swimming spots ranked most polluted with faecal matter"
[8]: https://www.airquality.nsw.gov.au/air-quality-data-services/data-download-facility?utm_source=chatgpt.com "Data download facility - Air Quality NSW"
