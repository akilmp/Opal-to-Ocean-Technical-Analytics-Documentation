CREATE OR REPLACE TABLE stg_env_day AS
WITH air AS (
    SELECT
        DATE(observation_ts AT TIME ZONE 'Australia/Sydney') AS date,
        station_id,
        AVG(pm25) AS pm25_mean,
        AVG(pm10) AS pm10_mean
    FROM raw_air_quality
    GROUP BY date, station_id
),
weather AS (
    SELECT
        DATE(observation_ts AT TIME ZONE 'Australia/Sydney') AS date,
        SUM(rain_mm) AS rain_24h,
        AVG(temperature_c) AS temp_mean_c
    FROM raw_weather
    GROUP BY date
),
beach AS (
    SELECT
        DATE(observation_ts AT TIME ZONE 'Australia/Sydney') AS date,
        ARG_MAX(status, observation_ts) AS beach_status_primary,
        AVG(enterococci) AS enterococci_mean,
        ARG_MAX(site_id, observation_ts) AS representative_site
    FROM raw_beachwatch
    GROUP BY date
)
SELECT
    COALESCE(a.date, w.date, b.date) AS date,
    ARG_MAX(a.station_id, a.pm25_mean) AS primary_station_id,
    AVG(a.pm25_mean) AS pm25_mean,
    AVG(a.pm10_mean) AS pm10_mean,
    MAX(w.rain_24h) AS rain_24h,
    AVG(w.temp_mean_c) AS temp_mean_c,
    MAX(b.beach_status_primary) AS beach_status_primary,
    AVG(b.enterococci_mean) AS enterococci_mean,
    ARG_MAX(b.representative_site, b.enterococci_mean) AS beach_site_id
FROM air a
FULL OUTER JOIN weather w USING (date)
FULL OUTER JOIN beach b USING (date)
GROUP BY date
ORDER BY date;
