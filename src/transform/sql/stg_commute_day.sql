CREATE OR REPLACE TABLE stg_commute_day AS
WITH base AS (
    SELECT
        DATE(trip_start_ts AT TIME ZONE 'Australia/Sydney') AS date,
        COALESCE(primary_mode, 'unknown') AS primary_mode,
        COALESCE(trip_minutes, 0) AS trip_minutes,
        COALESCE(fare_amount, 0) AS fare_amount,
        COALESCE(delay_seconds, 0) AS delay_seconds
    FROM raw_commute_trips
),
mode_rank AS (
    SELECT
        date,
        primary_mode,
        COUNT(*) AS trip_count,
        AVG(delay_seconds) AS avg_delay
    FROM base
    GROUP BY date, primary_mode
),
primary_mode AS (
    SELECT
        date,
        ARG_MAX(primary_mode, trip_count - COALESCE(avg_delay, 0) / 100.0) AS primary_mode
    FROM mode_rank
    GROUP BY date
)
SELECT
    b.date,
    pm.primary_mode,
    SUM(b.trip_minutes) AS commute_minutes,
    SUM(b.fare_amount) AS opal_cost,
    SUM(CASE WHEN b.delay_seconds > 60 THEN 1 ELSE 0 END) AS late_trip_count,
    COUNT(*) AS total_trip_count,
    AVG(NULLIF(b.delay_seconds, 0)) AS mean_delay_s
FROM base b
LEFT JOIN primary_mode pm USING (date)
GROUP BY b.date, pm.primary_mode
ORDER BY b.date;
