CREATE OR REPLACE TABLE dim_commute_mode AS
WITH ranked AS (
    SELECT
        primary_mode,
        COUNT(*) AS day_count,
        AVG(commute_minutes) AS avg_minutes,
        AVG(mean_delay_s) AS avg_delay_s,
        AVG(CAST(late_trip_count AS DOUBLE) / NULLIF(total_trip_count, 0)) AS late_trip_rate
    FROM stg_commute_day
    GROUP BY primary_mode
)
SELECT
    primary_mode,
    day_count,
    avg_minutes,
    avg_delay_s,
    late_trip_rate,
    CASE
        WHEN avg_minutes <= 45 THEN 'short'
        WHEN avg_minutes <= 75 THEN 'moderate'
        ELSE 'long'
    END AS commute_band
FROM ranked
ORDER BY day_count DESC;
