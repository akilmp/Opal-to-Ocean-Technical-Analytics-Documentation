CREATE OR REPLACE TABLE dim_environment_metric AS
SELECT
    date,
    primary_station_id,
    pm25_mean,
    pm10_mean,
    rain_24h,
    temp_mean_c,
    beach_status_primary,
    enterococci_mean,
    beach_site_id,
    CASE
        WHEN pm25_mean <= 15 THEN 'good'
        WHEN pm25_mean <= 25 THEN 'moderate'
        ELSE 'unhealthy'
    END AS pm25_category,
    CASE
        WHEN rain_24h < 1 THEN 'dry'
        WHEN rain_24h < 10 THEN 'wet'
        ELSE 'storm'
    END AS rainfall_category,
    beach_status_primary IN ('Good', 'Very good') AS is_swimmable
FROM stg_env_day
ORDER BY date;
