{{ config(materialized='table', schema='final') }}

WITH bounds AS (
    SELECT
        COALESCE(MIN(dt), '2020-01-01'::date) AS min_date,
        COALESCE(MAX(dt), '2030-12-31'::date) AS max_date
    FROM (
        SELECT departure_date AS dt
        FROM {{ ref('stg_flight_bookings') }}
        WHERE departure_date IS NOT NULL

        UNION ALL

        SELECT check_in_date AS dt
        FROM {{ ref('stg_hotel_bookings') }}
        WHERE check_in_date IS NOT NULL

        UNION ALL

        SELECT check_out_date AS dt
        FROM {{ ref('stg_hotel_bookings') }}
        WHERE check_out_date IS NOT NULL
    ) d
),
date_series AS (
    SELECT generate_series(
        (SELECT min_date - 365 FROM bounds),
        (SELECT max_date + 365 FROM bounds),
        '1 day'::interval
    )::date AS full_date
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::int  AS date_key,
    full_date,
    EXTRACT(YEAR    FROM full_date)::int AS year,
    EXTRACT(QUARTER FROM full_date)::int AS quarter,
    EXTRACT(MONTH   FROM full_date)::int AS month,
    TO_CHAR(full_date, 'Month')          AS month_name,
    EXTRACT(DAY     FROM full_date)::int AS day,
    TO_CHAR(full_date, 'Day')            AS day_name,
    CASE WHEN EXTRACT(DOW FROM full_date)
         IN (0,6) THEN TRUE
         ELSE FALSE END                  AS is_weekend
FROM date_series