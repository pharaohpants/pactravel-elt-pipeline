{{ config(materialized='view', schema='staging') }}

SELECT
    trip_id,
    customer_id,
    hotel_id,
    check_in_date::date   AS check_in_date,
    check_out_date::date  AS check_out_date,
    price::numeric(12,2)  AS price,
    breakfast_included
FROM {{ source('staging', 'hotel_bookings') }}
WHERE price IS NOT NULL