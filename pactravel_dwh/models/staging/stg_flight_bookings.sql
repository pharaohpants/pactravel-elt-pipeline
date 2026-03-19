{{ config(materialized='view', schema='staging') }}

SELECT
    trip_id,
    flight_number,
    seat_number,
    customer_id,
    airline_id,
    aircraft_id,
    airport_src,
    airport_dst,
    departure_time,
    departure_date::date        AS departure_date,
    flight_duration,
    travel_class,
    price::numeric(12,2)        AS price
FROM {{ source('staging', 'flight_bookings') }}
WHERE price IS NOT NULL