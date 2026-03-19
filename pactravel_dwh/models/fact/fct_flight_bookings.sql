{{
  config(
    materialized='incremental',
    unique_key='booking_sk',
    schema='final'
  )
}}

SELECT
    MD5(fb.trip_id::text || '|' || fb.flight_number || '|' || fb.seat_number) AS booking_sk,
    fb.trip_id,
    fb.flight_number,
    fb.seat_number,
    dc.customer_sk,
    dd.date_key,
    fb.airport_src          AS origin_airport_id,
    fb.airport_dst          AS dest_airport_id,
    fb.airline_id,
    fb.aircraft_id,
    fb.travel_class,
    fb.flight_duration,
    fb.price                AS ticket_price
FROM {{ ref('stg_flight_bookings') }} fb
LEFT JOIN {{ ref('dim_customers') }} dc
       ON fb.customer_id = dc.customer_id
      AND dc.is_current = TRUE
LEFT JOIN {{ ref('dim_date') }} dd
       ON fb.departure_date = dd.full_date

{% if is_incremental() %}
WHERE fb.departure_date > (
  SELECT COALESCE(MAX(dd2.full_date), '1900-01-01'::date)
    FROM {{ this }} t
    JOIN {{ ref('dim_date') }} dd2
      ON t.date_key = dd2.date_key
)
{% endif %}