{{ config(materialized='table', schema='final') }}

SELECT
    dd.date_key,
    dd.full_date,
    COUNT(fb.booking_sk)           AS total_bookings,
    SUM(fb.ticket_price)           AS total_revenue,
    AVG(fb.ticket_price)           AS avg_ticket_price,
    COUNT(DISTINCT fb.customer_sk) AS unique_customers
FROM {{ ref('fct_flight_bookings') }} fb
JOIN {{ ref('dim_date') }}      dd ON fb.date_key    = dd.date_key
GROUP BY dd.date_key, dd.full_date
ORDER BY dd.full_date