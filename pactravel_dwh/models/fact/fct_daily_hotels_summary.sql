{{ config(materialized='table', schema='final') }}

SELECT
    dd.date_key,
    dd.full_date,
    COUNT(hb.trip_id)              AS total_bookings,
    SUM(hb.total_price)            AS total_revenue,
    AVG(hb.total_price)            AS avg_total_price,
    COUNT(DISTINCT hb.customer_sk) AS unique_customers
FROM {{ ref('fct_hotel_bookings') }} hb
JOIN {{ ref('dim_date') }}      dd ON hb.check_in_date_key = dd.date_key
GROUP BY dd.date_key, dd.full_date
ORDER BY dd.full_date