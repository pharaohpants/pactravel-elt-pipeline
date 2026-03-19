{{
  config(
    materialized='incremental',
    unique_key='trip_id',
    schema='final'
  )
}}

SELECT
    hb.trip_id,
    dc.customer_sk,
    hb.hotel_id,
    dd_in.date_key          AS check_in_date_key,
    dd_out.date_key         AS check_out_date_key,
    hb.price                AS total_price,
    hb.breakfast_included
FROM {{ ref('stg_hotel_bookings') }} hb
LEFT JOIN {{ ref('dim_customers') }} dc
       ON hb.customer_id = dc.customer_id
      AND dc.is_current = TRUE
LEFT JOIN {{ ref('dim_date') }} dd_in
       ON hb.check_in_date = dd_in.full_date
LEFT JOIN {{ ref('dim_date') }} dd_out
       ON hb.check_out_date = dd_out.full_date

{% if is_incremental() %}
WHERE hb.check_in_date > (
  SELECT COALESCE(MAX(dd2.full_date), '1900-01-01'::date)
    FROM {{ this }} t
    JOIN {{ ref('dim_date') }} dd2
      ON t.check_in_date_key = dd2.date_key
)
{% endif %}