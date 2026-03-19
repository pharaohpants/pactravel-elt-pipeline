{{ config(materialized='table', schema='final') }}

SELECT
    hotel_id,
    hotel_name,
    hotel_address,
    city,
    country,
    hotel_score
FROM {{ ref('stg_hotel') }}