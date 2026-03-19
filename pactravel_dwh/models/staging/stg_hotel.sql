{{ config(materialized='view', schema='staging') }}

SELECT
    hotel_id,
    hotel_name,
    hotel_address,
    city,
    country,
    hotel_score::numeric(3,1) AS hotel_score
FROM {{ source('staging', 'hotel') }}