{{ config(materialized='view', schema='staging') }}

SELECT
    airport_id,
    airport_name,
    city,
    latitude,
    longitude
FROM {{ source('staging', 'airports') }}