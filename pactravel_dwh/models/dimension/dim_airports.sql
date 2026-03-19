{{ config(materialized='table', schema='final') }}

SELECT
    airport_id,
    airport_name,
    city,
    latitude,
    longitude
FROM {{ ref('stg_airports') }}