{{ config(materialized='view', schema='staging') }}

SELECT
    airline_id,
    airline_name,
    country,
    airline_iata,
    airline_icao,
    alias
FROM {{ source('staging', 'airlines') }}