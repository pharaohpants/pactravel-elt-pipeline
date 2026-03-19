{{ config(materialized='table', schema='final') }}

SELECT
    airline_id,
    airline_name,
    country,
    airline_iata,
    airline_icao,
    alias
FROM {{ ref('stg_airlines') }}