{{ config(materialized='table', schema='final') }}

SELECT
    aircraft_id,
    aircraft_name,
    aircraft_iata,
    aircraft_icao
FROM {{ ref('stg_aircrafts') }}