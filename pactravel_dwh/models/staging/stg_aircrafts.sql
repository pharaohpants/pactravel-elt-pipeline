{{ config(materialized='view', schema='staging') }}

SELECT
    aircraft_id,
    aircraft_name,
    aircraft_iata,
    aircraft_icao
FROM {{ source('staging', 'aircrafts') }}