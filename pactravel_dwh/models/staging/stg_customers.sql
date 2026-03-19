{{ config(materialized='view', schema='staging') }}

SELECT
    customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_birth_date::date AS customer_birth_date,
    customer_country,
    customer_phone_number
FROM {{ source('staging', 'customers') }}