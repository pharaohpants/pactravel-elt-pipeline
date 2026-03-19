{{
  config(
    materialized='incremental',
    unique_key='customer_sk',
  incremental_strategy='delete+insert',
    schema='final'
  )
}}

WITH src AS (
  SELECT
    customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_birth_date,
    customer_country,
    customer_phone_number,
    MD5(
      CONCAT_WS(
        '||',
        customer_id::text,
        COALESCE(customer_first_name, ''),
        COALESCE(customer_family_name, ''),
        COALESCE(customer_gender, ''),
        COALESCE(customer_birth_date::text, ''),
        COALESCE(customer_country, ''),
        COALESCE(customer_phone_number::text, '')
      )
    ) AS customer_sk
  FROM {{ ref('stg_customers') }}
)

{% if not is_incremental() %}

SELECT
  s.customer_sk,
  s.customer_id,
  s.customer_first_name,
  s.customer_family_name,
  s.customer_gender,
  s.customer_birth_date,
  s.customer_country,
  s.customer_phone_number,
  CURRENT_TIMESTAMP         AS effective_start,
  '9999-12-31'::timestamp   AS effective_end,
  TRUE                      AS is_current
FROM src s

{% else %}

, curr AS (
  SELECT
    customer_sk,
    customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_birth_date,
    customer_country,
    customer_phone_number,
    effective_start,
    effective_end,
    is_current
  FROM {{ this }}
  WHERE is_current = TRUE
)

, changed_or_new AS (
  SELECT
    s.*,
    c.customer_sk AS current_customer_sk
  FROM src s
  LEFT JOIN curr c
       ON s.customer_id = c.customer_id
  WHERE c.customer_id IS NULL
     OR s.customer_sk <> c.customer_sk
)

, rows_to_expire AS (
  SELECT
    c.customer_sk,
    c.customer_id,
    c.customer_first_name,
    c.customer_family_name,
    c.customer_gender,
    c.customer_birth_date,
    c.customer_country,
    c.customer_phone_number,
    c.effective_start,
    CURRENT_TIMESTAMP       AS effective_end,
    FALSE                   AS is_current
  FROM curr c
  JOIN changed_or_new d
    ON c.customer_id = d.customer_id
)

, rows_to_insert AS (
  SELECT
    d.customer_sk,
    d.customer_id,
    d.customer_first_name,
    d.customer_family_name,
    d.customer_gender,
    d.customer_birth_date,
    d.customer_country,
    d.customer_phone_number,
    CURRENT_TIMESTAMP       AS effective_start,
    '9999-12-31'::timestamp AS effective_end,
    TRUE                    AS is_current
  FROM changed_or_new d
)

SELECT * FROM rows_to_expire
UNION ALL
SELECT * FROM rows_to_insert

{% endif %}