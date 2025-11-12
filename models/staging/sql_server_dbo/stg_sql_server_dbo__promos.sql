{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_id,
        promo_id AS promo_descr,
        discount AS discount_dollars,
        status, -- NORMALIZAR??
        _fivetran_deleted,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS date_load_utc
    FROM src_promos

    UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(["'no promo'"]) }} AS promo_id,
    'no promo' AS promo_descr,
    0 AS discount_dollars,
    'active' AS status,
    NULL AS _fivetran_deleted,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP(9)) AS date_load_utc
    )

SELECT * FROM renamed_casted


