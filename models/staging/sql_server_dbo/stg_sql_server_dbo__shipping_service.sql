{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT *, 
    COALESCE(NULLIF(shipping_service, ''), 'other') AS shipping_service_clean

    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['shipping_service_clean']) }} AS shipping_id,
    shipping_service_clean AS shipping_service_desc
    FROM src_orders
    )

SELECT * FROM renamed_casted
