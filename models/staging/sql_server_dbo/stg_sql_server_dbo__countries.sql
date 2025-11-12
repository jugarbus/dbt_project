{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['country']) }} AS country_id,
    country AS country_desc
    FROM src_orders
    )

SELECT  * FROM renamed_casted




