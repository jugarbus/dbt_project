{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_id,
    status
    FROM src_orders
    )

SELECT  * FROM renamed_casted




