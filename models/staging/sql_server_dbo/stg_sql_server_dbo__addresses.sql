{{
  config(
    materialized='incremental'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT DISTINCT 
    address_id,
    address AS address_desc,
    {{ dbt_utils.generate_surrogate_key(['country']) }} AS country_id,
    {{ dbt_utils.generate_surrogate_key(['state']) }} AS state_id,
    {{ dbt_utils.generate_surrogate_key(['zipcode']) }} AS zipcode_id


    FROM src_orders
    )

SELECT  * FROM renamed_casted




