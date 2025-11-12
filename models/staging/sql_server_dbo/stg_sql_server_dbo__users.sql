{{ 
  config(
    materialized='incremental'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}

    {% if is_incremental() %}
      where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}
),

cleaned AS (
SELECT
    address_id,
    CONVERT_TIMEZONE('UTC', created_at) AS created_at_utc,
    email,
    first_name,
    last_name,
    phone_number,
    CONVERT_TIMEZONE('UTC', updated_at) AS updated_at_utc,
    user_id,
    _fivetran_deleted,
    CONVERT_TIMEZONE('UTC', _fivetran_synced) AS _fivetran_synced_utc
FROM src_users

)

SELECT * FROM cleaned

