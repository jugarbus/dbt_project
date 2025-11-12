-- Aquí intenta que el registro de no promos lo enchufe de promos y no lo hagas guarrillo

{{ 
  config(
    materialized='incremental'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
),
src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }} 
    )
    ,


cleaned AS (
    SELECT
        *,
        COALESCE(NULLIF(promo_id, ''), 'no promo') AS promo_id_clean,
        COALESCE(NULLIF(shipping_service, ''), 'other') AS shipping_service_clean

    FROM src_orders
),

renamed_casted AS (
    SELECT
        order_id,
        {{ dbt_utils.generate_surrogate_key(['shipping_service_clean']) }} AS shipping_id,
        shipping_cost,
        address_id,
        CONVERT_TIMEZONE('UTC', created_at) AS created_at_utc,
        {{ dbt_utils.generate_surrogate_key(['promo_id_clean']) }} AS promo_id,
        CONVERT_TIMEZONE('UTC', estimated_delivery_at) AS estimated_delivery_at_utc,
        order_cost AS order_cost_dollars,
        user_id,
        order_total AS order_total_dollars,
        CONVERT_TIMEZONE('UTC', delivered_at) AS delivered_at_utc,
        tracking_id,
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_id,
        _fivetran_deleted,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS date_load_utc
    FROM cleaned
)

SELECT * FROM renamed_casted

{% if is_incremental() %}

  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}