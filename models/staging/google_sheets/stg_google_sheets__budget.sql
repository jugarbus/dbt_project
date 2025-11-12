{{ config(
    materialized='incremental'
    ) 
    }}

WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}
    ),

renamed_casted AS (
SELECT
    {{ dbt_utils.generate_surrogate_key(['_row', 'month', 'quantity']) }} AS row_hash_id,
    _row AS row_id,
    TO_DATE(month) AS month_date,
    product_id,
    quantity,
    CONVERT_TIMEZONE('UTC', _fivetran_synced) AS fivetran_synced_utc
FROM stg_budget_products

    )

SELECT * FROM renamed_casted

{% if is_incremental() %}

  where fivetran_synced_utc > (select max(fivetran_synced_utc) from {{ this }})

{% endif %}