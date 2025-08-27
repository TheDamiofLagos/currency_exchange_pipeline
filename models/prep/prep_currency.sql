{{
  config(
    materialized = 'table'
    )
}}

SELECT
    code AS currency_code,
    name AS currency_name,
    _fivetran_synced AS created_at,
    /* commit */
FROM 
    {{ ref('base_open_exchange__open_exchange_currency_raw') }} AS currency
WHERE 
    _fivetran_deleted = FALSE
QUALIFY ROW_NUMBER() OVER (PARTITION BY code ORDER BY _fivetran_synced DESC) = 1