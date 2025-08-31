{{
  config(
    materialized = 'table'
    )
}}

SELECT
    currency_code AS tocurrency,
    data_base AS fromcurrency,
    DATE(data_timestamps) AS created_at,
    rate
FROM
    {{ ref('base_open_exchange__open_exchange_rates_raw') }}
WHERE
    _fivetran_deleted = FALSE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY currency_code ORDER BY DATE(data_timestamps) DESC
) = 1
