WITH 
    base AS (
        SELECT
            *
        FROM {{ source('open_exchange', 'open_exchange_rates_raw') }}
    )

SELECT
    currency_code,
    data_base,
    data_timestamps,
    _fivetran_deleted,
    _fivetran_synced,
    rate
FROM 
    base