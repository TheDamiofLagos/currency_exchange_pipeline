WITH 
    base AS (
        SELECT
            *
        FROM {{ source('open_exchange', 'open_exchange_currency_raw') }}
    )

SELECT
    code,
    _fivetran_deleted,
    _fivetran_synced,
    name
FROM 
    base