SELECT
    *
FROM    
    {{ ref('base_open_exchange__open_exchange_rates_raw') }}
WHERE rate < 0