{{
    config(
        materialized = 'incremental'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['rates.fromcurrency', 'rates.created_at']) }} AS forexid,
    rates.tocurrency,
    currency.currency_name,
    rates.fromcurrency,
    rates.created_at,
    rates.rate AS unitsToUSD,
    
    -- Using macro for lagged values and daily changes with clean column names
    {{ calculate_lagged_values('rates.rate', suffix='UnitsToUSD') }},
    {{ calculate_daily_changes('rates.rate', suffix='UnitsToUSD') }},
    
    -- Currency conversion ratios
    1/rates.rate AS USDToUnits,
    LAG(1/rates.rate, 1) OVER (PARTITION BY rates.fromcurrency ORDER BY rates.created_at) AS previousDayUSDToUnits,
    (1/rates.rate) - (LAG(1/rates.rate, 1) OVER (PARTITION BY rates.fromcurrency ORDER BY rates.created_at)) AS dailyChangeUSDToUnits,
    
    -- Using macro for moving averages (much cleaner!)
    {{ calculate_moving_averages('rates.rate', [7, 30, 60, 90], 'rates.fromcurrency', 'rates.created_at') }}
FROM 
    {{ ref('prep_rates')}} AS rates
LEFT JOIN
    {{ ref("prep_currency")}} AS currency
ON
    rates.tocurrency = currency.currency_code
WHERE
    1 = 1
    {% if is_incremental() %}
    AND rates.created_at > (SELECT MAX(created_at) FROM {{this}})
    {% endif %}
