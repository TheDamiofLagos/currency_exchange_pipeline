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
    LAG(rates.rate, 1) OVER (PARTITION BY rates.fromcurrency ORDER BY rates.created_at) AS previousDayUnitsToUSD,
    rates.rate - (LAG(rates.rate, 1) OVER (PARTITION BY rates.fromcurrency ORDER BY rates.created_at)) AS dailyChange,
    1/rates.rate AS USDToUnits
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