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
    rates.rate - (LAG(rates.rate, 1) OVER (PARTITION BY rates.fromcurrency ORDER BY rates.created_at)) AS dailyChangeUnitsToUSD,
    1/rates.rate AS USDToUnits,
    LAG(1/rates.rate, 1) OVER (PARTITION BY rates.fromcurrency ORDER BY rates.created_at) AS previousDayUSDToUnits,
    (1/rates.rate) - (LAG(1/rates.rate, 1) OVER (PARTITION BY rates.fromcurrency ORDER BY rates.created_at)) AS dailyChangeUSDToUnits,
    ROUND(
        AVG(rates.rate) OVER (PARTITION BY rates.tocurrency, rates.fromcurrency ORDER BY rates.created_at ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        3) AS movingAverage7Days,
    ROUND(
        AVG(rates.rate) OVER (PARTITION BY rates.tocurrency, rates.fromcurrency ORDER BY rates.created_at ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        3) AS movingAverage30Days,
    ROUND(
        AVG(rates.rate) OVER (PARTITION BY rates.tocurrency, rates.fromcurrency ORDER BY rates.created_at ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
        3) AS movingAverage60Days,
    ROUND(
        AVG(rates.rate) OVER (PARTITION BY rates.tocurrency, rates.fromcurrency ORDER BY rates.created_at ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
        3) AS movingAverage90Days,
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