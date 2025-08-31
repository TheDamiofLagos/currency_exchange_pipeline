{{
    config(
        materialized = 'incremental'
    )
}}

SELECT
    rates.tocurrency,
    currency.currency_name,
    rates.fromcurrency,
    rates.created_at,
    rates.rate
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