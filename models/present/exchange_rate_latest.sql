{{
    config(
        materialized = 'table'
    )
}}

SELECT
    {{ dbt_utils.star(from=ref('fact_exchange_rates_daily')) }}
FROM 
    {{ ref("fact_exchange_rates_daily") }}
WHERE created_at = (
    SELECT MAX(created_at) 
    FROM {{ ref('fact_exchange_rates_daily') }}
)