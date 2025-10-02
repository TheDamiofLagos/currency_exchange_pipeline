{{
    config(
        materialized = 'table',
        description = 'Currency performance dashboard with YTD, QTD, MTD metrics and rankings'
    )
}}

WITH current_rates AS (
    SELECT 
        fromcurrency,
        currency_name,
        tocurrency,
        unitsToUSD,
        USDToUnits,
        created_at,
        dailyChangeUnitsToUSD,
        movingAverage30Days
    FROM {{ ref('fact_exchange_rates_daily') }}
    WHERE created_at = (SELECT MAX(created_at) FROM {{ ref('fact_exchange_rates_daily') }})
),

period_boundaries AS (
    SELECT
        {{ get_period_boundaries(['ytd', 'qtd', 'mtd', '7d', '30d']) }}
),

historical_rates AS (
    SELECT 
        fromcurrency,
        created_at,
        unitsToUSD,
        CASE 
            WHEN created_at >= (SELECT ytd_start FROM period_boundaries) THEN 'YTD'
            WHEN created_at >= (SELECT qtd_start FROM period_boundaries) THEN 'QTD' 
            WHEN created_at >= (SELECT mtd_start FROM period_boundaries) THEN 'MTD'
            WHEN created_at >= (SELECT 7d_start FROM period_boundaries) THEN '7D'
            WHEN created_at >= (SELECT 30d_start FROM period_boundaries) THEN '30D'
        END AS period_type,
        ROW_NUMBER() OVER (
            PARTITION BY fromcurrency, 
            CASE 
                WHEN created_at >= (SELECT ytd_start FROM period_boundaries) THEN 'YTD'
                WHEN created_at >= (SELECT qtd_start FROM period_boundaries) THEN 'QTD'
                WHEN created_at >= (SELECT mtd_start FROM period_boundaries) THEN 'MTD'
                WHEN created_at >= (SELECT 7d_start FROM period_boundaries) THEN '7D'
                WHEN created_at >= (SELECT 30d_start FROM period_boundaries) THEN '30D'
            END
            ORDER BY created_at ASC
        ) AS rn
    FROM {{ ref('fact_exchange_rates_daily') }}
    WHERE created_at >= (SELECT 30d_start FROM period_boundaries)
),

period_start_rates AS (
    SELECT 
        fromcurrency,
        period_type,
        unitsToUSD AS period_start_rate
    FROM historical_rates 
    WHERE rn = 1
),

performance_metrics AS (
    SELECT 
        c.fromcurrency,
        c.currency_name,
        c.tocurrency,
        c.unitsToUSD AS current_rate,
        c.dailyChangeUnitsToUSD AS daily_change,
        c.dailyChangeUnitsToUSD / NULLIF(c.unitsToUSD - c.dailyChangeUnitsToUSD, 0) * 100 AS daily_change_pct,
        
        -- Performance calculations using macro (much cleaner!)
        {{ calculate_performance_pct('c.unitsToUSD', 'ytd.period_start_rate') }} AS ytd_performance_pct,
        {{ calculate_performance_pct('c.unitsToUSD', 'qtd.period_start_rate') }} AS qtd_performance_pct,
        {{ calculate_performance_pct('c.unitsToUSD', 'mtd.period_start_rate') }} AS mtd_performance_pct,
        {{ calculate_performance_pct('c.unitsToUSD', 'week.period_start_rate') }} AS week_7d_performance_pct,
        {{ calculate_performance_pct('c.unitsToUSD', 'month.period_start_rate') }} AS month_30d_performance_pct,
        
        -- Volatility proxy using macro
        {{ calculate_volatility_vs_ma('c.unitsToUSD', 'c.movingAverage30Days') }},
        
        c.created_at AS as_of_date
        
    FROM current_rates c
    LEFT JOIN period_start_rates ytd ON c.fromcurrency = ytd.fromcurrency AND ytd.period_type = 'YTD'
    LEFT JOIN period_start_rates qtd ON c.fromcurrency = qtd.fromcurrency AND qtd.period_type = 'QTD'  
    LEFT JOIN period_start_rates mtd ON c.fromcurrency = mtd.fromcurrency AND mtd.period_type = 'MTD'
    LEFT JOIN period_start_rates week ON c.fromcurrency = week.fromcurrency AND week.period_type = '7D'
    LEFT JOIN period_start_rates month ON c.fromcurrency = month.fromcurrency AND month.period_type = '30D'
),

ranked_performance AS (
    SELECT *,
        -- Rankings (1 = best performer)
        ROW_NUMBER() OVER (ORDER BY ytd_performance_pct DESC) AS ytd_rank,
        ROW_NUMBER() OVER (ORDER BY qtd_performance_pct DESC) AS qtd_rank,
        ROW_NUMBER() OVER (ORDER BY mtd_performance_pct DESC) AS mtd_rank,
        ROW_NUMBER() OVER (ORDER BY week_7d_performance_pct DESC) AS week_7d_rank,
        ROW_NUMBER() OVER (ORDER BY month_30d_performance_pct DESC) AS month_30d_rank,
        
        -- Percentiles
        ROUND(PERCENT_RANK() OVER (ORDER BY ytd_performance_pct) * 100, 1) AS ytd_percentile,
        ROUND(PERCENT_RANK() OVER (ORDER BY qtd_performance_pct) * 100, 1) AS qtd_percentile,
        ROUND(PERCENT_RANK() OVER (ORDER BY mtd_performance_pct) * 100, 1) AS mtd_percentile,
        
        COUNT(*) OVER () AS total_currencies
    FROM performance_metrics
)

SELECT *,
    -- Using macros for cleaner classification logic
    {{ classify_performance('ytd_performance_pct') }} AS ytd_performance_category,
    
    -- Top/Bottom performer flags using macro
    {{ top_bottom_flags('ytd_rank', 'total_currencies') }},
    
    -- Risk-adjusted performance using macro
    {{ calculate_risk_adjusted_performance('ytd_performance_pct', 'volatility_vs_ma') }},
    
    CURRENT_DATETIME() AS dashboard_updated_at
    
FROM ranked_performance
ORDER BY ytd_performance_pct DESC
