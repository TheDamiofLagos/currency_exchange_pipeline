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
        DATE_TRUNC(CURRENT_DATE(), YEAR) AS ytd_start,
        DATE_TRUNC(CURRENT_DATE(), QUARTER) AS qtd_start,
        DATE_TRUNC(CURRENT_DATE(), MONTH) AS mtd_start,
        DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS week_start,
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS month_start
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
            WHEN created_at >= (SELECT week_start FROM period_boundaries) THEN '7D'
            WHEN created_at >= (SELECT month_start FROM period_boundaries) THEN '30D'
        END AS period_type,
        ROW_NUMBER() OVER (
            PARTITION BY fromcurrency, 
            CASE 
                WHEN created_at >= (SELECT ytd_start FROM period_boundaries) THEN 'YTD'
                WHEN created_at >= (SELECT qtd_start FROM period_boundaries) THEN 'QTD'
                WHEN created_at >= (SELECT mtd_start FROM period_boundaries) THEN 'MTD'
                WHEN created_at >= (SELECT week_start FROM period_boundaries) THEN '7D'
                WHEN created_at >= (SELECT month_start FROM period_boundaries) THEN '30D'
            END
            ORDER BY created_at ASC
        ) AS rn
    FROM {{ ref('fact_exchange_rates_daily') }}
    WHERE created_at >= (SELECT month_start FROM period_boundaries)
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
        
        -- Performance calculations
        ROUND((c.unitsToUSD - ytd.period_start_rate) / NULLIF(ytd.period_start_rate, 0) * 100, 2) AS ytd_performance_pct,
        ROUND((c.unitsToUSD - qtd.period_start_rate) / NULLIF(qtd.period_start_rate, 0) * 100, 2) AS qtd_performance_pct,
        ROUND((c.unitsToUSD - mtd.period_start_rate) / NULLIF(mtd.period_start_rate, 0) * 100, 2) AS mtd_performance_pct,
        ROUND((c.unitsToUSD - week.period_start_rate) / NULLIF(week.period_start_rate, 0) * 100, 2) AS week_7d_performance_pct,
        ROUND((c.unitsToUSD - month.period_start_rate) / NULLIF(month.period_start_rate, 0) * 100, 2) AS month_30d_performance_pct,
        
        -- Volatility proxy using moving average deviation
        ROUND(ABS(c.unitsToUSD - c.movingAverage30Days) / NULLIF(c.movingAverage30Days, 0) * 100, 2) AS volatility_vs_ma30,
        
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
    -- Performance categories
    CASE 
        WHEN ytd_performance_pct >= 10 THEN 'Strong Gain'
        WHEN ytd_performance_pct >= 3 THEN 'Moderate Gain'
        WHEN ytd_performance_pct BETWEEN -3 AND 3 THEN 'Stable'
        WHEN ytd_performance_pct >= -10 THEN 'Moderate Loss'
        ELSE 'Strong Loss'
    END AS ytd_performance_category,
    
    -- Top/Bottom performer flags
    CASE WHEN ytd_rank <= 5 THEN TRUE ELSE FALSE END AS is_top_5_ytd,
    CASE WHEN ytd_rank > total_currencies - 5 THEN TRUE ELSE FALSE END AS is_bottom_5_ytd,
    
    -- Risk-adjusted performance (simplified)
    CASE 
        WHEN volatility_vs_ma30 > 0 
        THEN ROUND(ytd_performance_pct / NULLIF(volatility_vs_ma30, 0), 2) 
        ELSE NULL 
    END AS risk_adjusted_ytd_performance,
    
    CURRENT_DATETIME() AS dashboard_updated_at
    
FROM ranked_performance
ORDER BY ytd_performance_pct DESC
