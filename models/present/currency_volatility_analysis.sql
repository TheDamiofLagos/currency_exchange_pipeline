{{
    config(
        materialized = 'table',
        description = 'Comprehensive currency volatility analysis with Bollinger Bands, volatility regimes, and risk metrics'
    )
}}

WITH current_rates AS (
    -- ✅ Reuse existing exchange_rate_latest model (single source of truth)
    SELECT *
    FROM {{ ref('exchange_rate_latest') }}
),

daily_changes_with_pct AS (
    SELECT 
        fromcurrency,
        created_at,
        unitsToUSD,
        dailyChangeUnitsToUSD,
        movingAverage30Days,
        LAG(unitsToUSD, 1) OVER (PARTITION BY fromcurrency ORDER BY created_at) AS prev_rate,
        ABS(dailyChangeUnitsToUSD / NULLIF(LAG(unitsToUSD, 1) OVER (PARTITION BY fromcurrency ORDER BY created_at), 0)) * 100 AS daily_change_pct_abs
    FROM {{ ref('fact_exchange_rates_daily') }}
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),

historical_volatility AS (
    SELECT 
        fromcurrency,
        created_at,
        unitsToUSD,
        dailyChangeUnitsToUSD,
        movingAverage30Days,
        daily_change_pct_abs,
        
        -- Calculate rolling standard deviations (volatility)
        STDDEV(unitsToUSD) OVER (
            PARTITION BY fromcurrency 
            ORDER BY created_at 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volatility_7d,
        
        STDDEV(unitsToUSD) OVER (
            PARTITION BY fromcurrency 
            ORDER BY created_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d,
        
        STDDEV(unitsToUSD) OVER (
            PARTITION BY fromcurrency 
            ORDER BY created_at 
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS volatility_60d,
        
        STDDEV(unitsToUSD) OVER (
            PARTITION BY fromcurrency 
            ORDER BY created_at 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS volatility_90d,
        
        -- Rolling average of absolute percentage changes (now using pre-calculated values)
        AVG(daily_change_pct_abs) OVER (
            PARTITION BY fromcurrency 
            ORDER BY created_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_daily_volatility_30d
        
    FROM daily_changes_with_pct
),

latest_volatility AS (
    SELECT *
    FROM historical_volatility
    WHERE created_at = (SELECT MAX(created_at) FROM historical_volatility)
),

volatility_percentiles AS (
    SELECT 
        fromcurrency,
        volatility_30d,
        avg_daily_volatility_30d,
        
        -- Calculate percentiles for volatility context
        PERCENT_RANK() OVER (ORDER BY volatility_30d) * 100 AS volatility_30d_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_daily_volatility_30d) * 100 AS daily_volatility_percentile,
        
        -- Global volatility rankings
        ROW_NUMBER() OVER (ORDER BY volatility_30d DESC) AS volatility_rank_30d,
        ROW_NUMBER() OVER (ORDER BY avg_daily_volatility_30d DESC) AS daily_volatility_rank
        
    FROM latest_volatility
),

current_analysis AS (
    SELECT 
        c.fromcurrency,
        c.currency_name,
        c.tocurrency,
        c.created_at,
        c.unitsToUSD AS current_rate,
        c.dailyChangeUnitsToUSD AS daily_change,
        c.movingAverage30Days,
        
        -- Volatility metrics
        h.volatility_7d,
        h.volatility_30d,
        h.volatility_60d,
        h.volatility_90d,
        h.avg_daily_volatility_30d,
        h.daily_change_pct_abs AS current_daily_change_pct,
        
        -- Percentiles and rankings
        v.volatility_30d_percentile,
        v.daily_volatility_percentile,
        v.volatility_rank_30d,
        v.daily_volatility_rank,
        
        -- Bollinger Bands (30-day moving average ± 2 standard deviations)
        c.movingAverage30Days + (2 * h.volatility_30d) AS bollinger_upper,
        c.movingAverage30Days - (2 * h.volatility_30d) AS bollinger_lower,
        
        -- Band position (where current rate sits within Bollinger Bands)
        CASE 
            WHEN h.volatility_30d > 0 THEN
                (c.unitsToUSD - c.movingAverage30Days) / (2 * h.volatility_30d) * 100
            ELSE 0
        END AS bollinger_position_pct,
        
        COUNT(*) OVER () AS total_currencies
        
    FROM current_rates c
    JOIN latest_volatility h 
        ON c.fromcurrency = h.fromcurrency
    JOIN volatility_percentiles v 
        ON c.fromcurrency = v.fromcurrency
)

SELECT *,
    -- Volatility regime classification
    CASE 
        WHEN volatility_30d_percentile >= 90 THEN 'Extreme Volatility'
        WHEN volatility_30d_percentile >= 75 THEN 'High Volatility'
        WHEN volatility_30d_percentile >= 25 THEN 'Normal Volatility'
        ELSE 'Low Volatility'
    END AS volatility_regime,
    
    -- Bollinger Band signals
    CASE 
        WHEN current_rate > bollinger_upper THEN 'Above Upper Band'
        WHEN current_rate < bollinger_lower THEN 'Below Lower Band'
        WHEN bollinger_position_pct > 50 THEN 'Upper Half'
        WHEN bollinger_position_pct < -50 THEN 'Lower Half'
        ELSE 'Middle Range'
    END AS bollinger_signal,
    
    -- Volatility breakout detection
    CASE 
        WHEN current_daily_change_pct > avg_daily_volatility_30d * 2 THEN 'High Volatility Breakout'
        WHEN current_daily_change_pct > avg_daily_volatility_30d * 1.5 THEN 'Moderate Volatility Spike'
        WHEN current_daily_change_pct < avg_daily_volatility_30d * 0.5 THEN 'Low Volatility Day'
        ELSE 'Normal Volatility'
    END AS volatility_breakout_status,
    
    -- Risk classification
    CASE 
        WHEN volatility_rank_30d <= 5 THEN 'Extreme Risk'
        WHEN volatility_rank_30d <= total_currencies * 0.1 THEN 'High Risk'
        WHEN volatility_rank_30d <= total_currencies * 0.3 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_classification,
    
    -- Volatility trend (comparing different timeframes)
    CASE 
        WHEN volatility_7d > volatility_30d * 1.2 THEN 'Increasing Volatility'
        WHEN volatility_7d < volatility_30d * 0.8 THEN 'Decreasing Volatility'
        ELSE 'Stable Volatility'
    END AS volatility_trend,
    
    CURRENT_DATETIME() AS analysis_updated_at
    
FROM current_analysis
ORDER BY volatility_rank_30d ASC
