{% macro bollinger_bands(current_rate, moving_average, volatility, num_std_dev=2) %}
  {{ moving_average }} + ({{ num_std_dev }} * {{ volatility }}) AS bollinger_upper,
  {{ moving_average }} - ({{ num_std_dev }} * {{ volatility }}) AS bollinger_lower,
  CASE 
    WHEN {{ volatility }} > 0 THEN
      ({{ current_rate }} - {{ moving_average }}) / ({{ num_std_dev }} * {{ volatility }}) * 100
    ELSE 0
  END AS bollinger_position_pct
{% endmacro %}

{% macro classify_performance(performance_column, thresholds=[10, 3, -3, -10]) %}
  CASE 
    WHEN {{ performance_column }} >= {{ thresholds[0] }} THEN 'Strong Gain'
    WHEN {{ performance_column }} >= {{ thresholds[1] }} THEN 'Moderate Gain'
    WHEN {{ performance_column }} BETWEEN {{ thresholds[3] }} AND {{ thresholds[2] }} THEN 'Stable'
    WHEN {{ performance_column }} >= {{ thresholds[3] }} THEN 'Moderate Loss'
    ELSE 'Strong Loss'
  END 
{% endmacro %}

{% macro classify_volatility_regime(volatility_percentile) %}
  CASE 
    WHEN {{ volatility_percentile }} >= 90 THEN 'Extreme Volatility'
    WHEN {{ volatility_percentile }} >= 75 THEN 'High Volatility'
    WHEN {{ volatility_percentile }} >= 25 THEN 'Normal Volatility'
    ELSE 'Low Volatility'
  END AS volatility_regime
{% endmacro %}

{% macro classify_risk_level(volatility_rank, total_currencies) %}
  CASE 
    WHEN {{ volatility_rank }} <= 5 THEN 'Extreme Risk'
    WHEN {{ volatility_rank }} <= {{ total_currencies }} * 0.1 THEN 'High Risk'
    WHEN {{ volatility_rank }} <= {{ total_currencies }} * 0.3 THEN 'Medium Risk'
    ELSE 'Low Risk'
  END AS risk_classification
{% endmacro %}

{% macro top_bottom_flags(rank_column, total_currencies, top_n=5) %}
  CASE WHEN {{ rank_column }} <= {{ top_n }} THEN TRUE ELSE FALSE END AS is_top_{{ top_n }},
  CASE WHEN {{ rank_column }} > {{ total_currencies }} - {{ top_n }} THEN TRUE ELSE FALSE END AS is_bottom_{{ top_n }}
{% endmacro %}

{#
  Usage Examples:
  
  -- Bollinger Bands calculation
  SELECT
    {{ bollinger_bands('c.unitsToUSD', 'c.movingAverage30Days', 'h.volatility_30d') }}
  FROM current_rates c JOIN historical_volatility h ON ...
  
  -- Performance classification
  SELECT
    {{ classify_performance('ytd_performance_pct') }}
  FROM performance_data
  
  -- Custom thresholds for performance classification
  SELECT
    {{ classify_performance('ytd_performance_pct', [15, 5, -5, -15]) }}
  FROM performance_data
  
  -- Volatility regime classification
  SELECT
    {{ classify_volatility_regime('volatility_30d_percentile') }}
  FROM volatility_analysis
  
  -- Risk classification
  SELECT
    {{ classify_risk_level('volatility_rank_30d', 'total_currencies') }}
  FROM risk_metrics
  
  -- Top/Bottom performer flags
  SELECT
    {{ top_bottom_flags('ytd_rank', 'total_currencies') }}
  FROM performance_rankings
#}
