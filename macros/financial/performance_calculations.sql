{% macro calculate_performance_pct(current_value, start_value, round_digits=2) %}
  ROUND(({{ current_value }} - {{ start_value }}) / NULLIF({{ start_value }}, 0) * 100, {{ round_digits }})
{% endmacro %}

{% macro calculate_performance_metrics(current_rate, period_start_rates, periods=['ytd', 'qtd', 'mtd', '7d', '30d'], round_digits=2) %}
  {%- for period in periods -%}
    {{ calculate_performance_pct(current_rate, period_start_rates ~ '.' ~ period ~ '_rate', round_digits) }} AS {{ period }}_performance_pct
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{% endmacro %}

{% macro calculate_daily_change_pct(current_value, partition_by='fromcurrency', order_by='created_at', round_digits=2) %}
  ROUND(
    {{ current_value }} / NULLIF(
      LAG({{ current_value }}, 1) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
      ) - {{ current_value }}, 0
    ) * 100, 
    {{ round_digits }}
  ) AS daily_change_pct
{% endmacro %}

{% macro calculate_volatility_vs_ma(current_value, moving_average, round_digits=2) %}
  ROUND(
    ABS({{ current_value }} - {{ moving_average }}) / NULLIF({{ moving_average }}, 0) * 100, 
    {{ round_digits }}
  ) AS volatility_vs_ma
{% endmacro %}

{% macro calculate_risk_adjusted_performance(performance_pct, volatility_measure, round_digits=2) %}
  CASE 
    WHEN {{ volatility_measure }} > 0 
    THEN ROUND({{ performance_pct }} / NULLIF({{ volatility_measure }}, 0), {{ round_digits }}) 
    ELSE NULL 
  END AS risk_adjusted_performance
{% endmacro %}

{#
  Usage Examples:
  
  -- Calculate simple performance percentage
  SELECT 
    {{ calculate_performance_pct('current_rate', 'ytd_start_rate') }} AS ytd_performance
  FROM rates
  
  -- Calculate multiple period performances at once
  SELECT
    {{ calculate_performance_metrics('c.unitsToUSD', 'period_rates', ['ytd', 'qtd', 'mtd']) }}
  FROM current_rates c
  JOIN period_rates ON c.fromcurrency = period_rates.fromcurrency
  
  -- Calculate daily change percentage
  SELECT
    {{ calculate_daily_change_pct('rate') }}
  FROM exchange_rates
  
  -- Calculate volatility vs moving average
  SELECT
    {{ calculate_volatility_vs_ma('c.unitsToUSD', 'c.movingAverage30Days') }}
  FROM current_rates c
  
  -- Calculate risk-adjusted performance
  SELECT
    {{ calculate_risk_adjusted_performance('ytd_performance_pct', 'volatility_vs_ma30') }}
  FROM performance_data
  
  Output:
  ROUND((current_rate - ytd_start_rate) / NULLIF(ytd_start_rate, 0) * 100, 2) AS ytd_performance
#}
