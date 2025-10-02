{% macro calculate_moving_averages(column, periods=[7, 30, 60, 90], partition_by='fromcurrency', order_by='created_at', round_digits=3) %}
  {%- for period in periods -%}
    ROUND(
        AVG({{ column }}) OVER (
            PARTITION BY {{ partition_by }} 
            ORDER BY {{ order_by }} 
            ROWS BETWEEN {{ period - 1 }} PRECEDING AND CURRENT ROW
        ),
        {{ round_digits }}
    ) AS movingAverage{{ period }}Days
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{% endmacro %}

{% macro calculate_lagged_values(column, partition_by='fromcurrency', order_by='created_at', lag_periods=[1], suffix='') %}
  {%- for lag_period in lag_periods -%}
    LAG({{ column }}, {{ lag_period }}) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
    ) AS previousDay{{ suffix if suffix else column.split('.')[-1] | title }}
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{% endmacro %}

{% macro calculate_daily_changes(column, partition_by='fromcurrency', order_by='created_at', suffix='') %}
  {{ column }} - (
    LAG({{ column }}, 1) OVER (
        PARTITION BY {{ partition_by }} 
        ORDER BY {{ order_by }}
    )
  ) AS dailyChange{{ suffix if suffix else column.split('.')[-1] | title }}
{% endmacro %}

{#
  Usage Examples:
  
  -- Calculate 7, 30, 60, 90-day moving averages
  SELECT 
    fromcurrency,
    created_at,
    {{ calculate_moving_averages('rates.rate', [7, 30, 60, 90]) }}
  FROM rates
  
  -- Calculate moving averages with custom precision
  SELECT 
    {{ calculate_moving_averages('price', [5, 10, 20], 'symbol', 'date', 4) }}
  FROM stock_prices
  
  -- Calculate lagged values (previous day rates)  
  SELECT
    {{ calculate_lagged_values('rates.rate') }},
    {{ calculate_lagged_values('1/rates.rate') }}
  FROM rates
  
  -- Calculate daily changes
  SELECT
    {{ calculate_daily_changes('rates.rate') }},
    {{ calculate_daily_changes('1/rates.rate') }}
  FROM rates
  
  Output:
  ROUND(AVG(rates.rate) OVER (...ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 3) AS movingAverage7Days,
  ROUND(AVG(rates.rate) OVER (...ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 3) AS movingAverage30Days,
  ...
#}
