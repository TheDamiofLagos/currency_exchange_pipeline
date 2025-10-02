{% macro rolling_stats(column, stat_function='stddev', periods=[7, 30, 60, 90], partition_by='fromcurrency', order_by='created_at', round_digits=null) %}
  {%- set valid_functions = ['stddev', 'avg', 'min', 'max', 'sum', 'count'] -%}
  {%- if stat_function not in valid_functions -%}
    {%- do exceptions.raise_compiler_error("Invalid stat_function: " ~ stat_function ~ ". Valid functions are: " ~ valid_functions | join(', ')) -%}
  {%- endif -%}
  
  {%- for period in periods -%}
    {%- set column_name = stat_function ~ '_' ~ period ~ 'd' -%}
    {%- if round_digits -%}
      ROUND(
    {%- endif -%}
    {{ stat_function.upper() }}({{ column }}) OVER (
      PARTITION BY {{ partition_by }} 
      ORDER BY {{ order_by }} 
      ROWS BETWEEN {{ period - 1 }} PRECEDING AND CURRENT ROW
    )
    {%- if round_digits -%}
      , {{ round_digits }})
    {%- endif %} AS {{ column_name }}
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{% endmacro %}

{% macro rolling_percentiles(column, periods=[30], partition_by='fromcurrency', order_by='created_at', round_digits=1) %}
  {%- for period in periods -%}
    ROUND(
      PERCENT_RANK() OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ column }}
      ) * 100, 
      {{ round_digits }}
    ) AS {{ column | replace('.', '_') | replace(' ', '_') }}_{{ period }}d_percentile
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{% endmacro %}

{% macro rank_metrics(columns, directions=['desc'], partition_by=none) %}
  {%- if directions | length == 1 -%}
    {%- set directions = directions * (columns | length) -%}
  {%- elif directions | length != columns | length -%}
    {%- do exceptions.raise_compiler_error("directions length (" ~ directions | length ~ ") must be 1 or equal to columns length (" ~ columns | length ~ ")") -%}
  {%- endif -%}
  
  {%- for column in columns -%}
    {%- set direction = directions[loop.index0] -%}
    ROW_NUMBER() OVER (
      {%- if partition_by -%}PARTITION BY {{ partition_by }}{%- endif %}
      ORDER BY {{ column }} {{ direction.upper() }}
    ) AS {{ column | replace('.', '_') | replace(' ', '_') }}_rank
    {%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
{% endmacro %}

{#
  Usage Examples:
  
  -- Calculate rolling standard deviation (volatility)
  SELECT 
    {{ rolling_stats('unitsToUSD', 'stddev', [7, 30, 60, 90]) }}
  FROM historical_rates
  
  -- Calculate rolling averages with rounding
  SELECT 
    {{ rolling_stats('daily_change_pct', 'avg', [30, 90], round_digits=2) }}
  FROM price_changes
  
  -- Calculate percentiles
  SELECT
    {{ rolling_percentiles('volatility_30d', [30]) }},
    {{ rolling_percentiles('avg_daily_volatility_30d', [30]) }}
  FROM volatility_data
  
  -- Rank multiple metrics
  SELECT
    {{ rank_metrics(['ytd_performance_pct', 'volatility_30d'], ['desc', 'asc']) }}
  FROM performance_analysis
  
  Output:
  STDDEV(unitsToUSD) OVER (...ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS stddev_7d,
  STDDEV(unitsToUSD) OVER (...ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS stddev_30d,
  ...
#}
