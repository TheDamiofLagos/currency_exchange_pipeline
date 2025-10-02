{% macro get_period_boundaries(periods=['ytd', 'qtd', 'mtd', '7d', '30d', '60d', '90d']) %}
  {%- set period_map = {
    'ytd': 'DATE_TRUNC(CURRENT_DATE(), YEAR)',
    'qtd': 'DATE_TRUNC(CURRENT_DATE(), QUARTER)', 
    'mtd': 'DATE_TRUNC(CURRENT_DATE(), MONTH)',
    '7d': 'DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)',
    '30d': 'DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)',
    '60d': 'DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)',
    '90d': 'DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)',
    '1y': 'DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)'
  } -%}
  
  {%- for period in periods -%}
    {%- if period in period_map -%}
      {{ period_map[period] }} AS {{ period }}_start
      {%- if not loop.last -%},{%- endif -%}
    {%- else -%}
      {%- do exceptions.raise_compiler_error("Invalid period: " ~ period ~ ". Valid periods are: " ~ period_map.keys() | join(', ')) -%}
    {%- endif -%}
  {%- endfor -%}
{% endmacro %}

{#
  Usage Examples:
  
  -- Get common period boundaries
  SELECT {{ get_period_boundaries(['ytd', 'qtd', 'mtd', '30d']) }}
  
  -- Get custom period boundaries  
  SELECT {{ get_period_boundaries(['7d', '90d', '1y']) }}
  
  Output:
  DATE_TRUNC(CURRENT_DATE(), YEAR) AS ytd_start,
  DATE_TRUNC(CURRENT_DATE(), QUARTER) AS qtd_start,
  DATE_TRUNC(CURRENT_DATE(), MONTH) AS mtd_start,
  DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS 30d_start
#}
