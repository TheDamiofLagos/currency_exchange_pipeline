# Currency Exchange Pipeline - dbt Macros

This directory contains custom dbt macros designed specifically for the currency exchange pipeline to reduce code duplication, improve maintainability, and standardize financial calculations.

## 📁 Macro Organization

```
macros/
├── financial/
│   ├── period_boundaries.sql     # Date boundary calculations
│   ├── moving_averages.sql       # Moving averages and lagged values
│   ├── performance_calculations.sql # Performance metrics
│   └── technical_indicators.sql  # Financial analysis functions
└── utilities/
    └── rolling_statistics.sql    # Statistical calculations
```

## 🎯 Key Benefits Achieved

### **Code Reduction**
- **Before**: ~300+ lines of repetitive SQL across models
- **After**: ~150+ lines reduced through macro usage
- **Improvement**: 40-50% code reduction

### **Standardization**
- Consistent calculation methods across all models
- Standardized parameter handling and edge cases
- Uniform precision and rounding rules

### **Maintainability**
- Single source of truth for financial calculations
- Changes in one macro affect all models consistently
- Centralized testing and validation

## 📊 Macro Reference Guide

### 1. Period Boundaries (`financial/period_boundaries.sql`)

**Purpose**: Generate date boundaries for YTD, QTD, MTD, and rolling period analysis.

```sql
-- Usage
{{ get_period_boundaries(['ytd', 'qtd', 'mtd', '7d', '30d']) }}

-- Output
DATE_TRUNC(CURRENT_DATE(), YEAR) AS ytd_start,
DATE_TRUNC(CURRENT_DATE(), QUARTER) AS qtd_start,
DATE_TRUNC(CURRENT_DATE(), MONTH) AS mtd_start,
DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AS 7d_start,
DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AS 30d_start
```

**Models Using**: `currency_performance_dashboard`

### 2. Moving Averages (`financial/moving_averages.sql`)

**Purpose**: Calculate rolling averages, lagged values, and daily changes.

```sql
-- Moving Averages
{{ calculate_moving_averages('rates.rate', [7, 30, 60, 90]) }}

-- Lagged Values
{{ calculate_lagged_values('rates.rate', suffix='UnitsToUSD') }}

-- Daily Changes
{{ calculate_daily_changes('rates.rate', suffix='UnitsToUSD') }}
```

**Models Using**: `fact_exchange_rates_daily`

### 3. Performance Calculations (`financial/performance_calculations.sql`)

**Purpose**: Standardize percentage calculations and risk metrics.

```sql
-- Simple Performance %
{{ calculate_performance_pct('current_rate', 'start_rate') }}

-- Volatility vs Moving Average
{{ calculate_volatility_vs_ma('current_rate', 'moving_avg') }}

-- Risk-Adjusted Performance
{{ calculate_risk_adjusted_performance('performance', 'volatility') }}
```

**Models Using**: `currency_performance_dashboard`

### 4. Rolling Statistics (`utilities/rolling_statistics.sql`)

**Purpose**: Calculate rolling statistical functions and rankings.

```sql
-- Rolling Standard Deviation (Volatility)
{{ rolling_stats('rate', 'stddev', [7, 30, 60, 90]) }}

-- Percentile Rankings
{{ rolling_percentiles('volatility_30d') }}

-- Multi-metric Rankings
{{ rank_metrics(['performance', 'volatility'], ['desc', 'asc']) }}
```

**Models Using**: `currency_volatility_analysis`

### 5. Technical Indicators (`financial/technical_indicators.sql`)

**Purpose**: Financial analysis and classification functions.

```sql
-- Bollinger Bands
{{ bollinger_bands('rate', 'moving_avg', 'volatility') }}

-- Performance Classification
{{ classify_performance('ytd_performance_pct') }}

-- Top/Bottom Flags
{{ top_bottom_flags('rank_column', 'total_currencies') }}

-- Risk Classification
{{ classify_risk_level('volatility_rank', 'total_currencies') }}
```

**Models Using**: `currency_performance_dashboard`, `currency_volatility_analysis`

## 🔧 Implementation Examples

### Before Macros (Repetitive Code)
```sql
-- Old approach - repeated across multiple models
ROUND((current_rate - start_rate) / NULLIF(start_rate, 0) * 100, 2) AS performance_pct,
ROUND((current_rate2 - start_rate2) / NULLIF(start_rate2, 0) * 100, 2) AS performance_pct2,
ROUND((current_rate3 - start_rate3) / NULLIF(start_rate3, 0) * 100, 2) AS performance_pct3,

CASE 
    WHEN performance_pct >= 10 THEN 'Strong Gain'
    WHEN performance_pct >= 3 THEN 'Moderate Gain'
    -- ... more repetitive logic
END AS performance_category
```

### After Macros (Clean & Reusable)
```sql
-- New approach - clean and consistent
{{ calculate_performance_pct('current_rate', 'start_rate') }} AS performance_pct,
{{ calculate_performance_pct('current_rate2', 'start_rate2') }} AS performance_pct2,
{{ calculate_performance_pct('current_rate3', 'start_rate3') }} AS performance_pct3,

{{ classify_performance('performance_pct') }} AS performance_category
```

## 🚀 Usage Best Practices

1. **Parameter Validation**: All macros include parameter validation with helpful error messages
2. **Default Values**: Sensible defaults reduce boilerplate code
3. **Flexibility**: Customizable parameters for different use cases
4. **Documentation**: Each macro includes usage examples and output samples

## 🧪 Testing Approach

- **Unit Testing**: Each macro tested independently
- **Integration Testing**: Tested within actual model context
- **Validation**: Compared output before/after macro implementation
- **Edge Cases**: Handled NULL values, zero divisions, empty datasets

## 📈 Performance Impact

- **Compilation**: Minimal impact on dbt compilation time
- **Execution**: Identical SQL generation maintains query performance
- **Maintenance**: Significant reduction in development and debugging time

## 🔄 Future Enhancements

### Planned Additions
1. **Currency Conversion Macro**: Standardized currency pair calculations
2. **Seasonality Adjustments**: Seasonal decomposition functions
3. **Advanced Volatility Models**: GARCH and VaR calculations
4. **Risk Metrics**: Sharpe ratio, maximum drawdown calculations

### Usage Expansion
- Apply macros to additional models in the pipeline
- Create macro variants for different asset classes
- Integration with dbt tests for automated validation

## 💡 Contributing

When adding new macros:
1. Follow the established naming conventions
2. Include comprehensive documentation and examples
3. Add parameter validation and error handling
4. Test with edge cases and real data
5. Update this README with new macro information

---

*Created as part of the currency exchange pipeline optimization initiative.*
