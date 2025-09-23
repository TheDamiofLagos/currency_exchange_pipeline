{% docs currency_code %}
Three-letter ISO 4217 currency code representing the currency used for this transaction or monetary value. 

Common examples:
- **USD**: United States Dollar
- **EUR**: Euro
- **GBP**: British Pound Sterling
- **CAD**: Canadian Dollar
- **JPY**: Japanese Yen

All currency codes follow the international ISO 4217 standard format.
{% enddocs %}


{% docs base_currency %}
The standardized currency used for financial reporting and analysis across all transactions, regardless of the original transaction currency. 

In our system, the base currency is **USD** (United States Dollar). All monetary amounts are converted from their original currency to USD using exchange rates from the transaction date to enable:

- Consistent financial reporting across regions
- Accurate revenue aggregation across currencies  
- Simplified cross-currency analysis and comparisons
- Standardized KPI calculations

Original transaction currencies are preserved in the `currency_code` field, while converted amounts use this base currency standard.
{% enddocs %}

{% docs rate %}
The exchange rate used to convert monetary amounts from the original transaction currency to our base currency (USD).

**Rate Interpretation:**
- Rate represents units of original currency per 1 USD
- Example: If rate = 1.25 for EUR, then 1 USD = 1.25 EUR
- Example: If rate = 0.85 for GBP, then 1 USD = 0.85 GBP

**Rate Source:**
Exchange rates are sourced from the open-exchange API and captured at the time of transaction processing to ensure accurate historical conversion.

**Usage:**
- Original Amount ÷ Rate = USD Amount
- Used for converting all monetary values to standardized USD base currency
- Enables consistent cross-currency reporting and analysis
{% enddocs %}


{% docs forexid %}
A unique identifier for each exchange rate record in our system.

This ID helps us track and reference specific exchange rates by combining:
- The currency being converted to (like USD, EUR, GBP)
- When that rate was recorded

**Why it's useful:**
- Ensures each exchange rate can be uniquely identified
- Prevents duplicate or conflicting rates
- Makes it easy to track how exchange rates change over time
- Helps maintain data accuracy in our financial reports

**Example:** 
Each time we get a new USD exchange rate, it gets its own unique ID so we can tell different rate updates apart.
{% enddocs %}

{% docs moving_averages %}
Rolling averages of exchange rates calculated over specified time periods to smooth out short-term fluctuations and identify trends.

**Available Moving Averages:**
- **7-day average**: Short-term trend indicator, useful for identifying recent market movements
- **30-day average**: Medium-term trend indicator, commonly used for monthly analysis
- **60-day average**: Longer-term trend indicator for quarterly planning
- **90-day average**: Long-term trend indicator for seasonal pattern analysis

**How they work:**
Moving averages are calculated using window functions that look back over the specified number of days and compute the average exchange rate. Each day's moving average includes that day plus the previous N-1 days.

**Usage:**
- Compare current rates to moving averages to identify if currencies are trading above or below trend
- Use multiple time frames together to understand short vs. long-term trends
- Helpful for risk management and forecasting
{% enddocs %}

{% docs daily_changes %}
Calculated differences between consecutive days' exchange rates, showing the absolute change in currency values from one day to the next.

**Types of Daily Changes:**
- **dailyChangeUnitsToUSD**: Change in foreign currency units per USD (rate - previous_rate)
- **dailyChangeUSDToUnits**: Change in USD per foreign currency unit (inverse_rate - previous_inverse_rate)

**Interpretation:**
- Positive values indicate the foreign currency strengthened relative to USD
- Negative values indicate the foreign currency weakened relative to USD
- Larger absolute values indicate higher volatility

**Business Use:**
- Monitor currency volatility and market movements
- Identify significant market events or trends
- Risk assessment for international transactions
- Alert systems for unusual currency movements
{% enddocs %}

{% docs data_timestamps %}
Timestamps and dates used throughout the exchange rate pipeline to track when rates were recorded and processed.

**Key Timestamp Fields:**
- **data_timestamps**: Original Unix timestamp from Open Exchange API when rate was captured
- **created_at**: Converted date field used for daily aggregation and analysis
- **_fivetran_synced**: When Fivetran last synchronized this record from the source
- **dbt_run_at**: When the dbt transformation was last executed

**Data Flow:**
1. Open Exchange API records rates with Unix timestamps
2. Fivetran syncs data and adds sync timestamps
3. dbt converts to dates for daily analysis
4. Final models include dbt execution timestamps for freshness tracking
