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