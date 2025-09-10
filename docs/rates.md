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