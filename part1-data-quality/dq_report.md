# Incoming Data Quality Report

## Summary of Checks

- **schema_per_file** → *warn* — {'events_20250304.csv': ['referrer'], 'events_20250305.csv': ['referrer'], 'events_20250306.csv': ['referrer'], 'events_20250307.csv': ['referrer'], 'events_20250308.csv': ['referrer']}
- **row_counts** → *pass* — {}
- **schema** → *pass* — missing=[]
- **timestamp** → *pass* — bad_timestamps=0
- **event_taxonomy** → *fail* — ['checkout_completed']
- **json_parse** → *pass* — 
- **duplicates** → *fail* — 
- **client_id_nulls** → *warn* — 17237
- **referrer_anonymization** → *pass* — anonymized_rate=0.008
- **utm_coverage** → *pass* — utm_rate=0.266
- **purchase_price_validation** → *pass* — missing=0, zero_or_negative=0