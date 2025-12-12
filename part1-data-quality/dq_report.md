# Incoming Data Quality Report

## Summary of Checks

- **row_counts** → *pass* — file_row_stats=[{'source_file': 'events_20250223.csv', 'rows': 4070}, {'source_file': 'events_20250224.csv', 'rows': 3769}, {'source_file': 'events_20250225.csv', 'rows': 3470}, {'source_file': 'events_20250226.csv', 'rows': 3177}, {'source_file': 'events_20250227.csv', 'rows': 3393}, {'source_file': 'events_20250228.csv', 'rows': 3263}, {'source_file': 'events_20250301.csv', 'rows': 3489}, {'source_file': 'events_20250302.csv', 'rows': 4335}, {'source_file': 'events_20250303.csv', 'rows': 4042}, {'source_file': 'events_20250304.csv', 'rows': 3538}, {'source_file': 'events_20250305.csv', 'rows': 2881}, {'source_file': 'events_20250306.csv', 'rows': 3442}, {'source_file': 'events_20250307.csv', 'rows': 3251}, {'source_file': 'events_20250308.csv', 'rows': 3843}]
- **schema** → *pass* — missing=[]
- **timestamp** → *pass* — bad_timestamps=0
- **event_taxonomy** → *fail* — ['checkout_completed']
- **json_parse** → *pass* — 
- **duplicates** → *fail* — 
- **client_id_nulls** → *warn* — 
- **referrer_anonymization** → *pass* — anonymized_rate=0.008
- **utm_coverage** → *pass* — utm_rate=0.266
- **purchase_price_validation** → *pass* — missing=0, zero_or_negative=0

## Key Findings

- Invalid event names found: ['checkout_completed']
- Duplicate events: 9
- Null client IDs: 17237
- Referrer anonymization count: 398
- Purchase price missing: 0
- Purchase price ≤ 0: 0

## Recommended Ingestion Rules

- Fail the batch if purchase events contain missing or non-positive price.
- Trigger an alert if JSON parse errors exceed 1% of events.
- Trigger an alert if daily event volume deviates >3σ from baseline.
- Reject rows with unknown event_name.
- Reject events missing timestamp or with unparseable timestamp.
- Require `client_id` except for controlled exemptions.
- Track duplicate row rate and block repeated identical events.