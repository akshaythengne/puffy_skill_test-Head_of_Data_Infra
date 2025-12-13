
# part4-monitoring/monitoring.py

import duckdb
import pandas as pd
import json
from datetime import date, timedelta

# -----------------------
# Config
# -----------------------
DATA_PATH = "part2-transformation/output"
REPORT_OUT = "part4-monitoring/monitoring_report.json"

BASELINE_DAYS = 7
MAX_NULL_CLIENT_RATE = 0.20
MAX_DUP_RATE = 0.001
MAX_JSON_ERROR_RATE = 0.01
MAX_DIRECT_SHARE = 0.80
MAX_REV_DROP = 0.40

# -----------------------
# Load data
# -----------------------
con = duckdb.connect(database=":memory:")

con.execute(f"""
CREATE TABLE events_enriched AS
SELECT * FROM read_parquet('{DATA_PATH}/events_enriched.parquet');
""")

con.execute(f"""
CREATE TABLE purchases AS
SELECT * FROM read_parquet('{DATA_PATH}/purchase_attribution.parquet');
""")

data_today = con.execute("""
SELECT MAX(DATE(timestamp_utc)) FROM events_enriched
""").fetchone()[0]

if data_today is None:
    raise ValueError("No data available to determine monitoring date")

data_today = pd.to_datetime(data_today).date()
start_baseline = data_today - timedelta(days=BASELINE_DAYS)

alerts = []

# -----------------------
# 1. Pipeline Health
# -----------------------
row_count = con.execute("SELECT COUNT(*) FROM events_enriched").fetchone()[0]
purchase_count = con.execute(
    "SELECT COUNT(*) FROM events_enriched WHERE event_name='purchase'"
).fetchone()[0]

if row_count == 0:
    alerts.append(("CRITICAL", "No events ingested today"))

if purchase_count == 0:
    alerts.append(("CRITICAL", "No purchases recorded today"))

# -----------------------
# 2. Data Integrity
# -----------------------
null_client_rate = con.execute("""
SELECT AVG(CASE WHEN client_id IS NULL THEN 1 ELSE 0 END) FROM events_enriched
""").fetchone()[0]

if null_client_rate > MAX_NULL_CLIENT_RATE:
    alerts.append(("WARN", f"High null client_id rate: {null_client_rate:.2%}"))

dup_rate = con.execute("""
SELECT
  COUNT(*) FILTER (WHERE cnt > 1) * 1.0 / COUNT(*)
FROM (
  SELECT COUNT(*) AS cnt
  FROM events_enriched
  GROUP BY source_file, timestamp_utc, event_name, event_data
)
""").fetchone()[0]

if dup_rate > MAX_DUP_RATE:
    alerts.append(("WARN", f"Duplicate rate high: {dup_rate:.2%}"))

json_error_rate = con.execute("""
SELECT AVG(CASE WHEN event_data IS NOT NULL AND event_data_parsed IS NULL THEN 1 ELSE 0 END)
FROM events_enriched
""").fetchone()[0]

if json_error_rate > MAX_JSON_ERROR_RATE:
    alerts.append(("WARN", f"JSON parse error rate high: {json_error_rate:.2%}"))

# -----------------------
# 3. Business Drift
# -----------------------
daily_rev = con.execute("""
SELECT
  DATE(timestamp_utc) AS d,
  SUM(total) AS revenue
FROM events_enriched
WHERE event_name='purchase'
GROUP BY 1
ORDER BY 1 DESC
""").fetchdf()

if len(daily_rev) > BASELINE_DAYS:
    latest = daily_rev.iloc[0]["revenue"]
    baseline = daily_rev.iloc[1:BASELINE_DAYS+1]["revenue"].mean()

    if baseline > 0 and (baseline - latest) / baseline > MAX_REV_DROP:
        alerts.append((
            "CRITICAL",
            f"Revenue drop detected: {latest:.0f} vs baseline {baseline:.0f}"
        ))

direct_share = con.execute("""
SELECT
  SUM(revenue) FILTER (WHERE COALESCE(last_utm_source,'direct')='direct')
  / SUM(revenue)
FROM purchases
""").fetchone()[0]

if direct_share and direct_share > MAX_DIRECT_SHARE:
    alerts.append(("WARN", f"Direct traffic unusually high: {direct_share:.2%}"))

# -----------------------
# Output report
# -----------------------
report = {
    "date": str(data_today),
    "alerts": [
        {"severity": s, "message": m}
        for s, m in alerts
    ],
    "status": "FAIL" if any(a[0] == "CRITICAL" for a in alerts) else "PASS"
}

with open(REPORT_OUT, "w") as f:
    json.dump(report, f, indent=2)

print("Monitoring complete")
print(json.dumps(report, indent=2))
