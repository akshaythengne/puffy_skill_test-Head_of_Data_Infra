import os
import glob
import json
import re
from urllib.parse import urlparse, parse_qs

import pandas as pd
import numpy as np


# ============================================================
# CONFIG
# ============================================================

DATA_GLOB = "data/events_2025*.csv"    
OUT_DIR = "part1-data-quality"
os.makedirs(OUT_DIR, exist_ok=True)


# ============================================================
# LOAD DATA
# ============================================================

paths = sorted(glob.glob(DATA_GLOB))
if not paths:
    raise FileNotFoundError("No event files found.")

dfs = []
file_column_map = {}

for p in paths:
    df = pd.read_csv(p, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    df["source_file"] = os.path.basename(p)

    # Save the columns seen in this file (used later for per-file schema checks)
    cols = [c.strip().lower() for c in df.columns]
    file_column_map[os.path.basename(p)] = cols
    dfs.append(df)

events = pd.concat(dfs, ignore_index=True)
events.columns = [c.strip().lower() for c in events.columns]


# ============================================================
# NORMALIZE COLUMNS → CANONICAL SCHEMA
# ============================================================

def normalize_name(c):
    return re.sub(r"[^a-z0-9]", "", c.lower())

col_map = {normalize_name(c): c for c in events.columns}

expected = {
    "clientid": "client_id",
    "pageurl": "page_url",
    "referrer": "referrer",
    "timestamp": "timestamp",
    "eventname": "event_name",
    "eventdata": "event_data",
    "useragent": "user_agent",
}

canon_map = {}
for norm, canon in expected.items():
    canon_map[canon] = col_map.get(norm)

# Create missing columns
for canon, existing in canon_map.items():
    if existing is None:
        events[canon] = None
    else:
        events[canon] = events[existing]


# ============================================================
# PARSE TIMESTAMP
# ============================================================

events["timestamp_parsed"] = pd.to_datetime(
    events["timestamp"], utc=True, errors="coerce"
)


# ============================================================
# PARSE event_data JSON SAFELY
# ============================================================

def safe_json(s):
    if pd.isna(s) or s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None

    # Try strict JSON
    try:
        return json.loads(s)
    except:
        pass

    # Try repair: convert single quotes → double quotes
    try:
        return json.loads(s.replace("'", '"'))
    except:
        return None

events["event_data_parsed"] = events["event_data"].apply(safe_json)


# ============================================================
# EXTRACT UTM PARAMETERS
# ============================================================

def extract_utms(url):
    try:
        qs = parse_qs(urlparse(str(url)).query)
        return (
            qs.get("utm_source", [None])[0],
            qs.get("utm_medium", [None])[0],
            qs.get("utm_campaign", [None])[0],
        )
    except:
        return None, None, None

events[["utm_source", "utm_medium", "utm_campaign"]] = (
    events["page_url"].fillna("").apply(lambda u: pd.Series(extract_utms(u)))
)


# ============================================================
# DQ CHECKS
# ============================================================

checks = []

# ------------------------------------------------------------
# 0. STRICT PER-FILE SCHEMA VALIDATION
# ------------------------------------------------------------
required_normalized = ["clientid", "pageurl", "referrer", "timestamp", "eventname", "eventdata", "useragent"]


# Per-file missing columns (using normalized names)
file_norm_map = {fname: set(normalize_name(c) for c in cols)
                 for fname, cols in file_column_map.items()}

per_file_missing = {
    fname: [req for req in required_normalized if req not in normcols]
    for fname, normcols in file_norm_map.items()
}

# Severity logic
severe_required = {"timestamp", "eventname", "eventdata", "clientid", "pageurl"}

has_severe_missing = any(
    any(req in severe_required for req in missing_cols)
    for missing_cols in per_file_missing.values()
)


if has_severe_missing:
    schema_status = "fail"
    # Show only bad files
    details = {
        fname: missing 
        for fname, missing in per_file_missing.items() 
        if missing
    }
elif any(per_file_missing.values()):
    schema_status = "warn"
    details = {
        fname: missing 
        for fname, missing in per_file_missing.items() 
        if missing
    }
else:
    schema_status = "pass"
    details = {}   # Do NOT show full schema mapping when pass

checks.append({
    "check": "schema_per_file",
    "status": schema_status,
    "details": details
})

# 1. Row Counts
rows_per_file = events.groupby("source_file").size().reset_index(name="rows")

mean_rows = rows_per_file["rows"].mean()
zero_row_files = rows_per_file[rows_per_file["rows"] == 0]
anomalous_files = rows_per_file[
    abs(rows_per_file["rows"] - mean_rows) > 0.5 * mean_rows
]

if len(zero_row_files) > 0:
    rowc_status = "fail"
    details = zero_row_files.to_dict(orient='records')
elif len(anomalous_files) > 0:
    rowc_status = "warn"
    details = anomalous_files.to_dict(orient='records')
else:
    rowc_status = "pass"
    details = {}  # No detail when everything is normal

checks.append({
    "check": "row_counts",
    "status": rowc_status,
    "details": details
})

# 2. Global Schema Completeness
missing_cols = [k for k, v in canon_map.items() if v is None]
checks.append({"check": "schema", "status": "fail" if missing_cols else "pass",
               "details": f"missing={missing_cols}"})

# 3. Timestamp Validity
bad_ts = events["timestamp_parsed"].isna().sum()
checks.append({"check": "timestamp", "status": "fail" if bad_ts > 0 else "pass",
               "details": f"bad_timestamps={bad_ts}"})

# 4. Event Name Validity
allowed_events = {
    "page_viewed",
    "email_filled_on_popup",
    "product_added_to_cart",
    "checkout_started",
    "purchase",
}

invalid_event_names = [
    e for e in events["event_name"].dropna().unique()
    if e not in allowed_events
]

checks.append({"check": "event_taxonomy",
               "status": "fail" if invalid_event_names else "pass",
               "details": str(invalid_event_names)})

# 5. JSON Parse Errors
events["json_error"] = events["event_data"].notna() & events["event_data_parsed"].isna()
checks.append({"check": "json_parse", "status": "fail" if events['json_error'].sum() else "pass"})

# 6. Duplicate Events
dup_cols = ["source_file", "timestamp", "event_name", "event_data"]
dup_count = events.duplicated(subset=dup_cols).sum()
checks.append({"check": "duplicates", "status": "fail" if dup_count else "pass",
               "details": dup_count})

# 7. Client ID Completeness
null_client_ids = events["client_id"].isna().sum()
checks.append({"check": "client_id_nulls", "status": "warn" if null_client_ids else "pass",
               "details": null_client_ids})

# 8. Referrer Anonymization Heuristic
def is_anonymized(dom):
    if dom is None:
        return False
    return len(dom) > 30 or bool(re.search(r"[0-9a-f]{12,}", dom))

events["referrer_domain"] = events["referrer"].apply(
    lambda r: urlparse(str(r)).netloc.lower() if isinstance(r, str) else None
)
events["referrer_anonymized"] = events["referrer_domain"].apply(is_anonymized)

anon_rate = events["referrer_anonymized"].mean()

if anon_rate > 0.20:
    anon_status = "fail"
elif anon_rate > 0.05:
    anon_status = "warn"
else:
    anon_status = "pass"

checks.append({
    "check": "referrer_anonymization",
    "status": anon_status,
    "details": f"anonymized_rate={anon_rate:.3f}"
})

# 9. UTM Coverage
utm_rows = events[["utm_source", "utm_medium", "utm_campaign"]].notna().any(axis=1).sum()
utm_rate = utm_rows / len(events)

if utm_rate < 0.05:
    utm_status = "fail"
elif utm_rate < 0.15:
    utm_status = "warn"
else:
    utm_status = "pass"

checks.append({
    "check": "utm_coverage",
    "status": utm_status,
    "details": f"utm_rate={utm_rate:.3f}"
})

# 10. Purchase Price Validity
purchase = events[events["event_name"] == "purchase"].copy()
def extract_price(ed):
    if isinstance(ed, dict):
        for k in ["price", "total", "revenue", "amount", "value"]:
            if k in ed:
                try: return float(ed[k])
                except: return None
    return None

purchase["price"] = purchase["event_data_parsed"].apply(extract_price)

price_missing = purchase["price"].isna().sum()
price_zero_neg = (purchase["price"].fillna(0) <= 0).sum()

checks.append({"check": "purchase_price_validation",
               "status": "fail" if price_missing or price_zero_neg else "pass",
               "details": f"missing={price_missing}, zero_or_negative={price_zero_neg}"})

# ============================================================
# WRITE DETECTED ISSUE SAMPLE
# ============================================================

issues = events[
    (events["json_error"]) |
    (events.duplicated(subset=dup_cols)) |
    (events["event_name"].isin(invalid_event_names)) |
    (events["client_id"].isna())
]

issues.head(200).to_csv(f"{OUT_DIR}/detected_issues_sample.csv", index=False)


# ============================================================
# GENERATE MARKDOWN REPORT
# ============================================================

report = ["# Incoming Data Quality Report\n"]

report.append("## Summary of Checks\n")
for c in checks:
    report.append(f"- **{c['check']}** → *{c['status']}* — {c.get('details', '')}")

# report.append("\n## Key Findings\n")
# report.append(f"- Invalid event names found: {invalid_event_names}")
# report.append(f"- Duplicate events: {dup_count}")
# report.append(f"- Null client IDs: {null_client_ids}")
# report.append(f"- Referrer anonymization count: {events['referrer_anonymized'].sum()}")
# report.append(f"- Purchase price missing: {price_missing}")
# report.append(f"- Purchase price ≤ 0: {price_zero_neg}")

# report.append("\n## Recommended Ingestion Rules\n")
# report.append("- Fail the batch if purchase events contain missing or non-positive price.")
# report.append("- Trigger an alert if JSON parse errors exceed 1% of events.")
# report.append("- Trigger an alert if daily event volume deviates >3σ from baseline.")
# report.append("- Reject rows with unknown event_name.")
# report.append("- Reject events missing timestamp or with unparseable timestamp.")
# report.append("- Require `client_id` except for controlled exemptions.")
# report.append("- Track duplicate row rate and block repeated identical events.")

with open(f"{OUT_DIR}/dq_report.md", "w", encoding="utf-8") as f:
    f.write("\n".join(report))

print("DQ framework executed successfully.")
print(f"Outputs saved to: {OUT_DIR}")
