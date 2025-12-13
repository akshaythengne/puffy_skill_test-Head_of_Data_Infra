# clean_and_analyze.py
import os
import glob
import json
import re
from urllib.parse import urlparse, parse_qs
from datetime import timezone
import duckdb
import pandas as pd
import numpy as np

# -----------------------
# Config - edit if needed
# -----------------------
DATA_GLOB = "data/events_2025*.csv"   # same pattern as your validation code
OUT_DIR = r"part2-transformation\output"
CLEANED_PARQUET = os.path.join(OUT_DIR, "cleaned_events.parquet")
os.makedirs(OUT_DIR, exist_ok=True)

# Conservative remap for purchase-like taxonomy issues:
EVENT_NAME_REMAP = {
    "checkout_completed": "purchase"
}

# Duplicate drop subset
DUP_SUBSET = ["source_file", "timestamp", "event_name", "event_data", "page_url", "referrer", "client_id"]

# Session gap (seconds)
SESSION_GAP_SECONDS = 1800  # 30 minutes

# -----------------------
# Helpers
# -----------------------
def normalize_name(c):
    return re.sub(r"[^a-z0-9]", "", c.strip().lower()) if c is not None else ""

def safe_json_parse(s):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        return json.loads(s)
    except Exception:
        # try single-quote repair
        try:
            return json.loads(s.replace("'", '"'))
        except Exception:
            return None

def extract_utms_from_url(url):
    try:
        qs = parse_qs(urlparse(str(url)).query)
        return qs.get("utm_source", [None])[0], qs.get("utm_medium", [None])[0], qs.get("utm_campaign", [None])[0]
    except Exception:
        return None, None, None

def extract_domain(url):
    try:
        domain = urlparse(str(url)).netloc.lower()
        return domain if domain != "" else None
    except Exception:
        return None

def extract_price(event_data_parsed):
    if isinstance(event_data_parsed, dict):
        for k in ("price","total","revenue","amount","value"):
            if k in event_data_parsed:
                try:
                    return float(event_data_parsed[k])
                except Exception:
                    return None
    return None

def extract_from_items(ed, key):
    """
    Extract a key from event_data that may be nested under items[].
    Returns:
      - sum for numeric fields (quantity, total)
      - first non-null value for identifiers (product_id)
    """
    if not isinstance(ed, dict):
        return None

    items = ed.get("items")
    if not isinstance(items, list) or len(items) == 0:
        return None

    values = []
    for item in items:
        if isinstance(item, dict) and key in item:
            values.append(item.get(key))

    if not values:
        return None

    # quantity / price / total → numeric sum
    if key in ("quantity", "price", "total"):
        try:
            return sum(float(v) for v in values if v is not None)
        except Exception:
            return None

    # product_id / sku → first value
    return values[0]


# -----------------------
# Step 1: Load & combine CSVs
# -----------------------
paths = sorted(glob.glob(DATA_GLOB))
if not paths:
    raise FileNotFoundError(f"No files found for glob: {DATA_GLOB}")

dfs = []
file_column_map = {}
for p in paths:
    df = pd.read_csv(p, dtype=str)
    # normalize column names (lowercase)
    df.columns = [c.strip().lower() for c in df.columns]
    df["source_file"] = os.path.basename(p)
    cols = [c.strip().lower() for c in df.columns]
    file_column_map[os.path.basename(p)] = cols
    dfs.append(df)

events = pd.concat(dfs, ignore_index=True)
events.columns = [c.strip().lower() for c in events.columns]

# Canonical mapping (same as your validation)
expected = {
    "clientid": "client_id",
    "pageurl": "page_url",
    "referrer": "referrer",
    "timestamp": "timestamp",
    "eventname": "event_name",
    "eventdata": "event_data",
    "useragent": "user_agent",
}
col_map = {normalize_name(c): c for c in events.columns}
canon_map = {v: col_map.get(k) for k, v in expected.items()}

for canon, orig in canon_map.items():
    if orig is None:
        events[canon] = None
    else:
        events[canon] = events[orig]

# Ensure referrer column exists (may be missing in some files)
if "referrer" not in events.columns:
    events["referrer"] = None

# -----------------------
# Step 2: Clean transforms in pandas
# -----------------------

# 2.1 Fix known taxonomy issues (conservative remap)
events["event_name"] = events["event_name"].fillna("").astype(str).str.strip()
events["event_name"] = events["event_name"].apply(lambda v: EVENT_NAME_REMAP.get(v, v) if v != "" else None)

# 2.2 Add flags for missing referrer at file-level and row-level
# files missing referrer:
files_missing_referrer = {f for f, cols in file_column_map.items() if "referrer" not in cols}
events["source_file_referrer_missing"] = events["source_file"].apply(lambda f: f in files_missing_referrer)
events["referrer_missing"] = events["referrer"].isna()

# 2.3 Drop exact duplicates (keep first)
# use only columns that exist in df from DUP_SUBSET
dup_subset_existing = [c for c in DUP_SUBSET if c in events.columns]
before_count = len(events)
events = events.drop_duplicates(subset=dup_subset_existing, keep="first")
after_count = len(events)
dropped_dupes = before_count - after_count

# 2.4 Parse timestamp to pandas datetime (UTC)
events["timestamp_utc"] = pd.to_datetime(events["timestamp"], utc=True, errors="coerce")

# 2.5 Parse event_data JSON and extract price/total/quantity/product_id
events["event_data_parsed"] = events["event_data"].apply(safe_json_parse)
events["price"] = events["event_data_parsed"].apply(extract_price)
# extract total & quantity if present
def get_key(ed, key):
    if isinstance(ed, dict) and key in ed:
        return ed[key]
    return None


events["quantity"] = events["event_data_parsed"].apply(
    lambda ed: get_key(ed, "quantity") or extract_from_items(ed, "quantity")
)

events["product_id"] = events["event_data_parsed"].apply(
    lambda ed:
        get_key(ed, "product_id")
        or get_key(ed, "item_id")
        or get_key(ed, "sku")
        or extract_from_items(ed, "product_id")
        or extract_from_items(ed, "sku")
        or extract_from_items(ed, "item_id")
)

# derive total safely
events["unit_price"] = events["event_data_parsed"].apply(
    lambda ed: extract_price(ed) or extract_from_items(ed, "price")
)

events["total"] = (
    pd.to_numeric(events["unit_price"], errors="coerce") *
    pd.to_numeric(events["quantity"], errors="coerce")
)


# 2.6 Extract UTMs and referrer domain
events[["utm_source", "utm_medium", "utm_campaign"]] = events["page_url"].fillna("").apply(lambda u: pd.Series(extract_utms_from_url(u)))
events["referrer_domain"] = events["referrer"].apply(extract_domain)

# 2.7 Mark missing client_id (we do not drop by default)
events["client_id_missing"] = events["client_id"].isna()

# 2.8 Final normalization: cast types
# price numeric
events["price"] = pd.to_numeric(events["price"], errors="coerce")
events["total"] = pd.to_numeric(events["total"], errors="coerce")
events["quantity"] = pd.to_numeric(events["quantity"], errors="coerce")

# Save cleaned dataset to parquet
#events.to_csv(os.path.join(OUT_DIR, "cleaned_events.csv"), index=False)
events.to_parquet(CLEANED_PARQUET, index=False)
print(f"Saved cleaned parquet to: {CLEANED_PARQUET}")
#print(f"Dropped duplicates: {dropped_dupes}")

# -----------------------
# Step 3: DuckDB - register table and run SQL transforms/views
# -----------------------
con = duckdb.connect(database=':memory:')
# register parquet as a view/table
con.execute(f"CREATE TABLE events AS SELECT * FROM read_parquet('{CLEANED_PARQUET}');")

# 3.1 Create enriched view equivalent (we already exploded many fields in pandas)
con.execute("""
CREATE OR REPLACE VIEW analytics_events_enriched AS
SELECT
  source_file,
  client_id,
  page_url,
  referrer,
  timestamp_utc,
  event_name,
  event_data,
  event_data_parsed,
  user_agent,
  utm_source, 
  utm_medium,
  utm_campaign,
  price, 
  total, 
  quantity,
  product_id,
  referrer_domain,
  client_id_missing,
  source_file_referrer_missing,
  referrer_missing
FROM events;
""")

# 3.2 Sessionization (only for rows WITH client_id)
# We'll create a sessions table/view using 30-min inactivity gap.
con.execute(f"""
CREATE OR REPLACE VIEW analytics_sessions AS
WITH with_lag AS (
  SELECT
    *,
    lag(timestamp_utc) OVER (PARTITION BY client_id ORDER BY timestamp_utc) AS prev_ts
  FROM analytics_events_enriched
  WHERE client_id IS NOT NULL
),
flagged AS (
  SELECT
    *,
    CASE
      WHEN prev_ts IS NULL THEN 1
      WHEN datediff('second', prev_ts, timestamp_utc) > {SESSION_GAP_SECONDS} THEN 1
      ELSE 0
    END AS new_session_flag
  FROM with_lag
),
sequenced AS (
  SELECT
    *,
    sum(new_session_flag) OVER (PARTITION BY client_id ORDER BY timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_seq
  FROM flagged
)
SELECT
  client_id,
  session_seq,
  client_id || '_session_' || CAST(session_seq AS VARCHAR) AS session_id,
  min(timestamp_utc) OVER (PARTITION BY client_id, session_seq) AS session_start,
  max(timestamp_utc) OVER (PARTITION BY client_id, session_seq) AS session_end,
  CAST(
  datediff(
    'second',
    min(timestamp_utc) OVER (PARTITION BY client_id, session_seq),
    max(timestamp_utc) OVER (PARTITION BY client_id, session_seq)
  ) AS BIGINT
) AS session_duration_seconds,
  array_agg(
  struct_pack(
    event_name := event_name,
    timestamp_utc := timestamp_utc,
    page_url := page_url
  )
  ORDER BY timestamp_utc
) OVER (PARTITION BY client_id, session_seq) AS events_in_session
FROM sequenced;
""")

# 3.3 Users view
con.execute("""
CREATE OR REPLACE VIEW analytics_users AS
SELECT
  client_id,
  min(timestamp_utc) AS first_seen,
  max(timestamp_utc) AS last_seen,
  count(distinct client_id || '_session_' || session_seq) AS session_count,
  ARRAY_AGG(DISTINCT user_agent) FILTER (WHERE user_agent IS NOT NULL) AS sample_user_agent
FROM (
  SELECT e.client_id, e.user_agent, s.session_seq, e.timestamp_utc
  FROM analytics_events_enriched e
  LEFT JOIN analytics_sessions s USING(client_id)
)
WHERE client_id IS NOT NULL
GROUP BY client_id;
""")

# 3.4 Device view
con.execute("""
CREATE OR REPLACE VIEW analytics_events_with_device AS
SELECT
  *,
  /* -------------------------
     DEVICE TYPE
     ------------------------- */
  CASE
    WHEN lower(user_agent) LIKE '%ipad%' THEN 'tablet'
    WHEN lower(user_agent) LIKE '%iphone%' THEN 'mobile'
    WHEN lower(user_agent) LIKE '%android%' AND lower(user_agent) LIKE '%mobile%' THEN 'mobile'
    WHEN lower(user_agent) LIKE '%android%' THEN 'tablet'
    WHEN lower(user_agent) LIKE '%mobile%' THEN 'mobile'
    WHEN lower(user_agent) LIKE '%windows%' OR lower(user_agent) LIKE '%macintosh%' OR lower(user_agent) LIKE '%x11%' THEN 'desktop'
    ELSE 'unknown'
  END AS device_type,

  /* -------------------------
     OPERATING SYSTEM
     ------------------------- */
  CASE
    WHEN lower(user_agent) LIKE '%iphone%' OR lower(user_agent) LIKE '%ipad%' THEN 'iOS'
    WHEN lower(user_agent) LIKE '%android%' THEN 'Android'
    WHEN lower(user_agent) LIKE '%windows nt%' THEN 'Windows'
    WHEN lower(user_agent) LIKE '%mac os x%' AND lower(user_agent) NOT LIKE '%iphone%' THEN 'MacOS'
    WHEN lower(user_agent) LIKE '%linux%' THEN 'Linux'
    ELSE 'Other'
  END AS os,

  /* -------------------------
     BROWSER
     ------------------------- */
  CASE
    -- Order matters here
    WHEN lower(user_agent) LIKE '%crios%' THEN 'Chrome (iOS)'
    WHEN lower(user_agent) LIKE '%fxios%' THEN 'Firefox (iOS)'
    WHEN lower(user_agent) LIKE '%edgios%' THEN 'Edge (iOS)'
    WHEN lower(user_agent) LIKE '%chrome%' AND lower(user_agent) NOT LIKE '%edg%' THEN 'Chrome'
    WHEN lower(user_agent) LIKE '%safari%' AND lower(user_agent) NOT LIKE '%chrome%' THEN 'Safari'
    WHEN lower(user_agent) LIKE '%firefox%' THEN 'Firefox'
    WHEN lower(user_agent) LIKE '%edg%' THEN 'Edge'
    ELSE 'Other'
  END AS browser

FROM analytics_events_enriched;

            
            """)

# 3.5 Attribution per purchase (first & last click within 7-day window)
# Note: DuckDB supports correlated subqueries; using them conservatively.
con.execute("""
CREATE OR REPLACE VIEW analytics_purchase_attribution AS
WITH purchases AS (
  SELECT *, COALESCE(total, 0.0) AS revenue
  FROM analytics_events_enriched
  WHERE event_name = 'purchase'
)
SELECT
  p.*,
  -- first touch in 7-day window (with utm)
  (SELECT t.utm_source FROM analytics_events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_utc BETWEEN p.timestamp_utc - INTERVAL '7 days' AND p.timestamp_utc
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_utc ASC
     LIMIT 1) AS first_utm_source,
  (SELECT t.utm_medium FROM analytics_events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_utc BETWEEN p.timestamp_utc - INTERVAL '7 days' AND p.timestamp_utc
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_utc ASC
     LIMIT 1) AS first_utm_medium,
  (SELECT t.utm_campaign FROM analytics_events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_utc BETWEEN p.timestamp_utc - INTERVAL '7 days' AND p.timestamp_utc
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_utc ASC
     LIMIT 1) AS first_utm_campaign,
  (SELECT t.utm_source FROM analytics_events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_utc BETWEEN p.timestamp_utc - INTERVAL '7 days' AND p.timestamp_utc
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_utc DESC
     LIMIT 1) AS last_utm_source,
  (SELECT t.utm_medium FROM analytics_events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_utc BETWEEN p.timestamp_utc - INTERVAL '7 days' AND p.timestamp_utc
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_utc DESC
     LIMIT 1) AS last_utm_medium,
  (SELECT t.utm_campaign FROM analytics_events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_utc BETWEEN p.timestamp_utc - INTERVAL '7 days' AND p.timestamp_utc
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_utc DESC
     LIMIT 1) AS last_utm_campaign
FROM purchases p;
""")

# 3.6 Channel-level rollups
con.execute("""
CREATE OR REPLACE VIEW analytics_channel_revenue_last_click AS
SELECT
  COALESCE(last_utm_source, 'direct') AS channel,
  SUM(revenue) AS revenue,
  COUNT(*) AS purchases
FROM analytics_purchase_attribution
GROUP BY 1
ORDER BY revenue DESC;
""")

con.execute("""
CREATE OR REPLACE VIEW analytics_channel_revenue_first_click AS
SELECT
  COALESCE(first_utm_source, 'direct') AS channel,
  SUM(revenue) AS revenue,
  COUNT(*) AS purchases
FROM analytics_purchase_attribution
GROUP BY 1
ORDER BY revenue DESC;
""")

# -----------------------
# Step 4: Validation checks (reconciliations)
# -----------------------
print("\n=== VALIDATION: Basic reconciliation checks ===")

# Raw purchases revenue from cleaned table
raw_revenue = con.execute("SELECT COALESCE(SUM(COALESCE(total, 0.0)),0) AS raw_revenue FROM analytics_events_enriched WHERE event_name='purchase'").fetchdf()
attrib_revenue = con.execute("SELECT COALESCE(SUM(revenue),0) AS attributed_revenue FROM analytics_purchase_attribution").fetchdf()

print("Raw revenue (from enriched events):", raw_revenue['raw_revenue'].iloc[0])
print("Attributed revenue (from attribution view):", attrib_revenue['attributed_revenue'].iloc[0])

# Duplicate count recorded earlier (dropped_dupes)
print("Dropped duplicates (pandas stage):", dropped_dupes)

# Examples of queries to answer marketing questions:
print("\n=== EXAMPLES for Marketing ===")
print("1) Top channels by last-click revenue:")
print(con.execute("SELECT * FROM analytics_channel_revenue_last_click LIMIT 20").fetchdf())

print("\n2) Top channels by first-click revenue:")
print(con.execute("SELECT * FROM analytics_channel_revenue_first_click LIMIT 20").fetchdf())

print("\n3) Session distribution (sample):")
print(con.execute("SELECT session_id, session_start, session_end, session_duration_seconds FROM analytics_sessions LIMIT 10").fetchdf())

print("\n4) Conversion Rate by Channel (Last-Click) (sample):")
print(con.execute("""SELECT
  last_utm_source AS channel,
  COUNT(*) AS purchases,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(*) * 1.0 / COUNT(DISTINCT session_id) AS conversion_rate
FROM analytics_purchase_attribution p
JOIN analytics_sessions s
  ON p.client_id = s.client_id
GROUP BY channel
ORDER BY conversion_rate DESC;
""").fetchdf())

print("\n5) Revenue per Session by Device (sample):")
print(con.execute("""SELECT
  device_type,
  COUNT(DISTINCT session_id) AS sessions,
  SUM(p.total) AS revenue,
  SUM(p.total) / COUNT(DISTINCT session_id) AS revenue_per_session
FROM analytics_events_with_device p
JOIN analytics_sessions s
  ON p.client_id = s.client_id
GROUP BY device_type
ORDER BY revenue_per_session DESC;
""").fetchdf())

print("\n6) Assisted vs Direct Conversion Indicator (sample):")
print(con.execute("""SELECT
  CASE
    WHEN first_utm_source IS NULL THEN 'Pure Direct'
    WHEN first_utm_source = last_utm_source THEN 'Single Channel'
    ELSE 'Assisted Conversion'
  END AS conversion_type,
  COUNT(*) AS purchases,
  SUM(revenue) AS revenue
FROM analytics_purchase_attribution
GROUP BY conversion_type;
""").fetchdf())


# Save DuckDB in-memory views to parquet for downstream BI if needed
con.execute(f"COPY (SELECT * FROM analytics_events_enriched) TO '{OUT_DIR}/events_enriched.parquet' (FORMAT PARQUET);")
con.execute(f"COPY (SELECT * FROM analytics_purchase_attribution) TO '{OUT_DIR}/purchase_attribution.parquet' (FORMAT PARQUET);")

print("\nOutputs written to:", OUT_DIR)
print("Cleaned parquet:", CLEANED_PARQUET)
