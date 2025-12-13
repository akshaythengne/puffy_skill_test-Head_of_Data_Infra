#!/usr/bin/env python3
"""
Part 3: Generate business analysis CSVs and charts from cleaned events parquet.

Usage:
  python part3_generate_analysis.py --parquet path/to/cleaned_events.parquet --outdir part3-outputs
"""

import os
import argparse
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams["figure.figsize"] = (8, 4)

OUT_DIR = r"part2-transformation\output"
parquet_path = os.path.join(OUT_DIR, "cleaned_events.parquet")
OUTPUT_DIR = r"part3-analysis\output"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
# ---------------------------
# SQL queries
# ---------------------------

SQL_CREATE_EVENTS = """
CREATE OR REPLACE TABLE events AS
SELECT * FROM read_parquet('{parquet_path}');
"""

SQL_ENRICHED = """
CREATE OR REPLACE VIEW events_enriched AS
SELECT
  *,
  -- ensure timestamp is TIMESTAMP
  CAST(timestamp_utc AS TIMESTAMP) AS timestamp_ts,
  DATE(CAST(timestamp_utc AS TIMESTAMP)) AS dt,
  COALESCE(utm_source, 'direct') AS utm_source_coalesced,
  COALESCE(utm_medium, 'direct') AS utm_medium_coalesced,
  COALESCE(utm_campaign, 'direct') AS utm_campaign_coalesced
FROM events;
"""

# 7-day first and last click attribution per purchase
SQL_PURCHASE_ATTRIBUTION = """
CREATE OR REPLACE VIEW purchase_attribution AS
WITH purchases AS (
  SELECT *, COALESCE(price, total, 0.0) AS revenue
  FROM events_enriched
  WHERE event_name = 'purchase'
)
SELECT
  p.*,
  (SELECT t.utm_source FROM events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_ts BETWEEN p.timestamp_ts - INTERVAL '7 days' AND p.timestamp_ts
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_ts ASC
     LIMIT 1) AS first_utm_source,
  (SELECT t.utm_medium FROM events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_ts BETWEEN p.timestamp_ts - INTERVAL '7 days' AND p.timestamp_ts
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_ts ASC
     LIMIT 1) AS first_utm_medium,
  (SELECT t.utm_campaign FROM events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_ts BETWEEN p.timestamp_ts - INTERVAL '7 days' AND p.timestamp_ts
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_ts ASC
     LIMIT 1) AS first_utm_campaign,
  (SELECT t.utm_source FROM events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_ts BETWEEN p.timestamp_ts - INTERVAL '7 days' AND p.timestamp_ts
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_ts DESC
     LIMIT 1) AS last_utm_source,
  (SELECT t.utm_medium FROM events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_ts BETWEEN p.timestamp_ts - INTERVAL '7 days' AND p.timestamp_ts
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_ts DESC
     LIMIT 1) AS last_utm_medium,
  (SELECT t.utm_campaign FROM events_enriched t
     WHERE t.client_id = p.client_id
       AND t.timestamp_ts BETWEEN p.timestamp_ts - INTERVAL '7 days' AND p.timestamp_ts
       AND t.utm_source IS NOT NULL
     ORDER BY t.timestamp_ts DESC
     LIMIT 1) AS last_utm_campaign
FROM purchases p;
"""

# Daily revenue and purchase counts
SQL_DAILY_REVENUE = """
SELECT dt AS date,
       COUNT(1) AS purchases,
       SUM(COALESCE(revenue,0)) AS revenue,
       AVG(COALESCE(revenue,0)) AS avg_order_value
FROM purchase_attribution
GROUP BY date
ORDER BY date;
"""

# Channel rollups (first and last)
SQL_CHANNEL_LAST = """
SELECT COALESCE(last_utm_source, 'direct') AS channel,
       COUNT(1) AS purchases,
       SUM(COALESCE(revenue,0)) AS revenue
FROM purchase_attribution
GROUP BY 1
ORDER BY revenue DESC;
"""

SQL_CHANNEL_FIRST = """
SELECT COALESCE(first_utm_source, 'direct') AS channel,
       COUNT(1) AS purchases,
       SUM(COALESCE(revenue,0)) AS revenue
FROM purchase_attribution
GROUP BY 1
ORDER BY revenue DESC;
"""

# Sessions: simple sessionization per client_id (30 min) to compute sessions and session counts by channel
SQL_SESSIONS_SIMPLE = """
CREATE OR REPLACE VIEW sessions_simple AS
WITH ordered AS (
  SELECT *,
    lag(timestamp_ts) OVER (PARTITION BY client_id ORDER BY timestamp_ts) AS prev_ts
  FROM events_enriched
  WHERE client_id IS NOT NULL
),
flags AS (
  SELECT *,
    CASE
      WHEN prev_ts IS NULL THEN 1
      WHEN datediff('second', prev_ts, timestamp_ts) > 1800 THEN 1
      ELSE 0
    END AS start_flag
  FROM ordered
),
seq AS (
  SELECT *,
    sum(start_flag) OVER (
      PARTITION BY client_id
      ORDER BY timestamp_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_seq
  FROM flags
),
session_bounds AS (
  SELECT
    client_id,
    session_seq,
    client_id || '_session_' || CAST(session_seq AS VARCHAR) AS session_id,
    min(timestamp_ts) AS session_start,
    max(timestamp_ts) AS session_end
  FROM seq
  GROUP BY client_id, session_seq
),
session_last_utm AS (
  SELECT
    sb.client_id,
    sb.session_seq,
    -- last non-null utm_source inside session
    max_by(e.utm_source, e.timestamp_ts) AS session_last_utm
  FROM session_bounds sb
  JOIN events_enriched e
    ON e.client_id = sb.client_id
   AND e.timestamp_ts BETWEEN sb.session_start AND sb.session_end
   AND e.utm_source IS NOT NULL
  GROUP BY sb.client_id, sb.session_seq
)
SELECT
  sb.client_id,
  sb.session_seq,
  sb.session_id,
  sb.session_start,
  sb.session_end,
  CAST(
    datediff('second', sb.session_start, sb.session_end) AS BIGINT
  ) AS session_duration_seconds,
  sl.session_last_utm
FROM session_bounds sb
LEFT JOIN session_last_utm sl
  ON sb.client_id = sl.client_id
 AND sb.session_seq = sl.session_seq;
"""

# Conversion rate by session's channel (last)
SQL_CONVERSION_BY_CHANNEL = """
WITH sessions AS (
  SELECT session_id, session_last_utm AS channel
  FROM sessions_simple
),
session_counts AS (
  SELECT COALESCE(channel,'direct') AS channel, COUNT(1) AS sessions
  FROM sessions
  GROUP BY 1
),
purchase_sessions AS (
  -- map purchase to the session where purchase happened (by client_id and timestamp)
  SELECT p.*, s.session_id, COALESCE(s.session_last_utm,'direct') AS channel
  FROM purchase_attribution p
  LEFT JOIN sessions_simple s
    ON p.client_id = s.client_id
   AND p.timestamp_ts BETWEEN s.session_start AND s.session_end
)
SELECT
  sc.channel,
  COALESCE(ps.purchases,0) AS purchases,
  sc.sessions,
  ROUND(COALESCE(ps.purchases,0) * 1.0 / NULLIF(sc.sessions,0), 6) AS conversion_rate
FROM session_counts sc
LEFT JOIN (
  SELECT channel AS channel, COUNT(1) AS purchases
  FROM purchase_sessions
  GROUP BY 1
) ps USING(channel)
ORDER BY conversion_rate DESC
LIMIT 200;
"""

# Revenue by device type (device parsing may have been done in earlier pipeline; if not, fallback)
SQL_REVENUE_BY_DEVICE = """
SELECT
  CASE
    WHEN lower(p.user_agent) LIKE '%ipad%' THEN 'tablet'
    WHEN lower(p.user_agent) LIKE '%iphone%' THEN 'mobile'
    WHEN lower(p.user_agent) LIKE '%android%' AND lower(p.user_agent) LIKE '%mobile%' THEN 'mobile'
    WHEN lower(p.user_agent) LIKE '%android%' THEN 'tablet'
    WHEN lower(p.user_agent) LIKE '%mobile%' THEN 'mobile'
    WHEN lower(p.user_agent) LIKE '%windows%' OR lower(p.user_agent) LIKE '%macintosh%' OR lower(p.user_agent) LIKE '%x11%' THEN 'desktop'
    ELSE 'unknown'
  END AS device_type,
  COUNT(DISTINCT p.client_id || '_' || COALESCE(CAST(p.dt AS VARCHAR), 'na')) AS sessions, -- heuristic
  SUM(COALESCE(revenue,0)) AS revenue,
  ROUND(SUM(COALESCE(revenue,0)) / NULLIF(COUNT(DISTINCT p.client_id || '_' || COALESCE(CAST(p.dt AS VARCHAR), 'na')),0),2) AS revenue_per_session
FROM purchase_attribution p
LEFT JOIN events_enriched e
  ON p.client_id = e.client_id AND p.timestamp_ts = e.timestamp_ts
GROUP BY device_type
ORDER BY revenue DESC;
"""

# Assisted vs direct conversions
SQL_ASSISTED_DIRECT = """
-- classify purchases into pure direct (first and last are direct), single-channel (same channel), assisted (first != last and neither is direct)
SELECT
  CASE
    WHEN (first_utm_source IS NULL OR first_utm_source = 'direct') AND (last_utm_source IS NULL OR last_utm_source = 'direct') THEN 'Pure Direct'
    WHEN first_utm_source = last_utm_source THEN 'Single Channel'
    ELSE 'Assisted Conversion'
  END AS conversion_type,
  COUNT(1) AS purchases,
  SUM(COALESCE(revenue,0)) AS revenue
FROM purchase_attribution
GROUP BY 1
ORDER BY purchases DESC;
"""

# Top products
SQL_TOP_PRODUCTS = """
SELECT COALESCE(product_id,'unknown') AS product_id,
       COUNT(1) AS purchases,
       SUM(COALESCE(revenue,0)) AS revenue
FROM purchase_attribution
GROUP BY 1
ORDER BY revenue DESC
LIMIT 50;
"""

# ---------------------------
# Helper to run query, save CSV and return DataFrame
# ---------------------------
def run_and_save(con, sql, out_csv, params=None):
    if params:
        sql = sql.format(**params)
    df = con.execute(sql).fetchdf()
    df.to_csv(out_csv, index=False, encoding='utf-8')
    print(f"Wrote {out_csv} (rows={len(df)})")
    return df

# ---------------------------
# Main
# ---------------------------
def main(parquet_path, outdir):
    os.makedirs(outdir, exist_ok=True)
    con = duckdb.connect(database=':memory:')

    # load parquet into duckdb table
    con.execute(SQL_CREATE_EVENTS.format(parquet_path=parquet_path))
    con.execute(SQL_ENRICHED)
    con.execute(SQL_PURCHASE_ATTRIBUTION)
    # basic validations
    daily_rev = run_and_save(con, SQL_DAILY_REVENUE, os.path.join(outdir, 'daily_revenue.csv'))
    ch_last = run_and_save(con, SQL_CHANNEL_LAST, os.path.join(outdir, 'channel_revenue_last_click.csv'))
    ch_first = run_and_save(con, SQL_CHANNEL_FIRST, os.path.join(outdir, 'channel_revenue_first_click.csv'))

    # sessions + conversions
    con.execute(SQL_SESSIONS_SIMPLE)
    conv_by_channel = run_and_save(con, SQL_CONVERSION_BY_CHANNEL, os.path.join(outdir, 'conversion_rate_by_channel.csv'))

    # revenue by device, assisted direct, top products
    revenue_by_device = run_and_save(con, SQL_REVENUE_BY_DEVICE, os.path.join(outdir, 'revenue_by_device.csv'))
    assisted = run_and_save(con, SQL_ASSISTED_DIRECT, os.path.join(outdir, 'assisted_vs_direct.csv'))
    top_products = run_and_save(con, SQL_TOP_PRODUCTS, os.path.join(outdir, 'top_products.csv'))

    # ---------------------------
    # Charts (matplotlib)
    # ---------------------------

    # 1) Revenue trend
    if not daily_rev.empty:
        plt.clf()
        plt.plot(pd.to_datetime(daily_rev['date']), daily_rev['revenue'], marker='o')
        plt.title('Daily Revenue')
        plt.xlabel('Date')
        plt.ylabel('Revenue')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        fn = os.path.join(outdir, 'chart_daily_revenue.png')
        plt.savefig(fn, dpi=150)
        print("Saved", fn)

    # 2) Top channels (last-click) bar chart - top 10
    if not ch_last.empty:
        top = ch_last.head(10)
        plt.clf()
        plt.bar(top['channel'].astype(str), top['revenue'].astype(float))
        plt.title('Top 10 Channels by Last-Click Revenue')
        plt.xlabel('Channel')
        plt.ylabel('Revenue')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        fn = os.path.join(outdir, 'chart_top_channels_last.png')
        plt.savefig(fn, dpi=150)
        print("Saved", fn)

    # 3) Revenue by device
    if not revenue_by_device.empty:
        plt.clf()
        plt.bar(revenue_by_device['device_type'].astype(str), revenue_by_device['revenue'].astype(float))
        plt.title('Revenue by Device Type')
        plt.xlabel('Device Type')
        plt.ylabel('Revenue')
        plt.tight_layout()
        fn = os.path.join(outdir, 'chart_revenue_by_device.png')
        plt.savefig(fn, dpi=150)
        print("Saved", fn)

    # 4) Conversion rate by channel (scatter or bar)
    if not conv_by_channel.empty:
        plt.clf()
        dfc = conv_by_channel.sort_values('conversion_rate', ascending=False).head(15)
        plt.bar(dfc['channel'].astype(str), dfc['conversion_rate'].astype(float))
        plt.title('Top Conversion Rates by Channel (Last-Click by Session)')
        plt.xlabel('Channel')
        plt.ylabel('Conversion rate')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        fn = os.path.join(outdir, 'chart_conversion_by_channel.png')
        plt.savefig(fn, dpi=150)
        print("Saved", fn)

    # 5) Assisted vs direct pie chart
    if not assisted.empty:
        plt.clf()
        labels = assisted['conversion_type'].astype(str)
        sizes = assisted['purchases'].astype(int)
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.axis('equal')
        plt.title('Assisted vs Direct Conversions')
        fn = os.path.join(outdir, 'chart_assisted_vs_direct.png')
        plt.savefig(fn, dpi=150)
        print("Saved", fn)

    print("All outputs saved to", outdir)
    con.close()

if __name__ == '__main__':
    
    main(parquet_path, OUTPUT_DIR)
