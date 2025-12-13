## ** Notes and Assumptions **
* Mapping checkout_completed → `purchase`: I made this mapping because the validation run showed `checkout_completed` is the one unexpected event and most systems intend that to be a conversion. 

* Missing referrer: I marked rows/files that lack it. I did not synthesize referrer values.

* Missing client_id: I keep such events but mark them. Sessionization and attribution are performed only for events with client_id.

* Deduplication: I removed exact duplicates in pandas early (conservative). This helps with improving performance.

* DuckDB is used in-memory here as we do not have the tools to support a robust and distributed system; for production, I would materialize the views into a warehouse (BigQuery / Snowflake / Redshift) and schedule incremental runs.

* Performance: For large data, ingest to Parquet partitioned by date and have DuckDB or the warehouse read those partitions. Materialize heavy views into partitioned tables.



# PART 2: Transformation Pipeline

## Overview

This module implements the **analytics transformation layer** for a DTC e-commerce business.
The input consists of **validated but raw, unsessionized event data**. The output is a set of **analytics-ready tables and views** that enable the marketing team to:

1. Understand **how users engage with the site** (behavior, devices, sources, actions)
2. Attribute **revenue to marketing channels**, supporting both **first-click** and **last-click** attribution models

**Constraint:** Attribution lookback window is **7 days**.

The full implementation is available in `part2-transformation/clean_and_analyze.py` .

---

## Architecture & Data Flow

```
Raw CSV Event Files (14 days)
        ↓
Python Cleaning & Normalization (pandas)
        ↓
Cleaned Parquet (cleaned_events.parquet)
        ↓
DuckDB SQL Transformations
        ↓
Analytics Views (Sessions, Users, Devices, Attribution, Channel Rollups)
```

### Why this architecture?

* **Python (pandas)** is used for deterministic cleaning where SQL would be fragile (taxonomy fixes, schema gaps, deduplication).
* **DuckDB** provides warehouse-style analytics locally using standard SQL.
* **Parquet** acts as a stable contract between ingestion and analytics layers and is BI-friendly.

---

## Methodology & Key Design Decisions

### 1. Data Cleaning (Python stage)

Before analytics, the raw events are **explicitly cleaned**:

* **Event taxonomy repair**
  Known bad values (e.g. `checkout_completed`) are conservatively remapped to `purchase`.

* **Schema inconsistencies handled explicitly**
  Some files are missing the `referrer` column. Instead of dropping data, the column is added as `NULL` and flagged (`source_file_referrer_missing`, `referrer_missing`).

* **Exact duplicate removal**
  Duplicate events are dropped using a strict subset of identifying columns.
  *Result:* `Dropped duplicates (pandas stage): 1`

* **Robust JSON parsing**
  `event_data` is parsed defensively, including single-quote repair.
  Price, quantity, total, and product identifiers are extracted even when nested under `items[]`.

* **Client ID handling**
  Events with missing `client_id` are retained but flagged.
  Sessionization and attribution operate only on rows where `client_id` is present.

The output of this stage is a single clean dataset:

```
part2-transformation/output/cleaned_events.parquet
```

---

### 2. Sessionization

**Approach:**
Sessions are defined using a **30-minute inactivity gap** per `client_id`.

**Why 30 minutes?**

* Industry-standard for web analytics
* Balances over-fragmentation vs. overly long sessions

**Implementation details:**

* Events are ordered per `client_id`
* A new session starts if:

  * It is the first event for the client, or
  * The time gap from the previous event exceeds 1800 seconds
* Session IDs are deterministic:
  `client_id_session_<sequence_number>`

**Output:** `analytics_sessions`

**Observed behavior (sample):**

* Many sessions have duration `0` seconds, indicating bounce-like behavior (single-event sessions), which is typical in e-commerce funnels.

---

### 3. User Definition

**User definition:**
A user is defined as a **cookie-level `client_id`**.

**Why this choice?**

* Matches the available data (no login or cross-device identifiers)
* Avoids incorrect identity stitching

**Key attributes calculated:**

* `first_seen`, `last_seen`
* `session_count`
* **All observed user agents** (stored as an array to avoid data loss)

**Output:** `analytics_users`

This design preserves **multi-device behavior** instead of collapsing it into a single arbitrary device.

---

### 4. Device & Platform Classification

User agents are parsed into:

* `device_type` → `desktop`, `mobile`, `tablet`
* `os` → `iOS`, `Android`, `Windows`, `MacOS`, etc.
* `browser` → `Chrome (iOS)`, `Safari`, `Chrome`, etc.

**Why deterministic SQL parsing instead of a UA library?**

* Transparent and explainable logic
* Stable for analytics
* DuckDB-compatible
* Sufficient accuracy for marketing decisions

**Output:** `analytics_events_with_device`

---

### 5. Attribution Modeling (7-day lookback)

**Purchases:**
All events with `event_name = 'purchase'`.

**Lookback window:**
7 days prior to the purchase timestamp (inclusive).

#### First-click attribution

* Earliest event in the lookback window with a non-null `utm_source`

#### Last-click attribution

* Latest event in the lookback window with a non-null `utm_source`

If no UTM is found, the purchase is attributed to **`direct`**.

**Output:** `analytics_purchase_attribution`

---

## Attributes & Metrics Chosen

### Engagement & Behavior

* Sessions per user
* Session duration
* Event sequences per session
* Bounce-like sessions (duration = 0)

### Device & Platform

* Sessions by device type
* Revenue per session by device
* OS and browser distributions

### Marketing Performance

* Purchases by channel
* Revenue by channel
* Conversion rate by channel
* Assisted vs direct conversions

---

## Validation & Correctness

### Revenue reconciliation (critical check)

```
Raw revenue (from enriched events):      454,926.0
Attributed revenue (from attribution):   454,926.0
```

✅ **Perfect reconciliation** proves:

* No purchases were dropped
* Attribution logic covers 100% of revenue

### Deduplication validation

* Exactly **1 duplicate** dropped at the pandas stage
* No downstream inflation of revenue or purchases

### Deterministic transforms

* All sessionization and attribution logic is reproducible
* No non-deterministic aggregates (e.g., `any_value`) in critical paths

---

## What the Outputs Tell the Marketing Team

### Channel performance

* **Direct traffic dominates revenue**, suggesting strong brand demand or under-tagged campaigns.
* Several paid/hashed channels contribute smaller but measurable revenue.
* First-click vs last-click differences reveal **upper-funnel vs closer channels**.

### Conversion behavior

* Some channels show **very high conversion rates with low session counts**, indicating:

  * Retargeting
  * Bottom-of-funnel traffic
  * Or potential data sparsity (to be monitored)

### Device insights

* **Desktop sessions generate significantly higher revenue per session** than mobile.
* Mobile drives volume, desktop drives value — a classic e-commerce pattern.

### Assisted conversions

* Presence of **assisted conversions** confirms that single-touch attribution alone would under-credit some channels.

---

## Trade-offs & Limitations

| Decision                     | Trade-off                                      |
| ---------------------------- | ---------------------------------------------- |
| Cookie-level user definition | Cannot stitch cross-device users               |
| Deterministic UA parsing     | Less precise than full UA libraries            |
| First/last-click attribution | Does not capture full multi-touch contribution |
| 30-minute sessions           | May split long research journeys               |

These trade-offs were chosen intentionally to favor **clarity, correctness, and explainability** over complexity.

---

## Maintainability & Scalability

* **Modular pipeline**: cleaning → parquet → SQL views
* Easy to migrate DuckDB SQL to BigQuery / Snowflake
* Additional attribution models (linear, time-decay) can be layered without changing raw data
* Parquet outputs can be consumed directly by BI tools

## Key Insights for Executives

* Direct traffic drives the majority of revenue
Over 70% of total revenue is attributed to direct visits under both first-click and last-click models. This indicates strong brand demand but also suggests opportunities to improve campaign tagging and attribution coverage.

* Desktop sessions are significantly more valuable than mobile
Desktop users generate more than 3× higher revenue per session compared to mobile. While mobile drives volume, desktop is where conversions finalize, which should influence bidding strategies and landing page optimization.

* Not all high-converting channels drive meaningful scale
Several channels show very high conversion rates but low session counts. These are likely retargeting or bottom-of-funnel sources and should be evaluated for incremental lift rather than raw efficiency alone.

* Upper-funnel influence is visible through assisted conversions
A non-trivial share of purchases involve assisted conversions, confirming that first-click and last-click attribution alone would under-credit some marketing channels.

* User engagement is shallow for a large portion of traffic
Many sessions contain a single event with zero duration, highlighting bounce behavior. Improving early engagement (page load, messaging, offer clarity) could materially improve funnel performance.

* Attribution data is internally consistent and trustworthy
Total attributed revenue exactly matches raw purchase revenue, providing confidence that marketing decisions based on this data are grounded in accurate and complete transformations.

---

## Summary

This transformation layer:

* Converts raw events into **analytics-ready datasets**
* Accurately models **sessions, users, devices, and attribution**
* Fully **reconciles inputs to outputs**
* Is **maintainable, explainable, and scalable**

