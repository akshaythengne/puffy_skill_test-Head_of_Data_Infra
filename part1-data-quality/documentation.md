# **Incoming Data Quality Framework **

This module implements a production-ready **Data Quality (DQ) framework** that validates incoming web-tracking event data *before* it enters the analytics layer.
---

## **Overview**

The provided dataset contains 14 days of e-commerce tracking events, exported as date-partitioned CSV files.
This DQ framework performs end-to-end validation across:

* Schema correctness
* Per-file schema drift detection
* Timestamp validity
* Event taxonomy integrity
* JSON payload quality
* Duplicate event detection
* Client ID completeness
* Referrer integrity
* UTM coverage
* Purchase event price validation

All checks return **pass**, **warn**, or **fail**, generating a summary and a Markdown report.

---

## **What the Framework Checks — and Why**

### **1. Strict Per-File Schema Validation**

Each raw file is validated against required normalized fields:
`clientid, pageurl, referrer, timestamp, eventname, eventdata, useragent`.

* Missing critical fields (e.g., timestamp, event_data) → **FAIL**
* Missing non-critical fields (e.g., referrer) → **WARN**
* No issues → **PASS**

This prevents silent schema drift from breaking attribution or revenue logic.

---

### **2. Row Count Anomaly Detection**

Detects:

* Zero-row files
* Files with >50% deviation from mean volume

Prevents ingestion of partial or corrupted files.

---

### **3. Global Schema Completeness**

Ensures all canonical columns exist after normalization.
Protects downstream datasets requiring consistent naming.

---

### **4. Timestamp Validity**

Identifies missing or unparseable timestamps — essential for sessionization, attribution, and funnel modeling.

---

### **5. Event Taxonomy Validation**

Flags events not belonging to Puffy’s approved taxonomy.
Prevents reporting gaps due to untracked or renamed events.

---

### **6. JSON Parse Integrity**

Verifies that `event_data` is valid JSON, using a safe-fallback parser.
Protects against malformed payloads that break purchase logic or cart interactions.

---

### **7. Duplicate Event Detection**

Checks for exact duplicates (same timestamp, file, event_name, and payload).
Prevents inflated metrics such as page views, ATC counts, and conversions.

---

### **8. Client ID Completeness**

Alerts when too many events lack a `client_id`, which harms attribution and user-journey stitching.

---

### **9. Referrer Quality Check**

Detects hashed or anonymized referrer domains, indicating tracking regressions or privacy filtering.

---

### **10. UTM Coverage**

Ensures marketing attribution metadata (source/medium/campaign) is present for a meaningful share of events.

---

### **11. Purchase Event Validation**

Ensures purchase payloads contain valid numeric price fields and no zero/negative values.

---

## **Issues Found in the Provided Dataset**

The framework identified the following:

### **⚠️ Missing Schema Fields (per-file WARN)**

Files **20250304 → 20250308** are missing the `referrer` column.
This likely caused attribution issues mid-period.

---

### **❌ Invalid Event Name (FAIL)**

`checkout_completed` appears but is not part of Puffy’s approved tracking taxonomy.
This would cause funnel and revenue undercounting.

---

### **❌ Duplicate Events (FAIL)**

Duplicate rows detected.
This inflates engagement and conversion metrics.

---

### **⚠️ Missing Client IDs (WARN)**

~9.3% of events have null client_id, hurting attribution quality.

---

### **Other Checks Passed**

* Timestamp validity
* JSON parsing
* Referrer integrity
* UTM coverage
* Purchase price validation

---

## **What Went Wrong During This Period**

Based on detected issues:

### **1. Schema drift occurred after March 4**

The referrer column disappeared mid-period → attribution inconsistencies.

### **2. Unauthorized new event emitted**

`checkout_completed` replaced/augmented expected funnel events → revenue misalignment.

### **3. Duplicate events inflated metrics**

### **4. Missing client IDs reduced attribution accuracy**

Together, these explain why revenue numbers appeared incorrect.

---

## **Does the Framework Catch These Issues in the Future?**

**Yes.**
The framework is resilient across tracking regressions, schema drift, malformed payloads, and ingestion pipeline issues.

It automatically flags:

* New or missing columns
* Abnormal traffic drops
* Unexpected event names
* Invalid JSON
* Duplicates
* Missing attribution metadata
* Broken purchase payloads

This makes it suitable for production ingestion pipelines.

---

## **Recommended Enhancements**

* Fail ingestion when `schema_per_file` or `event_taxonomy` or `duplicates` return **FAIL**.
* Alert (Slack/email) for **WARN** conditions.
* Add rolling baselines for row counts and anomaly detection.
* Integrate with dbt or Great Expectations for declarative validation.
* Add automated dashboards tracking DQ metrics over time.

---

## **Artifacts Produced**

The script generates:

* `dq_report.md`
* `detected_issues_sample.csv`
