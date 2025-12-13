# 🛏️ Puffy – Head of Data Infrastructure & Analytics Skill Test

I've created this repository using my knowledge and help from Chatgpt. This repository implements an **end-to-end analytics data pipeline** for a DTC e-commerce business, covering:

1. Incoming data quality validation
2. Data cleaning and transformation
3. Business analytics and executive reporting
4. Production-grade monitoring and alerting

The pipeline is designed to reflect **real-world marketing analytics systems** that power dashboards, attribution models, and executive decision-making.

---

## 📁 Repository Structure
Link to the repository - https://github.com/akshaythengne/puffy_skill_test-Head_of_Data_Infra
```text
.
├── data/                          # Raw CSV event exports (date-partitioned)
├── part1-data-quality/
│   └── data_quality_code.py       # Incoming data quality validation
├── part2-transformation/
│   └── clean_and_analyze.py       # Data cleaning + SQL transformations
├── part3-analysis/
│   └── business_analysis.py       # Business analysis & executive metrics
├── part4-monitoring/
│   └── monitoring.py              # Production monitoring & anomaly detection
├── requirements.txt
├── README.md
└── .github/
    └── workflows/
        └── pipeline.yml           # GitHub Actions pipeline
```

---

## 🔧 Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/akshaythengne/puffy_skill_test-Head_of_Data_Infra.git
cd puffy_skill_test-Head_of_Data_Infra
```

---

### 2️⃣ Create a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate        # macOS/Linux
venv\Scripts\activate           # Windows
```

---

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

**Key dependencies**

* `pandas`
* `duckdb`
* `pyarrow`
* `numpy`
* `matplotlib`
* `reportlab`

---

## ▶️ How to Run the Pipeline (Local)

The pipeline **must be run in order**, as each step depends on outputs from the previous step.

---

### ✅ Step 1: Data Quality Validation

```bash
python part1-data-quality/data_quality_code.py
```

**What this does**

* Validates raw event CSV files
* Checks schema completeness (per-file and global)
* Detects duplicates, invalid event taxonomy, malformed JSON
* Flags anomalies (row counts, UTM coverage, referrer issues)
* Produces a data quality report and issue samples

**Why it matters**
Prevents corrupted or incomplete data from entering analytics.

---

### ✅ Step 2: Data Cleaning & Transformation

```bash
python part2-transformation/clean_and_analyze.py
```

**What this does**

* Cleans raw events based on DQ findings
* Fixes event taxonomy issues
* Handles missing columns safely
* Drops exact duplicates
* Parses JSON safely
* Sessionizes user behavior (30-minute inactivity rule)
* Builds analytics-ready DuckDB views
* Generates attribution (first-click & last-click, 7-day window)
* Outputs cleaned Parquet + intermediate analytics tables

**Why it matters**
Creates a **single source of truth** for downstream analytics.

---

### ✅ Step 3: Business Analysis

```bash
python part3-analysis/business_analysis.py
```

**What this does**

* Computes core e-commerce KPIs:

  * Revenue & purchases
  * Conversion rates
  * Device performance
  * Channel performance (first-click vs last-click)
  * Assisted vs direct conversions
* Generates CSV outputs for analysis
* Produces charts (PNG) for executive reporting
* Used to generate the executive summary PDF

**Why it matters**
Transforms analytics data into **business insights**.

---

### ✅ Step 4: Production Monitoring

```bash
python part4-monitoring/monitoring.py
```

**What this does**

* Monitors pipeline health, data integrity, and business drift
* Anchors checks to the **latest date in the data** (not system time)
* Detects:

  * Missing or stale data
  * Duplicate inflation
  * Attribution skew (direct traffic dominance)
  * Revenue and conversion anomalies
* Outputs a structured monitoring report (JSON)

**Why it matters**
Ensures dashboards and marketing decisions remain **trustworthy** in production.

---

## 🧠 End-to-End Pipeline Summary

| Stage                 | Purpose                                                    |
| --------------------- | ---------------------------------------------------------- |
| **Data Quality**      | Catch broken, malformed, or inconsistent raw data          |
| **Transformation**    | Create analytics-ready, sessionized, attributed tables     |
| **Business Analysis** | Answer executive and marketing performance questions       |
| **Monitoring**        | Detect failures, silent corruption, and business anomalies |



## 🚀 GitHub Actions Pipeline

This repository includes a **GitHub Actions workflow** that runs the entire analytics pipeline end-to-end in a clean, reproducible environment.

The workflow is defined in:

```text
.github/workflows/pipeline.yml
```

---

### What the Pipeline Does

The GitHub Actions pipeline executes **all four parts of the assignment in sequence**, mirroring how the pipeline would run in production:

1. **Data Quality Validation**

   * Runs `data_quality_code.py`
   * Validates raw event data before it enters analytics
   * Fails early if critical data issues are detected

2. **Data Cleaning & Transformation**

   * Runs `clean_and_analyze.py`
   * Cleans validated events
   * Builds sessionized, analytics-ready tables
   * Computes first-click and last-click attribution

3. **Business Analysis**

   * Runs `business_analysis.py`
   * Generates marketing and performance metrics
   * Produces CSV outputs and charts used for executive analysis

4. **Production Monitoring**

   * Runs `monitoring.py`
   * Checks pipeline health, data integrity, and business drift
   * Outputs a structured monitoring report

Each step depends on the successful completion of the previous one, ensuring data correctness throughout the pipeline.

---

### When the Pipeline Runs

This workflow is intentionally **not scheduled**, as this is a one-time assignment.

It can be triggered in the following ways:

* **On demand (manual trigger)**
  Using the GitHub Actions UI

* **On pushes to the `main` branch**

* **On pull requests targeting `main`**

This allows:

* Easy re-runs for debugging or review
* Validation of changes before merging
* A clean demonstration of CI-style pipeline execution

---

### How to Run the Pipeline Manually (Recommended)

To run the full pipeline using GitHub Actions:

1. Go to the repository on GitHub
2. Click the **Actions** tab
3. Select **Puffy Analytics Pipeline**
4. Click **Run workflow**
5. Choose the `main` branch and start the run

GitHub Actions will:

* Provision a fresh Ubuntu environment
* Install all dependencies
* Execute the pipeline from start to finish
* Surface logs and failures clearly for each step

---

### Why GitHub Actions Is Used Here

Using GitHub Actions allows this assignment to demonstrate:

* Reproducibility (clean environment every run)
* End-to-end pipeline automation
* CI/CD-style validation for analytics code
* How this pipeline would realistically be operated in a team setting
