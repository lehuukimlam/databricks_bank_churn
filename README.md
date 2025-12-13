# Bank Churn Analytics on Databricks  
**Personal Project – Simulated Consulting Engagement**

![End-to-end Databricks Lakehouse Architecture](assets/Flow.png)

---

## 1. Context & Objective

This is a personal analytics engineering project designed to **showcase practical Databricks capabilities** through a simulated consulting engagement with a **medium-sized local retail bank**.

The bank approaches an analytics consultant to help **understand and monitor customer churn** using an existing dataset stored as CSV files on a shared drive. Beyond solving the business problem, the project explicitly aims to demonstrate the ability to:

- Design and operate a Databricks Lakehouse
- Use Unity Catalog for project-level organisation
- Implement Bronze / Silver / Gold modeling
- Build and run Databricks Pipelines
- Deliver business-ready dashboards from a governed semantic layer
- Integrate Databricks-native AI exploration (Genie) on curated data

The objective is not to over-engineer, but to reflect how Databricks is realistically used in a consulting or in-house analytics context.

---

## 2. Business Understanding

### Business Problem

The bank has observed increasing customer attrition but lacks a clear and consistent way to answer questions such as:

- How severe is customer churn across the whole bank?
- Which customer segments are most affected?
- Are high-value or high-risk customers leaving disproportionately?

The bank owner does **not** want:
- Raw tables or spreadsheets
- Ad-hoc queries with inconsistent definitions
- Complex technical artifacts

They want:
- A **high-level executive view** of churn
- A **segment-level view** for commercial decision-making
- Metrics that can be discussed and iterated collaboratively

---

## 3. Data Understanding

### Data Source

The dataset used in this project is a **synthetic retail banking churn dataset** sourced from Kaggle:

- **Dataset**: *Vietnam Bank Churn Dataset 2025*  
- **Author**: Tran Huu Nhan  
- **Link**:  
  https://www.kaggle.com/datasets/tranhuunhan/vietnam-bank-churn-dataset-2025  

The dataset simulates customer behavior, financial profiles, engagement, and churn outcomes for a Vietnamese retail bank. While synthetic, the structure and business logic closely resemble real-world banking data.

The bank owner (simulated client) provides this dataset as CSV files on a shared drive, representing a common real-world scenario where data is not yet managed in a centralized database.

---

### Data Structure

The dataset is **customer-level**, with one row per customer. Key categories include:

| Category | Description |
|------|-------------|
| Customer Identity | Customer ID, name, gender, age, marital status |
| Demographics | Occupation, province, residential district |
| Financial Profile | Monthly income, balance, credit score |
| Relationship | Tenure, number of cards, number of services |
| Engagement | Engagement score, loyalty level, digital behavior |
| Risk | Risk score, risk segment |
| Activity | Active member flag, last transaction |
| Target | Churn indicator (`exit`) |
| Segmentation | Business segment (Mass, Emerging, Affluent, Priority) |

The presence of both **personally identifiable information (PII)** and **banking behavior data** informs downstream modeling and governance decisions.

---

## 4. Agreed Analytics Scope

After reviewing the data with the bank owner, the scope is intentionally kept focused.

Two analytical deliverables are agreed:

1. **Overall Bank Churn Dashboard**  
   - Portfolio-level KPIs for executives

2. **Churn by Customer Segment Dashboard**  
   - Segment breakdown (Mass, Emerging, Affluent, Priority)

The understanding is that:
- These dashboards answer the first-order business questions
- Metric definitions must be consistent and transparent
- Additional analysis can be added later if required

---

## 5. Data Preparation & Architecture (Databricks Focus)

### Ingestion

- Source: CSV files on Google Drive (simulating a shared drive)
- Ingestion tool: Fivetran
- Output: A raw table landed inside Databricks

Once ingested, Databricks becomes the **single analytics platform**. Databricks Pipelines and Jobs do not depend on where the data originated.

---

### Lakehouse Design

This project uses a **Bronze / Silver / Gold** Lakehouse pattern implemented natively in Databricks.

A dedicated Unity Catalog is created:

- **Catalog**: `bank_churn`
- **Schemas**:
  - `bronze`
  - `silver`
  - `gold`

This structure isolates the project cleanly and mirrors enterprise Databricks deployments.

---

### Bronze Layer – Raw

- Object: `bank_churn.bronze.raw_customer`
- Implementation: a view over the Fivetran-ingested table
- Purpose:
  - Represent the raw source of truth inside the project catalog
  - No transformations or assumptions

---

### Silver Layer – Cleaned & Governed

To reflect real-world banking governance requirements, the Silver layer is **intentionally split**:

#### Silver – Customer Private Information
Contains **PII and sensitive attributes**, including:
- Name, gender, age, marital status
- Occupation, province, address
- Monthly income

**Governance note**:  
This dataset is designed to be protected by **Row-Level Security (RLS)** and restricted access in downstream tools. It is not intended for broad analytical use.

---

#### Silver – Customer Banking Information
Contains **banking, behavioral, and risk attributes**, including:
- Balance, credit score
- Tenure, cards, services
- Engagement and loyalty
- Risk scores and segments
- Activity flags
- Churn label

This dataset forms the **primary analytical base** for all Gold-layer metrics.

---

### Gold Layer – Business Semantic Layer

The Gold layer contains **only business-ready datasets**, designed for dashboards and AI exploration.

Two Gold views are created:

1. **Overall Churn KPIs**
   - Single-row portfolio snapshot

2. **Churn by Customer Segment**
   - Segment-level KPIs (one row per segment)

All KPI logic lives in the Gold layer. Dashboards do not compute metrics independently.

---

## 6. ETL Implementation (Databricks Pipelines)

- All transformations are implemented using **Databricks Pipelines**
- Each Bronze, Silver, and Gold dataset is defined as SQL inside the pipeline
- Dependencies are automatically visualized (Bronze → Silver → Gold)
- Pipelines materialize all table views end-to-end

This demonstrates:
- Native Databricks ELT capabilities
- Clear lineage and dependency management
- Centralized, maintainable transformation logic

---

## 7. Orchestration (Databricks Jobs)

A simple Databricks **Job** is used to:

- Re-run the pipeline after data refresh
- Refresh dashboards

Data ingestion via Fivetran is triggered manually to avoid exposing credentials in a public repository. The Job focuses only on internal analytics refresh, which is appropriate for a portfolio project.

---

## 8. Value Delivered

### Deliverable 1 – Overall Churn Dashboard
- Total customers
- Churn rate
- Average balance
- Credit and risk indicators
- Loyalty distribution

### Deliverable 2 – Churn by Segment Dashboard
- Churn rate by segment
- Segment balance contribution
- Credit score and risk comparison
- Tenure and activity indicators

These dashboards provide:
- Clear executive visibility
- Consistent KPI definitions
- A strong foundation for further analytics or AI exploration

---

## 9. AI Exploration (Genie)

The Gold layer is exposed to **Databricks Genie Space** to enable natural-language Q&A, such as:

- “Which segment has the highest churn rate?”
- “How does average balance differ between Priority and Mass customers?”
- “What share of active users belong to each segment?”

Genie operates exclusively on the Gold semantic layer, ensuring:
- No exposure of raw or sensitive data
- Consistent KPI definitions
- Business-friendly interaction

---

## 10. Notes

- This project focuses on **Databricks usage, Lakehouse modeling, and analytics engineering**, not production ML systems.
- No credentials, API keys, or external configuration are included.
- The scope is intentionally constrained to demonstrate clarity, governance, and business alignment.
