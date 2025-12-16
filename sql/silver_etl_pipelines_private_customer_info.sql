-- ============================================================
-- Object: bank_churn.2_silver.pipelines_private_customer_info
-- Type:   MATERIALIZED VIEW
--
-- Purpose:
--   Provide a Silver-layer dataset containing customer private
--   and demographic attributes (PII / sensitive fields) that are
--   separated from banking behavior metrics for governance.
--
-- Governance / Access Control:
--   This dataset is intended to be protected via restricted access
--   and/or Row-Level Security (RLS) in downstream consumption.
--   It should not be used as a default source for dashboards.
--
--   Principle:
--   - PII is isolated from banking and churn metrics
--   - Joins to this dataset should be explicit and justified
--
-- Source:
--   bank_churn.1_bronze.pipelines_raw_customer
--
-- Grain:
--   One row per customer (snapshot)
--
-- Transformations:
--   - Rename key fields to consistent analytical naming
--   - Derive a human-readable marital_status label from the
--     marital_status_code (married)
--
-- Usage:
--   - Customer profiling (controlled)
--   - Optional enrichment for analysis where PII is required
--   - Not required for Gold KPI layer in this project
-- ============================================================

CREATE MATERIALIZED VIEW `bank_churn`.`2_silver`.`pipelines_private_customer_info`
(
  -- Primary identifier
  customer_id             BIGINT,

  -- PII / sensitive personal attributes
  full_name               STRING,
  gender                  STRING,
  age                     BIGINT,
  occupation              STRING,

  -- Marital status (raw code + derived label)
  marital_status_code     BIGINT,
  marital_status          STRING,

  -- Location attributes (still considered sensitive)
  origin_province         STRING,
  address                 STRING,

  -- Income (sensitive)
  monthly_income_vnd      BIGINT
)
AS
SELECT
  id                      AS customer_id,
  full_name,
  gender,
  age,
  married                 AS marital_status_code,

  -- Derive readable marital status for reporting / analysis
  CASE married
    WHEN 0 THEN 'Single'
    WHEN 1 THEN 'Married'
    WHEN 2 THEN 'Divorced'
    WHEN 3 THEN 'Widowed'
    ELSE 'Unknown'
  END                     AS marital_status,

  occupation,
  origin_province,
  address,
  monthly_ir              AS monthly_income_vnd
FROM
  bank_churn.1_bronze.pipelines_raw_customer;