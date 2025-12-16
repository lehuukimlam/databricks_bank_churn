-- ============================================================
-- Object: bank_churn.2_silver.pipelines_banking_customer_info
-- Type:   MATERIALIZED VIEW
--
-- Purpose:
--   Provide a cleaned and business-ready Silver-layer dataset
--   containing customer banking, behavioral, engagement, and
--   risk attributes for analytical use.
--
-- Design Principles:
--   - Derived from Bronze raw data
--   - One row per customer (snapshot)
--   - Focuses only on banking-relevant attributes
--   - Excludes direct PII (handled separately in Silver private view)
--
-- Date Handling:
--   created_date and last_active_date are intentionally retained
--   as STRING values at this stage.
--
--   Reason:
--   - This dataset represents a point-in-time snapshot
--   - Source data provides dates as strings
--   - No assumptions are imposed on date formats in Silver
--   - Proper parsing and temporal modeling are deferred until
--     future time-series data becomes available
--
-- Source:
--   bank_churn.1_bronze.pipelines_raw_customer
--
-- Grain:
--   One row per customer
--
-- Usage:
--   - Primary input for Gold-layer KPI calculations
--   - Safe for broad analytical access
--   - Used by dashboards and AI exploration
-- ============================================================

CREATE MATERIALIZED VIEW `bank_churn`.`2_silver`.`pipelines_banking_customer_info`
(
  -- Primary identifier
  customer_id                   BIGINT,

  -- Financial metrics
  balance_vnd                   BIGINT,
  credit_score                  BIGINT,
  last_month_txn_amount_vnd     BIGINT,

  -- Relationship attributes
  tenure_years                  BIGINT,
  num_cards                     BIGINT,
  num_services                  BIGINT,
  active_member_flag            BOOLEAN,

  -- Temporal attributes (raw snapshot representation)
  created_date                  STRING,
  last_active_date              STRING,

  -- Segmentation and engagement
  customer_segment               STRING,
  engagement_score               BIGINT,
  loyalty_level                  STRING,
  digital_behavior               STRING,

  -- Risk and modeling attributes
  risk_score                     DOUBLE,
  risk_segment                   STRING,
  cluster_group                  BIGINT,

  -- Target variable
  exit_flag                      BOOLEAN
)
AS
SELECT
  id                        AS customer_id,
  balance                   AS balance_vnd,
  credit_sco                AS credit_score,
  tenure_ye                 AS tenure_years,
  nums_card                 AS num_cards,
  nums_service              AS num_services,
  last_transaction_month    AS last_month_txn_amount_vnd,
  active_member             AS active_member_flag,
  created_date,
  last_active_date,
  customer_segment,
  engagement_score,
  loyalty_level,
  digital_behavior,
  risk_score,
  risk_segment,
  cluster_group,
  exit                      AS exit_flag
FROM
  bank_churn.1_bronze.pipelines_raw_customer;