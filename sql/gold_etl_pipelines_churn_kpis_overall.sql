-- ============================================================
-- Object: bank_churn.3_gold.pipelines_churn_kpis_overall
-- Type:   MATERIALIZED VIEW
--
-- Purpose:
--   Provide a single-row, executive-level snapshot of overall
--   bank performance related to customer churn, engagement,
--   risk, and portfolio composition.
--
--   This dataset answers high-level business questions such as:
--   - How severe is churn across the bank?
--   - What is the overall customer mix?
--   - How engaged and risky is the portfolio?
--
-- Source:
--   bank_churn.2_silver.pipelines_banking_customer_info
--
-- Grain:
--   One row representing the entire customer portfolio
--   (point-in-time snapshot)
--
-- Design Principles:
--   - All KPIs are pre-calculated and standardized
--   - No calculations are delegated to dashboards
--   - Ratios are used to provide relative context
--   - Safe for direct executive consumption
--
-- Usage:
--   - Overall Bank Churn Dashboard
--   - Executive reporting
--   - AI exploration via Genie
-- ============================================================

CREATE MATERIALIZED VIEW `bank_churn`.`3_gold`.`pipelines_churn_kpis_overall`
(
  -- Portfolio volume
  total_customers                BIGINT,
  churned_customers              BIGINT,
  churn_rate                     DECIMAL(38, 16),

  -- Financial exposure
  avg_balance_vnd                DOUBLE,
  avg_balance_ratio              DOUBLE,

  -- Credit profile
  avg_credit_score               DOUBLE,
  avg_credit_score_ratio         DOUBLE,

  -- Relationship depth
  avg_tenure_years               DOUBLE,
  avg_num_cards                  DOUBLE,
  avg_num_services               DOUBLE,

  -- Activity
  pct_active_users               DECIMAL(38, 16),

  -- Customer segmentation counts
  count_mass                     BIGINT,
  count_priority                 BIGINT,
  count_affluent                 BIGINT,
  count_emerging                 BIGINT,

  -- Risk profile
  avg_risk_score                 DOUBLE,
  avg_risk_score_ratio           DOUBLE,

  -- Loyalty distribution
  count_loyalty_bronze            BIGINT,
  count_loyalty_silver            BIGINT,
  count_loyalty_gold              BIGINT,
  count_loyalty_platinum          BIGINT
)
AS

-- ------------------------------------------------------------
-- Base CTE
-- Customer-level banking and behavioral data prepared in Silver.
-- ------------------------------------------------------------
WITH base AS (
  SELECT
    customer_id,
    balance_vnd,
    credit_score,
    tenure_years,
    num_cards,
    num_services,
    active_member_flag,
    customer_segment,
    risk_score,
    loyalty_level,
    exit_flag
  FROM
    bank_churn.2_silver.pipelines_banking_customer_info
)

-- ------------------------------------------------------------
-- Final aggregation
-- Portfolio-level KPI calculations.
-- ------------------------------------------------------------
SELECT
  -- Customer volume
  COUNT(*) AS total_customers,

  -- Churn volume
  SUM(
    CASE
      WHEN exit_flag = TRUE THEN 1
      ELSE 0
    END
  ) AS churned_customers,

  -- Churn rate: churned customers / total customers
  SUM(
    CASE
      WHEN exit_flag = TRUE THEN 1
      ELSE 0
    END
  ) * 1.0 / COUNT(*) AS churn_rate,

  -- Average balance (absolute)
  AVG(balance_vnd) AS avg_balance_vnd,

  -- Average balance relative to the maximum balance
  -- Used to provide scale/context for portfolio concentration
  AVG(balance_vnd) / MAX(balance_vnd) AS avg_balance_ratio,

  -- Credit profile (absolute)
  AVG(credit_score) AS avg_credit_score,

  -- Credit score relative to portfolio maximum
  AVG(credit_score) / MAX(credit_score) AS avg_credit_score_ratio,

  -- Relationship depth
  AVG(tenure_years) AS avg_tenure_years,
  AVG(num_cards) AS avg_num_cards,
  AVG(num_services) AS avg_num_services,

  -- Activity ratio
  SUM(
    CASE
      WHEN active_member_flag = TRUE THEN 1
      ELSE 0
    END
  ) * 1.0 / COUNT(*) AS pct_active_users,

  -- Customer segment composition
  SUM(CASE WHEN customer_segment = 'Mass' THEN 1 ELSE 0 END) AS count_mass,
  SUM(CASE WHEN customer_segment = 'Priority' THEN 1 ELSE 0 END) AS count_priority,
  SUM(CASE WHEN customer_segment = 'Affluent' THEN 1 ELSE 0 END) AS count_affluent,
  SUM(CASE WHEN customer_segment = 'Emerging' THEN 1 ELSE 0 END) AS count_emerging,

  -- Risk profile (absolute)
  AVG(risk_score) AS avg_risk_score,

  -- Risk score relative to maximum observed risk
  AVG(risk_score) / MAX(risk_score) AS avg_risk_score_ratio,

  -- Loyalty distribution
  SUM(CASE WHEN loyalty_level = 'Bronze' THEN 1 ELSE 0 END) AS count_loyalty_bronze,
  SUM(CASE WHEN loyalty_level = 'Silver' THEN 1 ELSE 0 END) AS count_loyalty_silver,
  SUM(CASE WHEN loyalty_level = 'Gold' THEN 1 ELSE 0 END) AS count_loyalty_gold,
  SUM(CASE WHEN loyalty_level = 'Platinum' THEN 1 ELSE 0 END) AS count_loyalty_platinum

FROM
  base;