-- ============================================================
-- Object: bank_churn.3_gold.pipelines_churn_by_customer_segment
-- Type:   MATERIALIZED VIEW
--
-- Purpose:
--   Provide a business-ready, segment-level churn analytics
--   dataset to support executive and commercial decision-making.
--
--   This Gold-layer view aggregates customer-level banking data
--   into customer segments (Mass, Emerging, Affluent, Priority)
--   and calculates standardized KPIs used by dashboards and
--   AI exploration tools.
--
-- Source:
--   bank_churn.2_silver.pipelines_banking_customer_info
--
-- Grain:
--   One row per customer_segment
--
-- Design Principles:
--   - All metrics are pre-aggregated and standardized
--   - No dashboard-side calculations
--   - Safe for direct consumption by dashboards and Genie
--   - Metrics reflect a point-in-time snapshot
--
-- Usage:
--   - Churn by Customer Segment Dashboard
--   - Executive reporting
--   - Business discussion and prioritisation
-- ============================================================

CREATE MATERIALIZED VIEW `bank_churn`.`3_gold`.`pipelines_churn_by_customer_segment`
(
  -- Segmentation
  customer_segment               STRING,

  -- Customer volume
  total_customers                BIGINT,

  -- Churn metrics
  churn_rate                     DECIMAL(38, 16),

  -- Financial metrics
  avg_balance_vnd                DOUBLE,
  segment_balance_share          DECIMAL(38, 16),

  -- Risk & credit metrics
  avg_credit_score               DOUBLE,
  avg_risk_score                 DOUBLE,

  -- Relationship metrics
  avg_tenure_years               DOUBLE,
  avg_num_cards                  DOUBLE,
  avg_num_services               DOUBLE,

  -- Activity metrics
  active_users                   BIGINT,
  active_user_share              DECIMAL(38, 16)
)
AS

-- ------------------------------------------------------------
-- Base CTE
-- Customer-level banking data prepared in Silver layer.
-- ------------------------------------------------------------
WITH base AS (
  SELECT
    customer_id,
    customer_segment,
    balance_vnd,
    credit_score,
    risk_score,
    tenure_years,
    num_cards,
    num_services,
    active_member_flag,
    exit_flag
  FROM
    bank_churn.2_silver.pipelines_banking_customer_info
),

-- ------------------------------------------------------------
-- Totals CTE
-- Portfolio-level totals used to calculate segment shares.
-- ------------------------------------------------------------
totals AS (
  SELECT
    SUM(balance_vnd) AS total_balance_all_segments,

    SUM(
      CASE
        WHEN active_member_flag = TRUE THEN 1
        ELSE 0
      END
    ) AS total_active_all_segments
  FROM
    base
)

-- ------------------------------------------------------------
-- Final aggregation
-- Segment-level KPI calculations.
-- ------------------------------------------------------------
SELECT
  b.customer_segment,

  -- Customer volume
  COUNT(*) AS total_customers,

  -- Churn rate: churned customers / total customers
  SUM(
    CASE
      WHEN exit_flag = TRUE THEN 1
      ELSE 0
    END
  ) * 1.0 / COUNT(*) AS churn_rate,

  -- Financial exposure
  AVG(balance_vnd) AS avg_balance_vnd,
  SUM(balance_vnd) * 1.0 / t.total_balance_all_segments AS segment_balance_share,

  -- Credit and risk
  AVG(credit_score) AS avg_credit_score,
  AVG(risk_score) AS avg_risk_score,

  -- Relationship depth
  AVG(tenure_years) AS avg_tenure_years,
  AVG(num_cards) AS avg_num_cards,
  AVG(num_services) AS avg_num_services,

  -- Activity
  SUM(
    CASE
      WHEN active_member_flag = TRUE THEN 1
      ELSE 0
    END
  ) AS active_users,

  SUM(
    CASE
      WHEN active_member_flag = TRUE THEN 1
      ELSE 0
    END
  ) * 1.0 / t.total_active_all_segments AS active_user_share

FROM
  base b
  CROSS JOIN totals t

GROUP BY
  b.customer_segment,
  t.total_balance_all_segments,
  t.total_active_all_segments;