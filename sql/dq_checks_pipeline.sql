-- ============================================================
-- View: bank_churn.0_dq_checks
-- Purpose:
--   Lightweight data quality checks to support exploratory data
--   analysis (EDA) on a client-provided dataset.
--
-- Key principle:
--   This is EDA-first. Outliers are NOT removed or failed here.
--   We keep extreme values for further investigation.
--   Therefore, checks focus on:
--     1) Nulls in critical fields (e.g., customer ID)
--     2) Non-negative validation for numeric metrics
--     3) Duplicate customer IDs
--
-- Data Source:
--   workspace.google_drive.bank_churn (Fivetran landing table)
--
-- Output:
--   Single-row DQ summary + pass_flag for workflow gating.
-- ============================================================

CREATE MATERIALIZED VIEW bank_churn.0_dq_checks.dq_checks AS

WITH base AS (
  -- Raw landing table (no transformations)
  SELECT *
  FROM workspace.google_drive.bank_churn
),

checks AS (
  SELECT
    -- Volume
    COUNT(*) AS row_count,

    -- 1) Critical null checks
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,

    -- Optional: segment required for the segment dashboard
    SUM(CASE WHEN customer_segment IS NULL THEN 1 ELSE 0 END) AS null_segment,

    -- 2) Duplicate ID checks
    (COUNT(*) - COUNT(DISTINCT id)) AS duplicate_customer_id_count,

    -- 3) Non-negative checks (EDA-friendly: do NOT cap outliers)
    SUM(CASE WHEN balance IS NULL OR balance < 0 THEN 1 ELSE 0 END) AS invalid_balance_non_negative,
    SUM(CASE WHEN monthly_ir IS NULL OR monthly_ir < 0 THEN 1 ELSE 0 END) AS invalid_monthly_income_non_negative,
    SUM(CASE WHEN credit_sco IS NULL OR credit_sco < 0 THEN 1 ELSE 0 END) AS invalid_credit_score_non_negative,
    SUM(CASE WHEN tenure_ye IS NULL OR tenure_ye < 0 THEN 1 ELSE 0 END) AS invalid_tenure_years_non_negative,
    SUM(CASE WHEN nums_card IS NULL OR nums_card < 0 THEN 1 ELSE 0 END) AS invalid_num_cards_non_negative,
    SUM(CASE WHEN nums_service IS NULL OR nums_service < 0 THEN 1 ELSE 0 END) AS invalid_num_services_non_negative,
    SUM(CASE WHEN last_transaction_month IS NULL OR last_transaction_month < 0 THEN 1 ELSE 0 END) AS invalid_last_month_txn_non_negative,
    SUM(CASE WHEN engagement_score IS NULL OR engagement_score < 0 THEN 1 ELSE 0 END) AS invalid_engagement_non_negative,
    SUM(CASE WHEN risk_score IS NULL OR risk_score < 0 THEN 1 ELSE 0 END) AS invalid_risk_non_negative

  FROM base
),

final AS (
  SELECT
    row_count,
    null_customer_id,
    null_segment,
    duplicate_customer_id_count,

    invalid_balance_non_negative,
    invalid_monthly_income_non_negative,
    invalid_credit_score_non_negative,
    invalid_tenure_years_non_negative,
    invalid_num_cards_non_negative,
    invalid_num_services_non_negative,
    invalid_last_month_txn_non_negative,
    invalid_engagement_non_negative,
    invalid_risk_non_negative,

    -- Pass criteria (EDA-focused):
    --   - No null customer IDs
    --   - No duplicate customer IDs
    --   - No negative / null values in key numeric metrics
    -- Outliers are allowed (no upper-bound rules applied).
    CASE
      WHEN null_customer_id = 0
       AND duplicate_customer_id_count = 0
       AND invalid_balance_non_negative = 0
       AND invalid_monthly_income_non_negative = 0
       AND invalid_credit_score_non_negative = 0
       AND invalid_tenure_years_non_negative = 0
       AND invalid_num_cards_non_negative = 0
       AND invalid_num_services_non_negative = 0
       AND invalid_last_month_txn_non_negative = 0
       AND invalid_engagement_non_negative = 0
       AND invalid_risk_non_negative = 0
      THEN TRUE
      ELSE FALSE
    END AS pass_flag

  FROM checks
)

SELECT *
FROM final;
