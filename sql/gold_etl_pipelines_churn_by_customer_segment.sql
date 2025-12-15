CREATE MATERIALIZED VIEW `bank_churn`.`3_gold`.`pipelines_churn_by_customer_segment`
  (

    customer_segment STRING, total_customers BIGINT, churn_rate DECIMAL(38, 16), avg_balance_vnd
    DOUBLE, segment_balance_share DECIMAL(38, 16), avg_credit_score DOUBLE, avg_risk_score DOUBLE,
    avg_tenure_years DOUBLE, avg_num_cards DOUBLE, avg_num_services DOUBLE, active_users BIGINT,
    active_user_share DECIMAL(38, 16)
  ) AS
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
SELECT
  b.customer_segment,
  COUNT(*) AS total_customers,
  SUM(
    CASE
      WHEN exit_flag = TRUE THEN 1
      ELSE 0
    END
  )
  * 1.0
  / COUNT(*) AS churn_rate,
  AVG(balance_vnd) AS avg_balance_vnd,
  SUM(balance_vnd) * 1.0 / t.total_balance_all_segments AS segment_balance_share,
  AVG(credit_score) AS avg_credit_score,
  AVG(risk_score) AS avg_risk_score,
  AVG(tenure_years) AS avg_tenure_years,
  AVG(num_cards) AS avg_num_cards,
  AVG(num_services) AS avg_num_services,
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
  )
  * 1.0
  / t.total_active_all_segments AS active_user_share
FROM
  base b CROSS JOIN totals t
GROUP BY
  b.customer_segment,
  t.total_balance_all_segments,
  t.total_active_all_segments
