CREATE MATERIALIZED VIEW `bank_churn`.`3_gold`.`pipelines_churn_kpis_overall`
  (

    total_customers BIGINT, churned_customers BIGINT, churn_rate DECIMAL(38, 16), avg_balance_vnd
    DOUBLE, avg_balance_ratio DOUBLE, avg_credit_score DOUBLE, avg_credit_score_ratio DOUBLE,
    avg_tenure_years DOUBLE, avg_num_cards DOUBLE, avg_num_services DOUBLE, pct_active_users
    DECIMAL(38, 16), count_mass BIGINT, count_priority BIGINT, count_affluent BIGINT, count_emerging
    BIGINT, avg_risk_score DOUBLE, avg_risk_score_ratio DOUBLE, count_loyalty_bronze BIGINT,
    count_loyalty_silver BIGINT, count_loyalty_gold BIGINT, count_loyalty_platinum BIGINT
  ) AS
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
SELECT
  COUNT(*) AS total_customers,
  SUM(
    CASE
      WHEN exit_flag = TRUE THEN 1
      ELSE 0
    END
  ) AS churned_customers,
  SUM(
    CASE
      WHEN exit_flag = TRUE THEN 1
      ELSE 0
    END
  )
  * 1.0
  / COUNT(*) AS churn_rate,
  AVG(balance_vnd) AS avg_balance_vnd,
  AVG(balance_vnd) / MAX(balance_vnd) AS avg_balance_ratio,
  AVG(credit_score) AS avg_credit_score,
  AVG(credit_score) / MAX(credit_score) AS avg_credit_score_ratio,
  AVG(tenure_years) AS avg_tenure_years,
  AVG(num_cards) AS avg_num_cards,
  AVG(num_services) AS avg_num_services,
  SUM(
    CASE
      WHEN active_member_flag = TRUE THEN 1
      ELSE 0
    END
  )
  * 1.0
  / COUNT(*) AS pct_active_users,
  SUM(
    CASE
      WHEN customer_segment = 'Mass' THEN 1
      ELSE 0
    END
  ) AS count_mass,
  SUM(
    CASE
      WHEN customer_segment = 'Priority' THEN 1
      ELSE 0
    END
  ) AS count_priority,
  SUM(
    CASE
      WHEN customer_segment = 'Affluent' THEN 1
      ELSE 0
    END
  ) AS count_affluent,
  SUM(
    CASE
      WHEN customer_segment = 'Emerging' THEN 1
      ELSE 0
    END
  ) AS count_emerging,
  AVG(risk_score) AS avg_risk_score,
  AVG(risk_score) / MAX(risk_score) AS avg_risk_score_ratio,
  SUM(
    CASE
      WHEN loyalty_level = 'Bronze' THEN 1
      ELSE 0
    END
  ) AS count_loyalty_bronze,
  SUM(
    CASE
      WHEN loyalty_level = 'Silver' THEN 1
      ELSE 0
    END
  ) AS count_loyalty_silver,
  SUM(
    CASE
      WHEN loyalty_level = 'Gold' THEN 1
      ELSE 0
    END
  ) AS count_loyalty_gold,
  SUM(
    CASE
      WHEN loyalty_level = 'Platinum' THEN 1
      ELSE 0
    END
  ) AS count_loyalty_platinum
FROM
  base
