CREATE MATERIALIZED VIEW `bank_churn`.`2_silver`.`pipelines_banking_customer_info`
  (

    customer_id BIGINT, balance_vnd BIGINT, credit_score BIGINT, tenure_years BIGINT, num_cards
    BIGINT, num_services BIGINT, last_month_txn_amount_vnd BIGINT, active_member_flag BOOLEAN,
    created_date STRING, last_active_date STRING, customer_segment STRING, engagement_score BIGINT,
    loyalty_level STRING, digital_behavior STRING, risk_score DOUBLE, risk_segment STRING,
    cluster_group BIGINT, exit_flag BOOLEAN
  ) AS
SELECT
  id AS customer_id,
  balance AS balance_vnd,
  credit_sco AS credit_score,
  tenure_ye AS tenure_years,
  nums_card AS num_cards,
  nums_service AS num_services,
  last_transaction_month AS last_month_txn_amount_vnd,
  active_member AS active_member_flag,
  created_date,
  last_active_date,
  customer_segment,
  engagement_score,
  loyalty_level,
  digital_behavior,
  risk_score,
  risk_segment,
  cluster_group,
  exit AS exit_flag
FROM
  bank_churn.1_bronze.pipelines_raw_customer