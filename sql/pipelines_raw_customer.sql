CREATE MATERIALIZED VIEW `bank_churn`.`1_bronze`.`pipelines_raw_customer`
  (

    _line BIGINT, _fivetran_synced TIMESTAMP, occupation STRING, tenure_ye BIGINT, gender STRING,
    cluster_group BIGINT, digital_behavior STRING, nums_service BIGINT, balance BIGINT, id BIGINT,
    nums_card BIGINT, active_member BOOLEAN, address STRING, risk_score DOUBLE, origin_province
    STRING, last_transaction_month BIGINT, customer_segment STRING, engagement_score BIGINT, exit
    BOOLEAN, full_name STRING, last_active_date STRING, risk_segment STRING, credit_sco BIGINT,
    created_date STRING, loyalty_level STRING, monthly_ir BIGINT, married BIGINT, age BIGINT
  )
  COMMENT 'The table contains raw customer data relevant for analyzing customer behavior and segmentation. It includes demographic information such as age, gender, and occupation, as well as financial metrics like balance, credit score, and risk score. This data can be used to assess customer engagement, identify at-risk customers, and tailor marketing strategies based on customer segments and behaviors.' AS
SELECT
  *
FROM
  workspace.google_drive.bank_churn