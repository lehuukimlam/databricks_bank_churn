CREATE MATERIALIZED VIEW `bank_churn`.`2_silver`.`pipelines_private_customer_info`
  (

    customer_id BIGINT, full_name STRING, gender STRING, age BIGINT, marital_status_code BIGINT,
    marital_status STRING, occupation STRING, origin_province STRING, address STRING,
    monthly_income_vnd BIGINT
  ) AS
SELECT
  id AS customer_id,
  full_name,
  gender,
  age,
  married AS marital_status_code,
  CASE married
    WHEN 0 THEN 'Single'
    WHEN 1 THEN 'Married'
    WHEN 2 THEN 'Divorced'
    WHEN 3 THEN 'Widowed'
    ELSE 'Unknown'
  END AS marital_status,
  occupation,
  origin_province,
  address,
  monthly_ir AS monthly_income_vnd
FROM
  bank_churn.1_bronze.pipelines_raw_customer
