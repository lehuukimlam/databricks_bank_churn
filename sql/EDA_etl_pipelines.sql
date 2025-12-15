-- Databricks notebook source
-- MAGIC %md
-- MAGIC Basic EDA for bank_churn.1_bronze.pipelines_raw_customer
-- MAGIC
-- MAGIC 1. Summary statistics: mean, median, mode for key [metrics](url)

-- COMMAND ----------

SELECT
  AVG(balance) AS avg_balance,
  PERCENTILE(balance, 0.5) AS median_balance,
  MODE(balance) AS mode_balance,
  AVG(credit_sco) AS avg_credit_score,
  PERCENTILE(credit_sco, 0.5) AS median_credit_score,
  MODE(credit_sco) AS mode_credit_score,
  AVG(risk_score) AS avg_risk_score,
  PERCENTILE(risk_score, 0.5) AS median_risk_score,
  MODE(risk_score) AS mode_risk_score,
  AVG(age) AS avg_age,
  PERCENTILE(age, 0.5) AS median_age,
  MODE(age) AS mode_age
FROM bank_churn.1_bronze.pipelines_raw_customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Outlier detection using IQR for balance
-- MAGIC

-- COMMAND ----------


WITH stats AS (
  SELECT
    PERCENTILE(balance, 0.25) AS q1,
    PERCENTILE(balance, 0.75) AS q3
  FROM bank_churn.1_bronze.pipelines_raw_customer
)
SELECT
  c.id,
  c.balance,
  CASE
    WHEN c.balance < (s.q1 - 1.5 * (s.q3 - s.q1)) THEN 'Lower Outlier'
    WHEN c.balance > (s.q3 + 1.5 * (s.q3 - s.q1)) THEN 'Upper Outlier'
    ELSE 'Normal'
  END AS balance_outlier_flag
FROM bank_churn.1_bronze.pipelines_raw_customer c
CROSS JOIN stats s;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Distribution counts for categorical metrics

-- COMMAND ----------


SELECT
  gender,
  COUNT(*) AS count
FROM bank_churn.1_bronze.pipelines_raw_customer
GROUP BY gender;

-- COMMAND ----------


SELECT
  occupation,
  COUNT(*) AS count
FROM bank_churn.1_bronze.pipelines_raw_customer
GROUP BY occupation;

-- COMMAND ----------

SELECT
  customer_segment,
  COUNT(*) AS count
FROM bank_churn.1_bronze.pipelines_raw_customer
GROUP BY customer_segment;
