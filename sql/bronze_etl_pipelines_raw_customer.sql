-- ============================================================
-- Object: bank_churn.1_bronze.pipelines_raw_customer
-- Type:   MATERIALIZED VIEW
--
-- Purpose:
--   Represent the raw customer dataset as ingested from the
--   client’s shared drive (via Fivetran) inside the Databricks
--   Lakehouse. This object serves as the Bronze layer source of
--   truth for all downstream transformations.
--
-- Design Principles:
--   - No data cleansing or business logic applied
--   - Preserve original structure, values, and granularity
--   - Ensure reproducibility and traceability to the source
--
-- Source:
--   workspace.google_drive.bank_churn
--   (CSV files ingested via Fivetran from Google Drive)
--
-- Grain:
--   One row per customer (snapshot)
--
-- Usage:
--   - Input for Silver-layer transformations
--   - Reference point for data quality validation and EDA
--   - Never queried directly by dashboards or business users
-- ============================================================

CREATE MATERIALIZED VIEW `bank_churn`.`1_bronze`.`pipelines_raw_customer`
(
  -- Metadata fields
  _line                     BIGINT,
  _fivetran_synced           TIMESTAMP,

  -- Demographic attributes
  full_name                 STRING,
  gender                    STRING,
  age                       BIGINT,
  married                   BIGINT,
  occupation                STRING,
  origin_province           STRING,
  address                   STRING,

  -- Relationship & tenure
  tenure_ye                 BIGINT,
  created_date              STRING,
  last_active_date           STRING,
  active_member             BOOLEAN,

  -- Product & service usage
  nums_card                 BIGINT,
  nums_service              BIGINT,
  digital_behavior           STRING,
  loyalty_level              STRING,

  -- Financial metrics
  monthly_ir                BIGINT,
  balance                   BIGINT,
  credit_sco                BIGINT,
  last_transaction_month    BIGINT,

  -- Risk & engagement
  engagement_score           BIGINT,
  risk_score                 DOUBLE,
  risk_segment               STRING,
  cluster_group              BIGINT,

  -- Segmentation & target
  customer_segment           STRING,
  exit                       BOOLEAN,

  -- Primary identifier
  id                         BIGINT
)
COMMENT '
Bronze-layer materialized view containing raw customer data as supplied by the client.
The dataset includes demographic attributes, banking relationships, financial metrics,
engagement indicators, risk measures, and churn labels. No transformations or assumptions
are applied at this stage. This table serves as the immutable source of truth for all
downstream Silver and Gold analytics.'
AS
SELECT
  *
FROM
  workspace.google_drive.bank_churn;