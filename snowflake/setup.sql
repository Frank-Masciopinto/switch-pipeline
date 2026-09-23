-- =============================================================================
-- One-time Snowflake setup for switch-pipeline.
-- Run in a Snowsight worksheet as ACCOUNTADMIN, after `make snowflake-keypair`.
-- Names match the defaults in .env.example; change both together if you rename.
--
-- Snowflake no longer lets service users sign in with a password, so the
-- pipeline authenticates with a key pair (JWT) as a TYPE = SERVICE user.
-- =============================================================================
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS SWITCH_PIPELINE_ROLE;
GRANT ROLE SWITCH_PIPELINE_ROLE TO ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS SWITCH_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
GRANT USAGE, OPERATE ON WAREHOUSE SWITCH_WH TO ROLE SWITCH_PIPELINE_ROLE;

-- The demo source lives in its own database: the sample share is read-only and
-- the change simulation must insert and update rows.
CREATE DATABASE IF NOT EXISTS SWITCH_DEMO;
GRANT OWNERSHIP ON DATABASE SWITCH_DEMO TO ROLE SWITCH_PIPELINE_ROLE COPY CURRENT GRANTS;

-- Read access to TPC-H for `make seed` (SEED_STRATEGY=sample_share). If your
-- account has no SNOWFLAKE_SAMPLE_DATA database, create it first:
--   CREATE DATABASE SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE SWITCH_PIPELINE_ROLE;

CREATE USER IF NOT EXISTS SWITCH_PIPELINE
    TYPE = SERVICE
    DEFAULT_ROLE = SWITCH_PIPELINE_ROLE
    DEFAULT_WAREHOUSE = SWITCH_WH
    RSA_PUBLIC_KEY = '<paste the public key printed by make snowflake-keypair>';
GRANT ROLE SWITCH_PIPELINE_ROLE TO USER SWITCH_PIPELINE;

-- Your account identifier for SNOWFLAKE_ACCOUNT (<orgname>-<account_name>):
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS SNOWFLAKE_ACCOUNT;

-- -----------------------------------------------------------------------------
-- Production note: seeding and the change simulation need write access, the
-- adapter does not. With separate users, give the adapter only this role:
--
-- CREATE ROLE IF NOT EXISTS SWITCH_ADAPTER_READER;
-- GRANT USAGE ON WAREHOUSE SWITCH_WH TO ROLE SWITCH_ADAPTER_READER;
-- GRANT USAGE ON DATABASE SWITCH_DEMO TO ROLE SWITCH_ADAPTER_READER;
-- GRANT USAGE ON SCHEMA SWITCH_DEMO.RAW TO ROLE SWITCH_ADAPTER_READER;
-- GRANT SELECT ON TABLE SWITCH_DEMO.RAW.CUSTOMER_ORDERS TO ROLE SWITCH_ADAPTER_READER;
-- -----------------------------------------------------------------------------
