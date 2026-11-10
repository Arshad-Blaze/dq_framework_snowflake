# Master Operational Implementation Plan: Automotive Dealer Risk Data Quality & Machine Learning Architecture (Snowflake-Native)

---

## Executive Summary

This master operational blueprint defines the end-to-end Data Engineering, Data Quality, Data Profiling, Dynamic Scope Filtering, and Machine Learning architecture for the **Automotive Dealer Risk-Scoring & Retention Project** (covering Jeep India and Citroën India vehicle parcs).

The entire workflow is natively executed inside **Snowflake**. It leverages **Snowflake CoCo** for Data Quality monitoring and Data Metric Function (DMF) generation, multi-region dataset unioning (`IAP Salesforce`, `IAP Salesforce Org2`, `India XP`), scope-based filtering (Staging -> Silver), dynamic data cleansing, entity-relationship resolution, master data referencing, feature engineering via Snowpark, and in-database ML model training and scoring via **Snowpark ML / Snowflake Cortex ML**.

---

## System Architecture

+---------------------------------------------------------------------------------------------------+
|                                 STAGING LAYER: 36 RAW SOURCE TABLES                               |
|                                                                                                   |
|  [IAP Salesforce] (8 Tables)     [IAP Salesforce Org2] (18 Tables)   [India XP Legacy] (10 Tables)|
|  - Leads, Orders, Assets,        - Leads, Quotes, Orders, Assets,    - Orders, Assets, Dealers,   |
|    Test Drives, Dealers, etc.      Cases, Work Orders, Dealers, etc.   Work Orders, Products, etc.|
+---------------------------------------------------------------------------------------------------+
                                                  │
                                                  │ (Region Union, Scope Filters & Deduplication)
                                                  ▼
+---------------------------------------------------------------------------------------------------+
|                                    DEALER_RISK_DB.SILVER LAYER                                   |
|                                                                                                   |
|  - MD_DEALERS       (Filtered & Consolidated Master Dealer Lookup across all regions)          |
|  - MD_VEHICLES      (Filtered & Consolidated Parc Assets + SHA-256 Anonymized VINs)            |
|  - FCT_WORK_ORDERS  (Filtered & Consolidated After-Sales Repair Orders across legacy & Org2)   |
|  - Dynamic Snowflake Data Metric Functions (DMFs) for Quality Validation Gates                     |
+---------------------------------------------------------------------------------------------------+
                                                  │
                                                  │ (Monthly Feature Aggregations)
                                                  ▼
+---------------------------------------------------------------------------------------------------+
|                                    DEALER_RISK_DB.GOLD LAYER                                      |
|                                     (MODEL-FOCUSED DATA MARTS)                                    |
|                                                                                                   |
|  - MART_DEALER_RETENTION_FEATURES (Aggregated Dealer Risk Metrics & Workshop KPIs)               |
|  - MART_VEHICLE_LAPSE_FEATURES    (Vehicle Churn/Lapse Feature Matrix)                           |
|  - Snowflake Model Registry       (Registered Snowpark ML / Cortex Models)                        |
+---------------------------------------------------------------------------------------------------+
                                                  │
                                                  │ (In-Database Inference)
                                                  ▼
+---------------------------------------------------------------------------------------------------+
|                                    DEALER_RISK_DB.RISK_MART                                       |
|                                                                                                   |
|  - DEALER_SCORES (Final Dealer Risk Probabilities & Retention Metrics for Downstream Delivery)     |
+---------------------------------------------------------------------------------------------------+

---

## Step 1: Source Data Inventory, Scope Filtering & Master Data Resolution

### 1.1 Complete Inventory of Source Tables
The 36 staging tables ingested daily from Salesforce across the three source environments (`IAP Salesforce`, `IAP Salesforce Org2`, and `India XP`) are unioned and cleansed into three core Silver Master Entities:
1. `SILVER.MD_DEALERS` (Dealer Network Master)
2. `SILVER.MD_VEHICLES` (Vehicle Parc Inventory & Ownership)
3. `SILVER.FCT_WORK_ORDERS` (After-Sales Workshop Transactions)

### 1.2 Raw-to-Silver Data Filtering Rules (Scope Alignment)
When moving raw staging data into Silver consolidated tables, strict business logic filters align with the project scope:
* **Brand Scope:** `UPPER(TRIM(GN_BRAND)) IN ('JEEP', 'CITROEN')`.
* **Geographic Scope:** `COUNTRY_CODE = 'IN'` (India parc).
* **Dealer Master:** `GN_STATUS IN ('ACTIVE', 'INACTIVE')` and `DEALER_TYPE IN ('AUTHORIZED_DEALER', 'WORKSHOP')`.
* **Vehicle Master:** `DT_DELIVERED_DATE IS NOT NULL`, `DELIVERY_STATUS = 'DELIVERED'`, and `LENGTH(TRIM(GN_VIN_NUMBER)) = 17`.
  * **PII Protection:** Hashing raw VIN numbers using SHA-256 (`VIN_HASH = SHA2(UPPER(TRIM(GN_VIN_NUMBER)), 256)`).
* **Work Orders:** `DT_RO_CREATED_DATE >= DATEADD(year, -3, CURRENT_DATE())`, `GN_RO_TYPE IN ('PM', 'GR', 'BP')`, and `AMT_TOTAL_AMOUNT >= 0`.

---

## Step 2: Workspace Script Catalog & Execution Plan

Snowflake CoCo generates and manages **6 sequential scripts** stored in `{Dealer_Risk_DQ_Framework_with_Snowflake_CoCo}`:

1. **`01_DQ_data_profiling.sql`**: Profiles raw staging tables, assessing null ratios, record counts, regional key overlaps, and schema differences.
2. **`02_DQ_Proposed_Rules_and_Cleansing_Spec.sql`**: Rules definitions for raw tables and specifications for unioning, deduplicating, and scope-filtering into Silver entities.
3. **`03_DQ_Rules_and_DMFs.sql`**: Snowflake System and Custom Data Metric Function (DMF) declarations and schedule attachments on Silver tables.
4. **`04_Cleansing_and_Feature_Views.sql`**: DDLs for metadata tables (`DQ_RULE_CONFIG`, `DQ_RUN_CONTROL`, etc.), regional union views (`MD_DEALERS`, `MD_VEHICLES`, `FCT_WORK_ORDERS`), and Model-Focused Data Marts (`MART_DEALER_RETENTION_FEATURES`, `MART_VEHICLE_LAPSE_FEATURES`).
5. **`05_DQ_Orchestration_and_ML_Pipeline.sql`**: Stored procedures for running DQ quality gates, refreshing Gold feature marts, calling Snowpark ML scoring models, and populating `RISK_MART.DEALER_SCORES`.
6. **`06_DQ_cleanup.sql`**: Automated teardown scripts for database resetting and resource cleanup.

---

## Step 3: Data Quality Rules & Quality Gate Matrix

| Rule ID | Target Silver Table | DQ Dimension | Priority | Threshold / Validation Rule |
| :--- | :--- | :--- | :--- | :--- |
| `R_DLR_01` | `SILVER.MD_DEALERS` | Uniqueness | **HIGH** | `ID_DEALER_ID` duplicates across combined regions must be `0`. |
| `R_AST_01` | `SILVER.MD_VEHICLES` | Uniqueness | **HIGH** | Duplicate Asset ID count across combined regions must be `0`. |
| `R_AST_02` | `SILVER.MD_VEHICLES` | Validity | **HIGH** | `GN_VIN_NUMBER` matches 17-character VIN regex pattern. |
| `R_RO_01` | `SILVER.FCT_WORK_ORDERS` | Uniqueness | **HIGH** | Duplicate Work Order ID count across combined regions must be `0`. |
| `R_RO_02` | `SILVER.FCT_WORK_ORDERS` | Integrity | **HIGH** | Orphaned RO rate < `0.01%` (Asset FK must exist in `SILVER.MD_VEHICLES`). |
| `R_RO_03` | `SILVER.FCT_WORK_ORDERS` | Range | **HIGH** | `0 <= GN_MILEAGE_KM <= 500,000`. |
| `R_RO_04` | `SILVER.FCT_WORK_ORDERS` | Math Accuracy | **HIGH** | `AMT_TOTAL_AMOUNT = AMT_PARTS_COST + AMT_LABOR_COST`. |
| `R_RO_05` | `SILVER.FCT_WORK_ORDERS` | Consistency | **MEDIUM** | `DT_RO_CLOSED_DATE >= DT_RO_CREATED_DATE`. |

---

## Step 4: Model-Focused Data Marts (Gold Layer)

1. **`GOLD.MART_DEALER_RETENTION_FEATURES`** (Dealer Level / Monthly Snapshot)
   * Primary input vector for Dealer Risk & Retention Model.
   * Features: 12M/24M Service Retention Rate %, Out-of-Warranty Parc Count, Average Workshop Turnaround Time, Parts-to-Labor Ratio, Service Complaints Count (from `ST_SFDC_CASE`).

2. **`GOLD.MART_VEHICLE_LAPSE_FEATURES`** (Vehicle Level / Monthly Snapshot)
   * Primary input vector for predicting vehicle churn/lapse probability.
   * Features: Days Since Last Service, Last Service Mileage (KM), Warranty Expiration Status, Cross-Dealer Mobility Flag (`IS_SERVICED_AT_SELLING_DEALER`).

---

## Step 5: Machine Learning Handoff Contract (Snowflake-Native)

| Handoff Component | Target Object / Specification |
| :--- | :--- |
| **Input Feature Mart** | `DEALER_RISK_DB.GOLD.MART_DEALER_RETENTION_FEATURES` |
| **Partition Key** | `SCORING_MONTH` (Format: `YYYY-MM`) |
| **Snowflake Model Registry** | `DEALER_RISK_DB.ML_MART.DEALER_RISK_MODEL` |
| **Inference Stored Procedure** | `DEALER_RISK_DB.ML_MART.SP_EXECUTE_DEALER_SCORING(SCORING_MONTH)` |
| **Output Target Table** | `DEALER_RISK_DB.RISK_MART.DEALER_SCORES` |

### Target Schema (`DEALER_RISK_DB.RISK_MART.DEALER_SCORES`)

| Column Name | Data Type | Constraint | Description |
| :--- | :--- | :--- | :--- |
| `MODEL_RUN_ID` | STRING (UUID) | Mandatory | Unique ML execution run ID |
| `SCORING_MONTH` | STRING (YYYY-MM) | Mandatory | Target scoring period |
| `DEALER_ID` | STRING | Mandatory / PK | Primary Dealer Code |
| `BRAND_CODE` | STRING | Mandatory | Authorized Brand (`JEEP` / `CITROEN`) |
| `DEALER_RISK_SCORE` | DECIMAL(5,4) | Mandatory | Continuous risk probability `[0.0000 to 1.0000]` |
| `RISK_TIER` | STRING | Mandatory | Risk category (`HIGH`, `MEDIUM`, `LOW`) |
| `TOP_RISK_DRIVER` | STRING | Nullable | Primary feature driving the elevated risk score |
| `MODEL_VERSION` | STRING | Mandatory | Registered ML Model Version (e.g., `v1.2.0`) |
| `CREATED_AT` | TIMESTAMP | Mandatory | Record creation timestamp |