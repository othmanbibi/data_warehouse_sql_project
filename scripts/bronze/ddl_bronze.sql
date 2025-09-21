/*Bronze Layer

1 - Analysing : Interview Source System Experts
2 - Coding : Data Ingestion
3 - Data Completeness & Schema Checks
4 - Data Documentating, Versioning in GIT
*/

--1/Analyse Source systems

/* ================================
   Business Context & Ownership
   ================================ */
-- Who owns the data?
-- What business process does it support?
-- System & Data documentation
-- Data Model & Data Catalog


/* ================================
   Architecture & Technology Stack
   ================================ */
-- How is data stored? (SQL Server, Oracle, AWS, Azure, ...)
-- What are the integration capabilities? (API, Kafka, File Extract, Direct DB, ...)


/* ================================
   Extract & Load
   ================================ */
-- Incremental vs. Full Loads?
-- Data Scope & Historical Needs
-- What is the expected size of the extracts?
-- Are there any data volume limitations?
-- How to avoid impacting the source system's performance?
-- Authentication and authorization 
--   (tokens, SSH keys, VPN, IP whitelisting, ...)


--DDL : Data Definition Language defines the structure of database tables

--DATA PROFILING : Explore the data to identify column names and data types


/* ================================
   Bronze Rules - Naming Conventions
   ================================ */
-- All names must start with the source system name.
-- Table names must match their original names without renaming.

-- Format:
--   <sourcesystem>_<entity>

-- <sourcesystem> : Name of the source system (e.g., crm, erp).
-- <entity>       : Exact table name from the source system.

-- Example:
--   crm_customer_info  -->  Customer information from the CRM system


/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
USE DataWarehouse
GO

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO
