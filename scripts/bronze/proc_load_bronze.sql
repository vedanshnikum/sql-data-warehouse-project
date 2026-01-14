/*
===============================================================================
Bronze Layer Load Script (Source -> Bronze)
===============================================================================
Purpose:
- Truncate bronze tables
- Load raw CSV data into bronze schema
- Normalize empty strings
- Fix Windows line endings (\r\n)
===============================================================================
*/

-- ---------------------------------------------------------------------------
-- Logging messages
-- ---------------------------------------------------------------------------
-- ===========================
-- Loading Bronze Layer
-- ===========================

-- ---------------------------
-- Loading CRM TABLES
-- ---------------------------

-- ---------------------------------------------------------------------------
-- CRM CUSTOMER INFO
-- ---------------------------------------------------------------------------
-- >> Truncating Table: bronze.crm_cust_info
TRUNCATE TABLE bronze.crm_cust_info;

-- >> Inserting Data Into: bronze.crm_cust_info
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @cst_id,
    @cst_key,
    @cst_firstname,
    @cst_lastname,
    @cst_marital_status,
    @cst_gndr,
    @cst_create_date
)
SET
    cst_id              = NULLIF(@cst_id, ''),
    cst_key             = NULLIF(@cst_key, ''),
    cst_firstname       = NULLIF(@cst_firstname, ''),
    cst_lastname        = NULLIF(@cst_lastname, ''),
    cst_marital_status  = NULLIF(@cst_marital_status, ''),
    cst_gndr            = NULLIF(REPLACE(@cst_gndr, '\r', ''), ''),
    cst_create_date     = NULLIF(@cst_create_date, '');

-- ---------------------------------------------------------------------------
-- CRM PRODUCT INFO
-- ---------------------------------------------------------------------------
-- >> Truncating Table: bronze.crm_prd_info
TRUNCATE TABLE bronze.crm_prd_info;

-- >> Inserting Data Into: bronze.crm_prd_info
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @prd_id,
    @prd_key,
    @prd_nm,
    @prd_cost,
    @prd_line,
    @prd_start_dt,
    @prd_end_dt
)
SET
    prd_id       = NULLIF(@prd_id, ''),
    prd_key      = NULLIF(@prd_key, ''),
    prd_nm       = NULLIF(@prd_nm, ''),
    prd_cost     = IFNULL(NULLIF(@prd_cost, ''), 0),
    prd_line     = NULLIF(REPLACE(@prd_line, '\r', ''), ''),
    prd_start_dt = NULLIF(@prd_start_dt, ''),
    prd_end_dt   = NULLIF(@prd_end_dt, '');

-- ---------------------------------------------------------------------------
-- CRM SALES DETAILS
-- ---------------------------------------------------------------------------
-- >> Truncating Table: bronze.crm_sales_details
TRUNCATE TABLE bronze.crm_sales_details;

-- >> Inserting Data Into: bronze.crm_sales_details
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- ---------------------------------------------------------------------------
-- ERP TABLES
-- ---------------------------------------------------------------------------
-- ---------------------------
-- Loading ERP TABLES
-- ---------------------------

-- ---------------------------------------------------------------------------
-- ERP CUSTOMER AZ12
-- ---------------------------------------------------------------------------
-- >> Truncating Table: bronze.erp_cust_az12
TRUNCATE TABLE bronze.erp_cust_az12;

-- >> Inserting Data Into: bronze.erp_cust_az12
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(
    @cid,
    @bdate,
    @gen
)
SET
    cid   = NULLIF(@cid, ''),
    bdate = NULLIF(@bdate, ''),
    gen   = NULLIF(REPLACE(@gen, '\r', ''), '');

-- ---------------------------------------------------------------------------
-- ERP LOCATION A101
-- ---------------------------------------------------------------------------
-- >> Truncating Table: bronze.erp_loc_a101
TRUNCATE TABLE bronze.erp_loc_a101;

-- >> Inserting Data Into: bronze.erp_loc_a101
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- ---------------------------------------------------------------------------
-- ERP PRODUCT CATEGORY
-- ---------------------------------------------------------------------------
-- >> Truncating Table: bronze.erp_px_cat_g1v2
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

-- >> Inserting Data Into: bronze.erp_px_cat_g1v2
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- ---------------------------------------------------------------------------
-- DONE
-- ---------------------------------------------------------------------------
-- ===========================
-- Bronze Load Completed
-- ===========================