/* 
These comments are just to explain why my code is like this,
I am following data with baraa project tutorial

Tried using stored procedure, too much work on mysql,
so for now i will leave it like this as it is a project as i am still learining.

i know i can use CLI or python or making a logging table
but i am not going to do that right now as i do not need to

MySQL doesn't have a PRINT, so i tried using SELECT and it did not have the intended effect
so I will just leave it here for now.

TRY CATCH also doesnt work

I only chose to write the part for one load duration because i know i am going to have difficulties with the message
 */

SET @start_time = NOW();
SET @end_time = NULL;

SELECT '===========================' AS Message;
SELECT 'Loading Bronze Layer' AS Message;
SELECT '===========================' AS Message;

SELECT '---------------------------' AS Message;
SELECT 'Loading CRM TABLES' AS Message;
SELECT '---------------------------' AS Message;

SET @start_time = NOW();
SELECT '>> Truncating Table: bronze.crm_cust_info' AS Message;
TRUNCATE TABLE bronze.crm_cust_info;
SELECT '>> Inserting Data Into: bronze.crm_cust_info' AS Message;
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
SET @start_time = NOW();
SELECT '>> Load Duration: ' + CAST(TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS CHAR) + ' seconds' AS Message;

SELECT '>> Truncating Table: bronze.crm_prd_info' AS Message;
TRUNCATE TABLE bronze.crm_prd_info;
SELECT '>> Inserting Data Into: bronze.crm_prd_info' AS Message;
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT '>> Truncating Table: bronze.crm_sales_details' AS Message;
TRUNCATE TABLE bronze.crm_sales_details;
SELECT '>> Inserting Data Into: bronze.crm_sales_details' AS Message;
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT '---------------------------' AS Message;
SELECT 'Loading ERP TABLES' AS Message;
SELECT '---------------------------' AS Message;

SELECT '>> Truncating Table: bronze.erp_cust_az12' AS Message;
TRUNCATE TABLE bronze.erp_cust_az12;
SELECT '>> Inserting Data Into: bronze.erp_cust_az12' AS Message;
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT '>> Truncating Table: bronze.erp_loc_a101' AS Message;
TRUNCATE TABLE bronze.erp_loc_a101;
SELECT '>> Inserting Data Into: bronze.erp_loc_a101' AS Message;
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT '>> Truncating Table: bronze.erp_px_cap_g1v2' AS Message;
TRUNCATE TABLE bronze.erp_px_cap_g1v2;
SELECT '>> Inserting Data Into: bronze.erp_px_cap_g1v2' AS Message;
LOAD DATA LOCAL INFILE
'/Users/vedanshnikum/Documents/sql-data-warehouse-project-main/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cap_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
