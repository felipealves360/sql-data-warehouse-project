/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ==============================================================
-- Check if the table crm_cust_info exists and drop it if it does
-- ==============================================================

-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    cst_id,
    count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL
    OR count(*) > 1;

-- Check fro unwanted spaces
-- Expectation: no results 
SELECT 
    cst_firstname,
    cst_lastname,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
    OR cst_lastname != TRIM(cst_lastname);

-- Data standardization & consistensy
-- Expectation: no results
SELECT distinct
    cst_marital_status
FROM silver.crm_cust_info;

-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    *
FROM
    (SELECT 
        *,
        row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM silver.crm_cust_info
    ) AS t
WHERE flag_last > 1
    OR cst_id IS NULL;

-- Check for errors in connectivity with the erp_loc_a101 table
-- Expectation: no results
SELECT
cst_id
FROM silver.crm_cust_info
WHERE cst_id NOT IN (SELECT 
    sls_cust_id
FROM silver.crm_sls_details);

-- ==============================================================
-- Check if the table crm_prd_info exists and drop it if it does
-- ==============================================================
-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    prd_id,
    count(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1
    OR prd_id IS NULL;

-- Check fro unwanted spaces
-- Expectation: no results
SELECT 
    prd_key,
    prd_nm
FROM silver.crm_prd_info
WHERE prd_key != TRIM(prd_key)
    OR prd_nm != TRIM(prd_nm);

-- Check for inconsistent costs values
-- Expectation: no results
SELECT distinct
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <= 0
    OR prd_cost IS NULL

-- Check for inconsistent data on prd_line
-- Expectation: qualifyed results
SELECT distinct
    prd_line
FROM silver.crm_prd_info


-- Check for inconsistent data on prd_start_dt and prd_end_dt
-- Expectation: no results
SELECT
    *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- Check for errors in connectivity with the erp_loc_a101 table
-- Expectation: only prd_key of products that were not sold
SELECT
prd_key
FROM silver.crm_prd_info
WHERE prd_key NOT IN (SELECT 
    sls_prd_key
FROM silver.crm_sls_details);

-- ==============================================================
-- Check if the table crm_sls_details exists and drop it if it does
-- ==============================================================

-- Check for NULLS
-- Expectation: no results
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id
FROM silver.crm_sls_details
WHERE sls_ord_num IS NULL
    OR sls_prd_key IS NULL

-- Check for NULLSy
-- Expectation: no results
SELECT
    sls_order_dt
FROM silver.crm_sls_details
WHERE sls_order_dt IS NULL
    OR sls_order_dt < '1900-01-01'
    OR sls_order_dt > '2100-12-31'

-- Check for inconsistent data
-- Expectation: no results
SELECT
    sls_ord_num,
    sls_prd_key
FROM silver.crm_sls_details
WHERE sls_ord_num != TRIM(sls_ord_num)
    OR sls_prd_key != TRIM(sls_prd_key)


-- Check for inconsistent data on sls_sales, sls_quantity and sls_price
-- Expectation: no results
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sls_details
WHERE sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL

-- ==============================================================
-- Checking quality of the data in the erp_cust_az12 table
-- ==============================================================

-- Check DISTINCT data
SELECT distinct
cid
FROM silver.erp_cust_az12

SELECT distinct
gen
FROM silver.erp_cust_az12

-- Check for TRIM errors in the cid column
-- Expectation: no results
SELECT
cid
from silver.erp_cust_az12
WHERE cid != TRIM(cid)

-- Check for errors in connectivity with the erp_loc_a101 table
-- Expectation: no results
SELECT
    cid
FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT
    cst_key
FROM silver.crm_cust_info)

-- Check for DATE errors in the bdate column
-- Expectation: NULL results
SELECT
    bdate
FROM silver.erp_cust_az12
WHERE bdate IS NULL
    OR bdate < '1900-01-01'
    OR bdate > '2100-12-31'

-- ==============================================================
-- Checking quality of the data in the erp_loc_a101 table
-- ==============================================================

-- Check for TRIM errors in the cid column
-- Expectation: no results
SELECT
    cid
FROM silver.erp_loc_a101
WHERE cid != TRIM(cid)

SELECT
    cntry
FROM silver.erp_loc_a101
WHERE cntry != TRIM(cntry)


-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    cid,
    cntry
FROM silver.erp_loc_a101
WHERE cid IS NULL
    OR cntry IS NULL

-- Chek for inconsistent data on cntry
-- Expectation: qualified results
SELECT DISTINCT
    cntry
FROM silver.erp_loc_a101
