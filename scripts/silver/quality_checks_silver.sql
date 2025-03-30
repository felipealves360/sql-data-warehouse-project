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

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Check for Nulls or duplicates in the primary key
-- Expectation: no results
SELECT cst_id,
	   COUNT( * )
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT( * )>1
	  OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: no results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname!=TRIM( cst_lastname );

-- Data standardization & consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Check for Nulls or duplicates in the primary key
-- Expectation: no results
SELECT prd_id,
	   COUNT( * )
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT( * )>1
	  OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: no results
SELECT
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS pr negative numbers
-- Expectation: no results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost<0
	  OR prd_cost IS NULL;

-- Data standadization & consistency (check distinct values for CASE statement)
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Checando a conexão entre a tabela de pedidos e produtos (quais produtos não possuem pedidos)
SELECT prd_id,
	   cat_id,
	   prd_key,
	   prd_nm,
	   prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt
FROM silver.crm_prd_info
WHERE SUBSTRING(prd_key,7,LEN(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sls_details)

-- Check for invalid data orders (end date < start date)
SELECT 
* 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- ====================================================================
-- Checking 'silver.crm_sls_details'
-- ====================================================================

-- Check for unwanted spaces
-- Expectation: no results
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM silver.crm_sls_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Check for invalid dates
SELECT
	sls_due_dt
FROM bronze.crm_sls_details
WHERE sls_due_dt < 0 
	OR LEN(sls_due_dt) != 8
	OR sls_due_dt > 20500101
	OR sls_due_dt < 19000101

-- Chek for invalid date orders
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM silver.crm_sls_details
WHERE sls_order_dt > sls_ship_dt 
	OR sls_order_dt > sls_due_dt

-- Check for negative numbers for sales infromation
-- Expectation: no results
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM silver.crm_sls_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Check for unwanted spaces
-- Expectation: no results
SELECT 
	*
FROM silver.erp_cust_az12
WHERE gen != TRIM(gen) 
	OR cid != TRIM(cid)

-- Check for NULLS pr negative numbers
-- Expectation: no results
SELECT 
	*
FROM silver.erp_cust_az12
WHERE cid IS NULL
	OR bdate IS NULL
	OR gen IS NULL

-- Checking conection btw erp_cust_az12 & silver.crm_cust_info after the transformation
-- Expectation: no results
SELECT 
	cid,
	CASE 
    	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
    	ELSE cid
    END AS cid,
	bdate,
	gen
FROM silver.erp_cust_az12
WHERE CASE 
    	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
    	ELSE cid
    END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- Check for invalid dates
-- Expectation: no results 
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' -- customers with 100 years old
	OR bdate > GETDATE( ) -- birthdate in the future

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Data standardization & consistency
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101
ORDER BY cntry

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check unwanted spaces
SELECT 
 id,
 cat,
 subcat,
 maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
	OR subcat != TRIM(subcat)
	OR maintenance != TRIM(maintenance)

-- Check data standardization & consitency
SELECT DISTINCT
	cat
FROM bronze.erp_px_cat_g1v2
