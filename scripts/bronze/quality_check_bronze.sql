-- ==============================================================
-- Checking quality of the data in the bronze.crm_cust_info table
-- ==============================================================

-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    cst_id,
    count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL
    OR count(*) > 1 

-- Check fro unwanted spaces
-- Expectation: no results 
SELECT 
    cst_firstname,
    cst_lastname,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
    OR cst_lastname != TRIM(cst_lastname);

-- Data standardization & consistensy
-- Expectation: no results
SELECT distinct
    cst_gndr
FROM bronze.crm_cust_info

-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    *
FROM
    (SELECT 
        *,
        row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    ) AS t
WHERE flag_last > 1
    OR cst_id IS NULL

-- ==============================================================
-- Checking quality of the data in the bronze.crm_prd_info table
-- ==============================================================

-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    prd_id,
    count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1
    OR prd_id IS NULL

-- Check fro unwanted spaces
-- Expectation: no results
SELECT 
    prd_key,
    prd_nm
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key)
    OR prd_nm != TRIM(prd_nm);

-- Check for inconsistent costs values
-- Expectation: no results
SELECT distinct
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <= 0
    OR prd_cost IS NULL

-- Check for inconsistent data on prd_line
-- Expectation: no results
SELECT distinct
    prd_key
FROM bronze.crm_prd_info
WHERE prd_key like '%BK-R93R-62'

-- Check for inconsistent data on prd_start_dt and prd_end_dt
-- Expectation: no results
SELECT
    *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt
    

-- ==============================================================
-- Checking quality of the data in the bronze.crm_sls_details table
-- ==============================================================

-- Check for NULLS
-- Expectation: no results
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id
FROM bronze.crm_sls_details
WHERE sls_ord_num IS NULL
    OR sls_prd_key IS NULL

-- Check for NULLSy
-- Expectation: no results
SELECT
    sls_order_dt
FROM bronze.crm_sls_details
WHERE sls_order_dt IS NULL
    OR sls_order_dt = 0
    OR LENGTH(sls_order_dt::text) != 8
    OR sls_order_dt < 19000101
    OR sls_order_dt > 21001231

-- Check for inconsistent data
-- Expectation: no results
SELECT
    sls_ord_num,
    sls_prd_key
FROM bronze.crm_sls_details
WHERE sls_ord_num != TRIM(sls_ord_num)
    OR sls_prd_key != TRIM(sls_prd_key)


-- Check for inconsistent data on sls_sales, sls_quantity and sls_price
-- Expectation: no results
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sls_details
WHERE sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL

-- ==============================================================
-- Checking quality of the data in the erp_cust_az12 table
-- ==============================================================

SELECT
    CASE
        WHEN SUBSTRING(cid FROM 1 for 3) like 'NAS%' THEN SUBSTRING(cid from 4 for char_length(cid))
        ELSE cid
    END AS cid,
    bdate,
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate,
    CASE
        WHEN UPPER(TRIM(gen)) = 'F' OR gen = 'Female' THEN 'Female'
        WHEN UPPER(TRIM(gen)) = 'M' OR gen = 'Male' THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12

-- Check DISTINCT data
SELECT distinct
cid,
FROM bronze.erp_cust_az12

SELECT distinct
gen
FROM bronze.erp_cust_az12

-- Check for TRIM errors in the cid column
-- Expectation: no results
SELECT
cid
from bronze.erp_cust_az12
WHERE cid != TRIM(cid)

-- Check for errors in connectivity with the erp_loc_a101 table
-- Expectation: no results
SELECT
    CASE
        WHEN SUBSTRING(cid FROM 1 for 3) like 'NAS%' THEN SUBSTRING(cid from 4 for char_length(cid))
        ELSE cid
    END AS cid
FROM bronze.erp_cust_az12
WHERE CASE
        WHEN SUBSTRING(cid FROM 1 for 3) like 'NAS%' THEN SUBSTRING(cid from 4 for char_length(cid))
        ELSE cid
    END NOT IN (SELECT
    cst_key
FROM silver.crm_cust_info)

-- Check for DATE errors in the bdate column
-- Expectation: no results
SELECT
    bdate
FROM bronze.erp_cust_az12
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
FROM bronze.erp_loc_a101
WHERE cid != TRIM(cid)

SELECT
    cntry
FROM bronze.erp_loc_a101
WHERE cntry != TRIM(cntry)


-- Check for NULLS or duplicates in primary key
-- Expectation: no results
SELECT
    cid,
    cntry
FROM bronze.erp_loc_a101
WHERE cid IS NULL
    OR cntry IS NULL

-- Chek for inconsistent data on cntry
-- Expectation: no results
SELECT DISTINCT
    cntry
FROM bronze.erp_loc_a101

-- ==============================================================
-- Checking quality of the data in the erp_px_cat_g1v2 table
-- ==============================================================

SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2

-- Check for TRIM errors in the id, cat, subcat and maintenance columns
-- Expectation: no results
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id)
    OR cat != TRIM(cat)
    OR subcat != TRIM(subcat)
    OR maintenance != TRIM(maintenance)

-- Check for inconsistent data on id, cat, subcat and maintenance
-- Expectation: only qualified results
SELECT DISTINCT
    maintenance
FROM bronze.erp_px_cat_g1v2

SELECT
    id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT 
    prd_cat
FROM silver.crm_prd_info)
