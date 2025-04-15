/*
===============================================================================
SILVER LAYER LOADING PROCEDURE
===============================================================================

PURPOSE:
This PostgreSQL stored procedure (load_silver) performs ETL (Extract, Transform, 
Load) operations to populate tables in the Silver layer from the Bronze layer. 
It handles data cleansing, standardization, and basic transformations.

TABLES PROCESSED:
1. CRM Tables
   - crm_cust_info: Customer information with standardized gender and marital status
   - crm_prd_info: Product information with categorization and date ranges
   - crm_sls_details: Sales details with date formatting and sales calculations

2. ERP Tables
   - erp_cust_az12: Customer data with cleaned IDs and standardized gender
   - erp_loc_a101: Location data with standardized country names
   - erp_px_cat_g1v2: Product categorization information

KEY FEATURES:
- Truncates destination tables before loading
- Performs data cleansing and standardization
- Handles NULL values and data type conversions
- Includes error handling and detailed logging
- Reports processing duration for each table and total batch

OUTPUT:
The procedure provides detailed logging including:
- Start and end of each table load
- Duration of individual table loads
- Total batch processing time
- Any errors that occur during execution

ERROR HANDLING:
If an error occurs, the procedure will:
- Catch the exception
- Log the error details
- Display the error message with clear formatting

USAGE:
CALL silver.load_silver();
===============================================================================
*/

CREATE OR replace PROCEDURE silver.load_silver() AS $$
DECLARE start_time TIMESTAMP;
        end_time TIMESTAMP;
        batch_start_time TIMESTAMP;
        batch_end_time TIMESTAMP;
BEGIN batch_start_time := clock_timestamp();
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'LOADING DATA INTO SILVER LAYER';
    RAISE NOTICE '====================================================';
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '====================================================';
    -- Load silver.crm_cust_info
    start_time := clock_timestamp();
        TRUNCATE TABLE silver.crm_cust_info;
        INSERT INTO silver.crm_cust_info
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE UPPER(TRIM(cst_marital_status)) 
                WHEN 'M' THEN 'Married'
                WHEN 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT 
                *,
                row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
            ) AS t
        WHERE flag_last = 1;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '----------------------------------------------------';

    -- Load silver.crm_prd_info
    start_time := clock_timestamp();
        TRUNCATE TABLE silver.crm_prd_info;
        INSERT INTO silver.crm_prd_info
        (
            prd_id,
            prd_cat,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            LEFT(prd_key, 7) AS prd_cat,
            SUBSTRING(prd_key from 7 for char_length(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'Montain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '----------------------------------------------------';

    -- Load silver.crm_sls_details
    start_time := clock_timestamp();
        TRUNCATE TABLE silver.crm_sls_details;
        INSERT INTO silver.crm_sls_details
        (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            case    
                when LENGTH(sls_order_dt::text) != 8 OR sls_order_dt = 0 then NULL
                else CAST(CAST(sls_order_dt AS VARCHAR) as DATE)
            end as sls_order_dt,
            case    
                when LENGTH(sls_ship_dt::text) != 8 OR sls_ship_dt = 0 then NULL
                else CAST(CAST(sls_ship_dt AS VARCHAR) as DATE)
            end as sls_ship_dt,
            case    
                when LENGTH(sls_due_dt::text) != 8 OR sls_due_dt = 0 then NULL
                else CAST(CAST(sls_due_dt AS VARCHAR) as DATE)
            end as sls_due_dt,
            case
                when sls_sales IS NULL OR sls_sales <= 0 then sls_quantity * sls_price
                else sls_sales
            end as sls_sales,
            case
                when sls_quantity IS NULL OR sls_quantity <= 0 then sls_sales / sls_price
                else sls_quantity
            end as sls_quantity,
            case
                when sls_price IS NULL OR sls_price <= 0 then sls_sales / sls_quantity
                else sls_price
            end as sls_price
        FROM bronze.crm_sls_details;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '----------------------------------------------------';

    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '====================================================';
    -- Load silver.erp_cust_az12
    start_time := clock_timestamp();
        TRUNCATE TABLE silver.erp_cust_az12;
        INSERT INTO silver.erp_cust_az12
        (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN SUBSTRING(cid FROM 1 for 3) like 'NAS%' THEN SUBSTRING(cid from 4 for char_length(cid))
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > CURRENT_DATE THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) = 'F' OR gen = 'Female' THEN 'Female'
                WHEN UPPER(TRIM(gen)) = 'M' OR gen = 'Male' THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '----------------------------------------------------';

    -- Load silver.erp_loc_a101
    start_time := clock_timestamp();
        TRUNCATE TABLE silver.erp_loc_a101;
        INSERT INTO silver.erp_loc_a101
        (
            cid,
            cntry
        )
        SELECT
            TRIM(REPLACE(cid, '-', '')) AS cid,
            CASE
                WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '----------------------------------------------------';

    -- Load silver.erp_px_cat_g1v2
    start_time := clock_timestamp();
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        INSERT INTO silver.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '----------------------------------------------------';

    batch_end_time := clock_timestamp();
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Total batch duration: % seconds',
    EXTRACT(epoch FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '====================================================';

    EXCEPTION
        WHEN OTHERS THEN RAISE NOTICE '=============================================';
        RAISE NOTICE 'Error in the load_silver procedure: %', SQLERRM;
        RAISE NOTICE '====================================================';
        
END;
$$ LANGUAGE plpgsql;
