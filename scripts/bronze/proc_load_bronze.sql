/*
===============================================================================
BRONZE LAYER LOADING PROCEDURE
===============================================================================

PURPOSE:
This PostgreSQL stored procedure (load_bronze) performs initial data loading from 
external CSV files into the Bronze layer tables. It represents the first stage 
of the ETL process, bringing raw data into the data warehouse.

TABLES PROCESSED:
1. CRM Tables
   - crm_cust_info: Customer information from CRM system
   - crm_prd_info: Product information and catalog
   - crm_sls_details: Sales transaction details

2. ERP Tables
   - erp_cust_az12: Customer data from ERP system
   - erp_loc_a101: Location and geography information
   - erp_px_cat_g1v2: Product categorization data

KEY FEATURES:
- Truncates destination tables before loading
- Uses COPY command for efficient bulk loading
- Handles CSV files with headers
- Uses UTF-8 encoding for international character support
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

FILE REQUIREMENTS:
- CSV files must be placed in specified directories
- Files must have headers
- Files must use comma as delimiter
- Files must use UTF-8 encoding

USAGE:
CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze() AS $$
DECLARE start_time TIMESTAMP;
        end_time TIMESTAMP;
        batch_start_time TIMESTAMP;
        batch_end_time TIMESTAMP;
BEGIN batch_start_time := clock_timestamp();
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading data into Bronze Layer';
    RAISE NOTICE '====================================================';
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '====================================================';
    -- Load bronze.crm_cust_info
    start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM 'C:/Users/felip/OneDrive/1 Projects/2025-03 SQL Data Wherehouse Project/datasets/source_crm/cust_info.csv' WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER ',',
                ENCODING 'UTF8'
            );
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '----------------------------------------------------';

    -- Load bronze.crm_prd_info
    start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM 'C:/Users/felip/OneDrive/1 Projects/2025-03 SQL Data Wherehouse Project/datasets/source_crm/prd_info.csv' WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER ',',
                ENCODING 'UTF8'
            );
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(
        epoch
        FROM (end_time - start_time)
    );
    RAISE NOTICE '----------------------------------------------------';

    -- Load bronze.crm_sls_details
    start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_sls_details;
        COPY bronze.crm_sls_details
        FROM 'C:/Users/felip/OneDrive/1 Projects/2025-03 SQL Data Wherehouse Project/datasets/source_crm/sales_details.csv' WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER ',',
                ENCODING 'UTF8'
            );
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(
        epoch
        FROM (end_time - start_time)
    );

    RAISE NOTICE '----------------------------------------------------';
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '====================================================';

    -- Load bronze.erp_cust_az12
    start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM 'C:/Users/felip/OneDrive/1 Projects/2025-03 SQL Data Wherehouse Project/datasets/source_erp/CUST_AZ12.csv' WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER ',',
                ENCODING 'UTF8'
            );
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(
        epoch
        FROM (end_time - start_time)
    );
    RAISE NOTICE '----------------------------------------------------';

    -- Load bronze.erp_loc_a101
    start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM 'C:/Users/felip/OneDrive/1 Projects/2025-03 SQL Data Wherehouse Project/datasets/source_erp/LOC_A101.csv' WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER ',',
                ENCODING 'UTF8'
            );
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(
        epoch
        FROM (end_time - start_time)
    );
    RAISE NOTICE '----------------------------------------------------';

    -- Load bronze.erp_px_cat_g1v2
    start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM 'C:/Users/felip/OneDrive/1 Projects/2025-03 SQL Data Wherehouse Project/datasets/source_erp/PX_CAT_G1V2.csv' WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER ',',
                ENCODING 'UTF8'
            );
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load duration: % seconds',
    EXTRACT(epoch FROM (end_time - start_time));
    
    RAISE NOTICE '----------------------------------------------------';
    batch_end_time := clock_timestamp();
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Total batch duration: % seconds',
    EXTRACT(
        epoch
        FROM (batch_end_time - batch_start_time)
    );
    RAISE NOTICE '====================================================';

EXCEPTION
    WHEN OTHERS THEN RAISE NOTICE '=============================================';
    RAISE NOTICE 'An error occurred: %',
    SQLERRM;
    RAISE NOTICE '=============================================';
END;
$$ LANGUAGE plpgsql;
