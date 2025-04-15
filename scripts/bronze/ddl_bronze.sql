/*
===============================================================================
DDL SCRIPT: CREATE BRONZE TABLES
===============================================================================

PURPOSE:
This script defines the Data Definition Language (DDL) for tables in the Bronze 
layer. It creates the initial landing zone for raw data ingestion from source 
systems, maintaining the original data structure.

TABLES CREATED:
1. CRM Tables
   - crm_cust_info: Raw customer information
   - crm_prd_info: Raw product catalog data
   - crm_sls_details: Raw sales transactions

2. ERP Tables
   - erp_cust_az12: Raw customer data from ERP
   - erp_loc_a101: Raw location information
   - erp_px_cat_g1v2: Raw product categorization

KEY FEATURES:
- Drops existing tables before recreation
- Preserves source data types
- Maintains original field names
- Uses consistent VARCHAR lengths
- Implements basic data structures

TABLE STRUCTURE:
- CRM tables maintain source system naming
- ERP tables follow source format
- All VARCHAR fields standardized to 50 chars
- Date fields preserved as source types
- Numeric fields maintained as INTs

EXECUTION:
Run this script to:
- Drop existing Bronze layer tables
- Create new table structures
- Reset the Bronze layer schema

DEPENDENCIES:
- Requires 'bronze' schema to exist
- Requires appropriate permissions for DROP and CREATE operations

===============================================================================
*/

DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
        AND table_name = 'crm_cust_info'
) THEN DROP TABLE bronze.crm_cust_info;
END IF;
END $$;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

-- ==============================================================
-- Check if the table crm_prd_info exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
        AND table_name = 'crm_prd_info'
) THEN DROP TABLE bronze.crm_prd_info;
END IF;
END $$;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt TIMESTAMP
);
-- ==============================================================
-- Check if the table crm_sls_details exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
        AND table_name = 'crm_sls_details'
) THEN DROP TABLE bronze.crm_sls_details;
END IF;
END $$;
CREATE TABLE bronze.crm_sls_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
-- ==============================================================
-- Check if the table erp_cust_az12 exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
        AND table_name = 'erp_cust_az12'
) THEN DROP TABLE bronze.erp_cust_az12;
END IF;
END $$;
CREATE TABLE bronze.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);
-- ==============================================================
-- Check if the table erp_loc_a101 exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
        AND table_name = 'erp_loc_a101'
) THEN DROP TABLE bronze.erp_loc_a101;
END IF;
END $$;
CREATE TABLE bronze.erp_loc_a101 (cid VARCHAR(50), cntry VARCHAR(50));
-- ==============================================================
-- Check if the table erp_loc_a101 exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
        AND table_name = 'erp_px_cat_g1v2'
) THEN DROP TABLE bronze.erp_px_cat_g1v2;
END IF;
END $$;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);
