/*
===============================================================================
DDL SCRIPT: CREATE SILVER TABLES
===============================================================================

PURPOSE:
This script defines the Data Definition Language (DDL) for tables in the Silver 
layer. It includes drop statements to ensure clean recreation of tables and 
creates standardized structures for transformed data.

TABLES CREATED:
1. CRM Tables
   - crm_cust_info: Standardized customer information
   - crm_prd_info: Product catalog with categorization
   - crm_sls_details: Cleaned sales transaction data

2. ERP Tables
   - erp_cust_az12: Customer demographics
   - erp_loc_a101: Geographic location data
   - erp_px_cat_g1v2: Product categorization reference

KEY FEATURES:
- Drops existing tables before recreation
- Includes data type specifications
- Adds DWH create timestamp to all tables
- Uses standardized naming conventions
- Implements appropriate field lengths

COMMON COLUMNS:
All tables include:
- dwh_create_dt: Timestamp for DWH ingestion tracking

EXECUTION:
Run this script to:
- Drop existing Silver layer tables
- Create new table structures
- Reset the Silver layer schema

DEPENDENCIES:
- Requires 'silver' schema to exist
- Requires appropriate permissions for DROP and CREATE operations

===============================================================================
*/

DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'silver'
        AND table_name = 'crm_cust_info'
) THEN DROP TABLE silver.crm_cust_info;
END IF;
END $$;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Add a column for DWH create date
);

-- ==============================================================
-- Check if the table crm_prd_info exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'silver'
        AND table_name = 'crm_prd_info'
) THEN DROP TABLE silver.crm_prd_info;
END IF;
END $$;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_cat VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Add a column for DWH create date
);
-- ==============================================================
-- Check if the table crm_sls_details exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'silver'
        AND table_name = 'crm_sls_details'
) THEN DROP TABLE silver.crm_sls_details;
END IF;
END $$;
CREATE TABLE silver.crm_sls_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Add a column for DWH create date
);
-- ==============================================================
-- Check if the table erp_cust_az12 exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'silver'
        AND table_name = 'erp_cust_az12'
) THEN DROP TABLE silver.erp_cust_az12;
END IF;
END $$;
CREATE TABLE silver.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50),
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Add a column for DWH create date
);
-- ==============================================================
-- Check if the table erp_loc_a101 exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'silver'
        AND table_name = 'erp_loc_a101'
) THEN DROP TABLE silver.erp_loc_a101;
END IF;
END $$;
CREATE TABLE silver.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50),
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Add a column for DWH create date;
);

-- ==============================================================
-- Check if the table erp_loc_a101 exists and drop it if it does
-- ==============================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'silver'
        AND table_name = 'erp_px_cat_g1v2'
) THEN DROP TABLE silver.erp_px_cat_g1v2;
END IF;
END $$;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Add a column for DWH create date
);
