/*
=============================================================================
DDL Script: Create Gold views
=============================================================================
Script purprose:
    - Create views for the Gold layer of the data warehouse.
    - The Gold layer is used for reporting and analytics.
    - The views are created from the Silver layer tables.
    - The views are used to create a star schema for reporting.

=============================================================================
*/

-- ============================================================
-- Create dimension tables for customers
-- ============================================================
CREATE VIEW gold.dim_customer AS
SELECT
    row_number() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS created_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca 
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;

-- ============================================================
-- Create dimension tables for products
-- ============================================================
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.prd_cat AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pn.prd_cat = pc.id
WHERE pn.prd_end_dt IS NULL; -- filter out discontinued products

-- ============================================================
-- Create fact tables for sales
-- ============================================================
CREATE VIEW gold.fac_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_price * sls_quantity AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sls_details AS sd
LEFT JOIN gold.dim_products AS pr 
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer AS cu 
    ON sd.sls_cust_id = cu.customer_id;

