--- =================================================================================
-- Purpose: Validates referential integrity between fact and dimension tables by identifying orphaned foreign keys
-- in the sales fact table that don't have corresponding records in customer or product dimension tables.
--- =================================================================================

SELECT
    *
FROM gold.fac_sales AS f
LEFT JOIN gold.dim_customer AS c 
    ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products AS p 
    ON f.product_key = p.product_key
WHERE f.customer_key NOT IN (SELECT customer_key FROM gold.dim_customer)
    OR f.product_key NOT IN (SELECT product_key FROM gold.dim_products);
