/*
=====================================================================================
Quality checks:
=====================================================================================
Script Purpose:
  This scripts performs quality checks to validate tyhe integrity,consistancy, and 
  accuracy of the Gold Layer.Thses checks ensures,
  - Uniqueness of surrogate keys in dimention tables
  - Referentials integrity between fact and dimentions tables
  - Validation of relationships in the data model for analytical purposes.

Usage Notes:

  - Run these checks after data loading Silver Layers
  - Investigate and resolve any discrepanies found during the tasks
=======================================================================================

*/
/*
-- ===============================================
-- Checks: gold.dim_customers 
-- ===============================================
*/
--DATA INTERGRATION
SELECT customer_id ,COUNT(*)FROM
SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE When ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- AS CRM is the master for gender info
		ELSE COALESCE (ca.gen,'n/a')
	END As new_gen
From Silver.crm_cust_info AS ci
Left join Silver.erp_cust_az12 AS ca
ON ci.cst_id = ca.CID
LEFT JOIN Silver.erp_loc_a101 AS la
ON ci.cst_key = la.CID)t
GROUP BY customer_id
HAVING COUNT(*) >1

Select * 
From gold.dim_customers

/*
-- ===============================================
-- Checks: gold.dim_products 
-- ===============================================
*/
SELECT prd_key, COUNT(*) from
(
Select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE
from Silver.crm_prd_info AS pn
LEFT JOIN Silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.ID
WHERE prd_end_dt IS NULL 
)t
GROUP By prd_key
HAVING COUNT(*)>1

--View check
SELECT *
from gold.dim_products
  
/*
-- ===============================================
-- Checks: gold.fact_sales 
-- ===============================================
*/
SELECT * FROM
gold.fact_sales

--Foreign Integrity (Dimentions): To find issues accross the table
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers as c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products as p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
