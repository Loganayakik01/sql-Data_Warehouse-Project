/*
****************************************************************************************
DDL Scripts : Create Gold views
****************************************************************************************
Scripts purpose:
  This script creates views for the gold layer in the Data warehouse.
  The Gold Layer represents the final dimentions annd fact tables (star-schema)

  Each view performs transformations and combines data from the Silver layer to produce
  clean,enriched and business-ready dataset.
Usage:
  -These views can be quried directly for analytics and reporting
*/
/*
****************************************
--Create Dimention : gold.dim_customers
****************************************
*/
--USE Datawarehouse
Create View gold.dim_customers AS --Create Object
Select 
	ROW_NUMBER()over(order by ci.cst_id ) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	ci.cst_marital_status AS marital_status,
	CASE When ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- AS CRM is the master for gender info
		ELSE COALESCE (ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date

From Silver.crm_cust_info AS ci
Left join Silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.CID
LEFT JOIN Silver.erp_loc_a101 AS la
ON ci.cst_key = la.CID

/*
****************************************
--Create Dimention : gold.dim_products
****************************************
*/
CREATE VIEW gold.dim_products AS--Creating the view:
Select 
	ROW_NUMBER()over(ORDER BY pn.prd_key,pn.prd_start_dt) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.CAT AS category,
	pc.SUBCAT AS subcategory,
	pc.MAINTENANCE ,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
	--pn.prd_end_dt,	
from Silver.crm_prd_info AS pn
LEFT JOIN Silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.ID
WHERE prd_end_dt IS NULL  -- Gives the currect recored and no historical data 
/*
****************************************
--Create Dimention : gold.fact_sales
****************************************
*/
CREATE VIEW gold.fact_sales AS
Select 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price

FROM Silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
