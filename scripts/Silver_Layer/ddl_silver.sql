/*
********************************************
DDL Script : Create Silver tables
********************************************
Script purpose:
	This scripts creates tables in the silver layer (Silver schema), dropping exsisting tables
 	if they already exsists.
	Run this script to re-define the DDL structure of 'bronze' Tables	
********************************************
*/
--Creating DDL for Silver Layer
IF OBJECT_ID('Silver.crm_cust_info','U') IS NOT NULL
	Drop Table Silver.crm_cust_info
CREATE TABLE Silver.crm_cust_info
(	cst_id INT ,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(10),
	cst_gndr VARCHAR(10),
	cst_create_date date,
	dwh_create_data DATETIME2 DEFAULT GETDATE()
);
IF OBJECT_ID('Silver.crm_prd_info','U') IS NOT NULL 
	Drop Table Silver.crm_prd_info
CREATE TABLE Silver.crm_prd_info
(	prd_id	INT,
	cat_id  VARCHAR(50), -- We are adjusting data after the data_transformation
	prd_key	VARCHAR(50),
	prd_nm	VARCHAR(100),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_data DATETIME2 DEFAULT GETDATE()

);

IF OBJECT_ID('Silver.crm_sales_details','U') IS NOT NULL 
	Drop Table Silver.crm_sales_details
CREATE TABLE Silver.crm_sales_details
(	sls_ord_num NVARCHAR(20),
	sls_prd_key	NVARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt DATE,
	sls_ship_dt	 DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity  INT,
	sls_price INT,
	dwh_create_data DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('Silver.erp_cust_az12','U') IS NOT NULL 
	Drop Table Silver.erp_cust_az12
CREATE TABLE Silver.erp_cust_az12
(	CID VARCHAR(50),
	BDATE DATE,
	GEN	Varchar(10),
	dwh_create_data DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('Silver.erp_loc_a101','U') IS NOT NULL 
	Drop Table Silver.erp_loc_a101
CREATE TABLE Silver.erp_loc_a101
(	CID VARCHAR(50),
	CNTRY Varchar(50),
	dwh_create_data DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('Silver.erp_px_cat_g1v2','U') IS NOT NULL 
	Drop Table Silver.erp_px_cat_g1v2
CREATE TABLE Silver.erp_px_cat_g1v2
(	ID VARCHAR(50),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(100),
	MAINTENANCE VARCHAR(50),
	dwh_create_data DATETIME2 DEFAULT GETDATE()
);


