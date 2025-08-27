/*
*************************************************
DDL Script: Create Bronze Tables

Script Purpose :
  This script creates tables in the 'bronze' schema, dropping existing tables if the y already exists
  Run this script to re-define the DDL structure of 'bronze' Tables
**************************************************
*/

IF OBJECT_ID('Bronze.crm_cust_info','U') IS NOT NULL 
	Drop Table Bronze.crm_cust_info
CREATE TABLE Bronze.crm_cust_info
(	cst_id INT ,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(1),
	cst_gndr VARCHAR(1),
	cst_create_date date
	--constraint 
);
IF OBJECT_ID('Bronze.crm_prd_info','U') IS NOT NULL 
	Drop Table Bronze.crm_prd_info
CREATE TABLE Bronze.crm_prd_info
(	prd_id	INT,
	prd_key	VARCHAR(50),
	prd_nm	VARCHAR(100),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt datetime,
	prd_end_dt datetime
);

IF OBJECT_ID('Bronze.crm_sales_details','U') IS NOT NULL 
	Drop Table Bronze.crm_sales_details
CREATE TABLE Bronze.crm_sales_details
(	sls_ord_num VARCHAR(20),
	sls_prd_key	VARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt INT,
	sls_ship_dt	 INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity  INT,
	sls_price INT
);

IF OBJECT_ID('Bronze.erp_cust_az12','U') IS NOT NULL 
	Drop Table Bronze.erp_cust_az12
CREATE TABLE Bronze.erp_cust_az12
(	CID VARCHAR(50),
	BDATE date,
	GEN	Varchar(10)
);

IF OBJECT_ID('Bronze.erp_loc_a101','U') IS NOT NULL 
	Drop Table Bronze.erp_loc_a101
CREATE TABLE Bronze.erp_loc_a101
(	CID VARCHAR(50),
	CNTRY Varchar(50)
);

IF OBJECT_ID('Bronze.erp_px_cat_g1v2','U') IS NOT NULL 
	Drop Table Bronze.erp_px_cat_g1v2
CREATE TABLE Bronze.erp_px_cat_g1v2
(	ID VARCHAR(50),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(100),
	MAINTENANCE VARCHAR(50)
);


