/*
=====================================================================
Quality Checks
=====================================================================
Srcipt Purpose:
  This scripts performs various quality checks for data consistancy,accuracy, and 
  Standardization accross the 'Silver' schemas. It includes chhecks for:
  --Null or duplicates primary keys.
  --Unwanted spaces in string feilds
  --Data standardizatition and consistancy
  --Invalid data ranges and orders
  -- Data consistancy between related feilds.
Usage Notes:
  --Run these checks after data loading silver layer.
  --Investigate and resolve any discrepancies found during the checks.
*/
/*
*********************************
crm_sales_details
**********************************
*/
--Checking the quality of date:
Select 
  NullIf(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 or len(sls_order_dt) != 8 or sls_order_dt > 20500101 or sls_order_dt < 19000101
--Check for invalid date orders
Select * 
from  bronze.crm_sales_details
Where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
--Business rule check
Select 
  sls_sales,
  sls_quantity,
  sls_price
  FROM bronze.crm_sales_details
  Where sls_sales != sls_quantity * sls_price
Or sls_sales IS NULL or sls_quantity IS NULL or sls_price IS NULL
Or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price
  
--Checking the table 
Select * from Silver.crm_sales_details

/*
*********************************
crm_prd_info
**********************************
*/
--Checks
Select 
prd_id,
Count(*)
from Bronze.crm_prd_info
group by prd_id
HAVING count(*) > 1 or prd_id IS NULL

Select prd_nm
from Bronze.crm_prd_info
Where prd_nm != TRIM(prd_nm)

--Check for NULLs or Negative number
Select prd_cost
from Bronze.crm_prd_info
Where prd_cost <0 or prd_cost IS NULL

--Select distinct id from bronze.erp_px_cat_g1v2
  
Select distinct 
prd_line
from Bronze.crm_prd_info

Select *
from Bronze.crm_prd_info
Where prd_end_dt < prd_start_dt
Select 
Count(*) 
from Silver.crm_prd_info
 
--Checking the table 
Select count(*) from Bronze.crm_prd_info

/*
*********************************
crm_prd_info
**********************************
*/
--Check for nulls or duplicates in primary key
--expectation : No result
--Checking the quality of data
--USE Datawarehouse
SELECT 
	cst_id,
	count(*)
	From bronze.crm_cust_info
	Group by cst_id
Having count(*) >1 or cst_id Is NULL

--Tranformations
Select *
From(
SELECT 
*,
ROW_NUMBER()over(partition by cst_id order by cst_create_date DESC) as FlagLast
From bronze.crm_cust_info
--WHERE cst_id = 29466  -- check for the newst value creation for this duplicated value using time and date created
)t 
Where FlagLast = 1

 --For next string coloumns
 --Expectation is no results 
 SELECT 
 cst_firstname ,--check for all columns with string
 from bronze.crm_cust_info
 Where cst_firstname != Trim(cst_firstname)

 --Data standardzation and Consistancy
 SELECT DISTINCT 
 cst_gndr
 from bronze.crm_cust_info

--Checks for transformation data
SELECT 
	cst_id,
	count(*)
	From silver.crm_cust_info
	Group by cst_id
Having count(*) >1 or cst_id Is NULL

 --For next string coloumns
 --Expectation is no results 
 SELECT 
 cst_firstname --check for all columns with string
 from silver.crm_cust_info
 Where cst_firstname != Trim(cst_firstname)

 --Data standardzation and Consistancy
 SELECT DISTINCT 
 cst_gndr
 from silver.crm_cust_info

 Select * from silver.crm_cust_info

  /*
*********************************
erp_loc_a101 
**********************************
*/  
Select 
	Replace(CID,'-','') CID,
	CNTRY
from Bronze.erp_loc_a101 
where Replace(CID,'-','') NOT IN (select cst_key from Silver.crm_cust_info)
--Data standarizaton
Select DISTINCT
	CNTRY
from Bronze.erp_loc_a101 
Order by CNTRY

/*
*********************************
erp_cust_az12
**********************************
*/
--Checks
Select 
Case when CID LIKE 'NAS%'
	THEN Substring(CID,4,len(CID))
	Else cid
	End as CID,
BDATE,
GEN
from Bronze.erp_cust_az12
Where Case when CID LIKE 'NAS%'
	THEN Substring(CID,4,len(CID))
	Else cid
	End NOT IN (Select cst_key From silver.crm_cust_info) ;
-----------------------------
Select DISTINCT
bdate
from Bronze.erp_cust_az12
Where bdate < '1924-01-01' or bdate > GETDATE()
-----------------------------
SELECT DISTINCT
GEN
from Bronze.erp_cust_az12
------------------------------
SELECT * FROM SILVER.erp_cust_az12

/*
*********************************
erp_px_cat_g1v2
**********************************
*/
--Checks
--Spaces
Select *c
FROM Bronze.erp_px_cat_g1v2
Where CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE)

--DATA STANDARDIZATION:
SELECT DISTINCT
MAINTENANCE
FROM Bronze.erp_px_cat_g1v2

--SELECT * FROM Silver.erp_px_cat_g1v2
