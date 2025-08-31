/*
********************************************
Stored Procedure: Load Silver Layer (Bronze-> Silver)
********************************************
Script purpose:
  This stored procedure performs the ETL (Extract,transform,Load) process to 
  populate the 'Silver' schema tables from the 'bronze' schema
  
Actions Performed:
  -Truncates Silver tables
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameter:
  None
  This stored procedure does not accepts any parameters or returns any values

Usage Example:
  Exec Silver.load_silver;

********************************************
*/

--USE Datawarehouse;

CREATE OR ALTER PROCEDURE Silver.load_Silver as
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @startBronzeLayerTime DATETIME, @EndBronzeLayerTime DATETIME;
	BEGIN TRY
		PRINT 'INSERTING DATA INTO: Silver layer'
		SET @startBronzeLayerTime = GETDATE();

		PRINT '*****TRUNCATING THE TABLE:Silver.crm_cust_info *****'
		TRUNCATE TABLE Silver.crm_cust_info
		PRINT '----------INSERTING DATA INTO TABLE : Silver.crm_cust_info ----------'

		SET @start_time = GETDATE();
		INSERT INTO Silver.crm_cust_info(
			cst_id ,
			cst_key ,
			cst_firstname ,
			cst_lastname ,
			cst_marital_status ,
			cst_gndr ,
			cst_create_date
		)
		Select 
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE
				when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
				when UPPER(TRIM(cst_marital_status)) = 'M'  then 'Married'
				ELSE 'n/a'
			END as cst_marital_status,
			CASE
				when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
				when UPPER(TRIM(cst_gndr)) = 'M'  then 'Male'
				ELSE 'n/a'
			END as cst_gndr, -- Normalizing gender values to readable formate
			cst_create_date
		From(
			SELECT 
			*,
			ROW_NUMBER()over(partition by cst_id order by cst_create_date DESC) as FlagLast
			From bronze.crm_cust_info
			WHERE cst_id IS NOT NULL -- check for the newst value creation for this duplicated value using time and date created
		)t 
		Where FlagLast = 1
		SET @end_time = GETDATE()
		PRINT 'Completed inserting the data to the table Silver.crm_cust_info: ' + CAST(datediff(second,@start_time,@end_time ) as VARCHAR) + 'SECONDS';

		PRINT '*****TRUNCATING THE TABLE:Silver.crm_prd_info*****'
		TRUNCATE TABLE Silver.crm_prd_info
		PRINT '----------INSERTING DATA INTO TABLE : Silver.crm_prd_info ----------'
		SET @start_time = GETDATE();
		INSERT INTO Silver.crm_prd_info(
			prd_id ,
			cat_id,
			prd_key,
			prd_nm ,
			prd_cost,
			prd_line ,
			prd_start_dt,
			prd_end_dt	
		)

		SELECT
			prd_id ,
			Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- In order to join with  bronze.erp_px_cat_g1v2 
			SubString(prd_key,7,len(prd_key)) as prd_key,-- In order join with sales details
			prd_nm ,
			Coalesce(prd_cost,0) as prd_cost,
			Case Upper(Trim(prd_line))
				WHEN 'M' Then 'Mountain'
				WHEN 'R' Then 'Road'
				WHEN 'S' Then 'Other Sales'
				WHEN 'T' Then 'Touring'
				ELSE 'n/a'

			End as prd_line ,
			CAST(prd_start_dt AS DATE) As prd_start_dt,
			CAST(LEAD(prd_start_dt)over(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) as prd_end_dt -- so there is no overlapping	(Data Enrichment)
		FROM Bronze.crm_prd_info 
		SET @end_time = GETDATE()
		PRINT 'Completed inserting the data to the table Silver.crm_prd_info : ' + CAST(datediff(second,@start_time,@end_time ) as VARCHAR) + 'SECONDS';

		PRINT '*****TRUNCATING THE TABLE: Silver.crm_sales_details*****'
		TRUNCATE TABLE Silver.crm_sales_details
		PRINT '----------INSERTING DATA INTO TABLE : Silver.crm_sales_details ----------'

		SET @start_time = GETDATE();

		INSERT INTO Silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales ,
			sls_quantity,
			sls_price)

		Select 
			sls_ord_num ,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR)As DATE)      -- As in sql server we can't directly cast an interger to Date ..so we are 1st converting to varchar and then to date.
			END sls_order_dt,
			CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR)As DATE)      -- As in sql server we can't directly cast an interger to Date ..so we are 1st converting to varchar and then to date.
			END sls_ship_dt,
			CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR)As DATE)      -- As in sql server we can't directly cast an interger to Date ..so we are 1st converting to varchar and then to date.
			END sls_due_dt,

			--Business Rule : Sum(Sales) = quantity * prize .If it is bad take with the expert
			sls_quantity,
			Case WHEN sls_sales is NULL or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END as sls_sales,

			Case when sls_sales is NULL or sls_sales <=0 
				THEN sls_sales/NullIF(sls_quantity ,0)
			ELSE sls_price
			END as sls_price
		FROM bronze.crm_sales_details

		SET @end_time = GETDATE()
		PRINT 'Completed inserting the data to the table Silver.crm_sales_details: ' + CAST(datediff(second,@start_time,@end_time ) as VARCHAR) + 'SECONDS';

		PRINT '*****TRUNCATING THE TABLE: Silver.erp_cust_az12*****'
		TRUNCATE TABLE Silver.erp_cust_az12
		PRINT '----------INSERTING DATA INTO TABLE : Silver.erp_cust_az12 ----------'
		SET @start_time = GETDATE();

		INSERT INTO Silver.erp_cust_az12(
			CID ,
			BDATE ,
			GEN
		)

		Select 
		Case when CID LIKE 'NAS%'
			THEN Substring(CID,4,len(CID))
			Else cid
			End as CID,
		Case When BDATE > getdate() Then NULL
			Else BDATE
			END as BDATE,
		CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') Then 'Female'
		WHEN UPPER(TRIM(GEN)) IN ('M','MALE') Then 'Male'
		ELSE 'N/A'
		END AS GEN
		from Bronze.erp_cust_az12;
		SET @end_time = GETDATE()
		PRINT 'Completed inserting the data to the table Silver.erp_cust_az12: ' + CAST(datediff(second,@start_time,@end_time ) as VARCHAR) + 'SECONDS';

		PRINT '*****TRUNCATING THE TABLE: Silver.erp_loc_a101*****'
		TRUNCATE TABLE Silver.erp_loc_a101
		PRINT '----------INSERTING DATA INTO TABLE : Silver.erp_loc_a101 ----------'
		SET @start_time = GETDATE();
		INSERT INTO Silver.erp_loc_a101 (
			CID,
			CNTRY
		)

		Select 
			Replace(CID,'-','') CID,
			CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United states'
			WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY) IS NULL THEN 'n/a'
			ELSE TRIM(CNTRY) 
			END CNTRY
		from Bronze.erp_loc_a101
		SET @end_time = GETDATE()
		PRINT 'Completed inserting the data to the table Silver.erp_loc_a101: ' + CAST(datediff(second,@start_time,@end_time ) as VARCHAR) + 'SECONDS';

		PRINT '*****TRUNCATING THE TABLE: Silver.erp_px_cat_g1v2*****'
		TRUNCATE TABLE Silver.erp_px_cat_g1v2
		PRINT '----------INSERTING DATA INTO TABLE : Silver.erp_px_cat_g1v2 ----------'
		SET @start_time = GETDATE();
		INSERT INTO Silver.erp_px_cat_g1v2(
			ID,
			CAT ,
			SUBCAT ,
			MAINTENANCE 
		)
		Select * 
		FROM Bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT 'Completed inserting the data to the table Silver.erp_px_cat_g1v2 ' + CAST(datediff(second,@start_time,@end_time ) as VARCHAR) + 'SECONDS';

		SET @EndBronzeLayerTime = GETDATE();
		PRINT 'EXECUTION TIME :' + CAST(datediff(second,@EndBronzeLayerTime,@startBronzeLayerTime) as VARCHAR ) + 'Seconds'
		PRINT 'INSERTED DATA INTO: Silver layer..Process COMPLETED'
		END TRY
		BEGIN CATCH
		PRINT '=================================';
		PRINT 'ERROR Occured During Loading Silver layer'
		PRINT 'ERROR Message' + ERROR_MESSAGE();
		PRINT 'ERROR Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=================================';

	END CATCH
	
END
