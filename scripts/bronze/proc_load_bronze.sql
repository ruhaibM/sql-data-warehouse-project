/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	Declare @start_time DATETIME ,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
	
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		print '=============================';
		print 'Loading the Bronze Layer';
		print '=============================';

		print '--------------------------------------------';
		print 'Loading CRM Table';
		print '--------------------------------------------';

		SET @start_time=GETDATE()
		print '>>Truncating table:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		print '>>Bulk Inserting table:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\nuhma\OneDrive\Desktop\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = GETDATE()
		print '>>Load Duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+' seconds';
		print '------------------------------------------------------------------';
	
		set @start_time = GETDATE()
		print '>>Truncating table:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		print '>>Bulk Inserting table:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\nuhma\OneDrive\Desktop\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		print '>> Load duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+' seconds';
		print '------------------------------------------------------------------';

		
		SET @start_time=GETDATE()
		print '>>Truncating table:bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		print '>>Bulk Inserting table:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details

		FROM 'C:\Users\nuhma\OneDrive\Desktop\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		print '>> Load duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+' seconds';
		print '------------------------------------------------------------------';
		
		print '--------------------------------------------';
		print 'Loading ERP Table';
		print '--------------------------------------------';

		SET @start_time=GETDATE()
		print '>>Truncating table:bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		print '>>Bulk Inserting table:bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101

		FROM 'C:\Users\nuhma\OneDrive\Desktop\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		print '>> Load duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+' seconds';
		print '------------------------------------------------------------------';
		
		
		SET @start_time=GETDATE()
		print '>>Truncating table:bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		print '>>Bulk Inserting table:bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\nuhma\OneDrive\Desktop\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		print '>> Load duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+' seconds';
		print '------------------------------------------------------------------';


		SET @start_time=GETDATE()
		print '>>Truncating table:bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		print '>>Bulk Inserting table:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\nuhma\OneDrive\Desktop\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		SET @end_time = GETDATE()
		print '>> Load duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+' seconds';
		print '------------------------------------------------------------------';

		set @batch_end_time = GETDATE();
		print '===================================================================';
		print ' >>Loading Bronze Layer is completed';
		print ' >> total duration:'+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds';
		print '===================================================================';
		 

		end try
		BEGIN CATCH 
			print '===================================';
			print 'Error Occured During Bronze layer';
			print 'Error Message'+ERROR_MESSAGE();
			print 'Error Message'+cast(ERROR_NUMBER() AS NVARCHAR) ;
			print 'Error Message'+cast(ERROR_STATE() AS NVARCHAR) ;
			print '===================================';
		END CATCH

END
