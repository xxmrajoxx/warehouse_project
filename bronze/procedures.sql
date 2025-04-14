--------------------------------------------
--any changes with the data set--
--------------------------------------------

/* script purpose: this store procedures loads data into the 'bronze' schema from external csv files. *

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME, @END_TIME DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================';

		PRINT '================================';
		PRINT 'Loading CRM tables';
		PRINT '================================';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_cust_info';
		PRINT '>>INSERTING TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;  
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\andre\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, --- FIRST ROW IS THE SECOND ROW
			FIELDTERMINATOR = ',', -- THE SEPERATOR 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' SECONDS';
		PRINT '>> --------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_prd_info';
		PRINT '>>INSERTING TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_prd_info;  
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\andre\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, --- FIRST ROW IS THE SECOND ROW
			FIELDTERMINATOR = ',', -- THE SEPERATOR 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' SECONDS';
		PRINT '>> --------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_sales_details';
		PRINT '>>INSERTING TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;  
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\andre\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, --- FIRST ROW IS THE SECOND ROW
			FIELDTERMINATOR = ',', -- THE SEPERATOR 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' SECONDS';
		PRINT '>> --------------';

		PRINT '================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================';

		PRINT '================================';
		PRINT 'Loading erp tables';
		PRINT '================================';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_sales_details';
		PRINT '>>INSERTING TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;  
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\andre\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, --- FIRST ROW IS THE SECOND ROW
			FIELDTERMINATOR = ',', -- THE SEPERATOR 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' SECONDS';
		PRINT '>> --------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.erp_loc_a1';
		PRINT '>>INSERTING TABLE: bronze.erp_loc_a1';
		TRUNCATE TABLE bronze.erp_loc_a101;  
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\andre\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, --- FIRST ROW IS THE SECOND ROW
			FIELDTERMINATOR = ',', -- THE SEPERATOR 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' SECONDS';
		PRINT '>> --------------';

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
		PRINT '>>INSERTING TABLE: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;  
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\andre\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, --- FIRST ROW IS THE SECOND ROW
			FIELDTERMINATOR = ',', -- THE SEPERATOR 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' SECONDS';
		PRINT '>> --------------';

		SET @batch_end_time = GETDATE()
			PRINT '========================================='
			PRINT 'loading Bronze Layer is Completed';
			PRINT '	- total Load Duration ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
			PRINT '========================================='

		END TRY
		BEGIN CATCH
			PRINT '========================================='
			PRINT 'error occured during loading bronze layer'
			PRINT 'error message' +  Error_message();
			PRINT 'error message' +  cast (Error_number() as NVARCHAR);
			PRINT 'error message' +  cast (Error_state() as NVARCHAR);
			PRINT  '========================================'
		END CATCH
END

exec bronze.load_bronze 
