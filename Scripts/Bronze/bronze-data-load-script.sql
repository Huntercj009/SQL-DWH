/* --Overall summsary what is happening--
This stored procedure orchestrates the end-to-end loading of the Bronze layer in a controlled ETL batch.
It initializes batch-level and step-level timestamps to measure execution time for each table and the overall run.
For each source file (CRM and ERP), the procedure truncates the target Bronze table to ensure a fresh load,
Ingests raw data using BULK INSERT with minimal transformation, and logs the load duration.
TRY–CATCH error handling ensures failures are captured and reported without leaving the process silent.
At completion, the procedure reports the total batch runtime, providing operational visibility and traceability.
*/

Create or alter procedure bronze.load_bronze As
Begin
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;-- Declare timestamp variables to track batch-level and step-level execution times.
	-- These variables enable monitoring of load duration, performance analysis, and operational logging.
	begin try
		SET @batch_start_time = GETDATE(); -- Capture the start timestamp of the overall batch execution to enable end-to-end runtime tracking and performance monitoring.
		print '=========================================================================';
		PRINT 'Loading The Bronze Layer';
		print '=========================================================================';

		print '-------------------------------------------------------------------------';
		PRINT 'Loading The Bronze Layer with CRM Folder files';
		print '-------------------------------------------------------------------------';
		SET @start_time = GETDATE(); -- Capture the start timestamp of the current load step to measure execution duration and support step-level performance monitoring.
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info -- BULK INSERT is used for high-volume, fast data ingestion from the file system
		from 'D:\DWH-Project\datasets\source_crm\cust_info.csv'  -- Load customer master data from a CSV file into the bronze.crm_cust_info table
		with(
		firstrow = 2, -- FIRSTROW = 2 skips the header row in the source file
		fieldterminator = ',', -- FIELDTERMINATOR = ',' specifies comma-separated values
		TABLOCK -- TABLOCK improves bulk load performance by acquiring a table-level lock
		);

		SET @end_time = GETDATE(); -- Capture the end timestamp of the current load step to calculate execution duration and support step-level performance analysis.

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info -- BULK INSERT is used for high-volume, fast data ingestion from the file system
		from 'D:\DWH-Project\datasets\source_crm\prd_info.csv'  -- Load customer master data from a CSV file into the bronze.crm_prd_info table
		with(
		firstrow = 2, -- FIRSTROW = 2 skips the header row in the source file
		fieldterminator = ',', -- FIELDTERMINATOR = ',' specifies comma-separated values
		TABLOCK -- TABLOCK improves bulk load performance by acquiring a table-level lock
		);
 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details -- BULK INSERT is used for high-volume, fast data ingestion from the file system
		from 'D:\DWH-Project\datasets\source_crm\sales_details.csv'  -- Load customer master data from a CSV file into the bronze.crm_sales_details table
		with(
		firstrow = 2, -- FIRSTROW = 2 skips the header row in the source file
		fieldterminator = ',', -- FIELDTERMINATOR = ',' specifies comma-separated values
		TABLOCK -- TABLOCK improves bulk load performance by acquiring a table-level lock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
 


		-- now erp folder 
		print '-------------------------------------------------------------------------';
		PRINT 'Loading The Bronze Layer with CRM Folder files';
		print '-------------------------------------------------------------------------'; 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12 -- BULK INSERT is used for high-volume, fast data ingestion from the file system
		from 'D:\DWH-Project\datasets\source_erp\cust_az12.csv'  -- Load customer master data from a CSV file into the bronze.erp_cust_az12 table
		with(
		firstrow = 2, -- FIRSTROW = 2 skips the header row in the source file
		fieldterminator = ',', -- FIELDTERMINATOR = ',' specifies comma-separated values
		TABLOCK -- TABLOCK improves bulk load performance by acquiring a table-level lock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101 -- BULK INSERT is used for high-volume, fast data ingestion from the file system
		from 'D:\DWH-Project\datasets\source_erp\loc_a101.csv'  -- Load customer master data from a CSV file into the bronze.erp_loc_a101 table
		with(
		firstrow = 2, -- FIRSTROW = 2 skips the header row in the source file
		fieldterminator = ',', -- FIELDTERMINATOR = ',' specifies comma-separated values
		TABLOCK -- TABLOCK improves bulk load performance by acquiring a table-level lock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2 -- BULK INSERT is used for high-volume, fast data ingestion from the file system
		from 'D:\DWH-Project\datasets\source_erp\px_cat_g1v2.csv'  -- Load customer master data from a CSV file into the bronze.erp_px_cat_g1v2 table
		with(
		firstrow = 2, -- FIRSTROW = 2 skips the header row in the source file
		fieldterminator = ',', -- FIELDTERMINATOR = ',' specifies comma-separated values
		TABLOCK -- TABLOCK improves bulk load performance by acquiring a table-level lock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE(); -- Capture the end timestamp of the overall batch execution to calculate total runtime and support batch-level performance monitoring.

		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

	end try
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END

-- Call the procedure for loading the data
Execute bronze.load_bronze;
