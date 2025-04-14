
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
*/


EXECUTE [bronze].[load_bronze]

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME

  BEGIN TRY
      SET @batch_start_time = GETDATE();
        PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

SET @START_TIME = GETDATE();

PRINT '>> Truncating Table: bronze.crm_cus_info';
Truncate Table bronze.crm_cus_info;

PRINT '>> Inserting data into : bronze.crm_cus_info';
BULK INSERT bronze.crm_cus_info
from 'C:\SQL Practice\SQL_DATAWAREHOUSE_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> ------------------';
-------------------------------------------------------

SET @START_TIME = GETDATE();

PRINT '>> Truncating Table: bronze.crm_prd_info';
Truncate Table bronze.crm_prd_info;

PRINT '>> Inserting data into : bronze.crm_prd_info';
BULK INSERT bronze.crm_prd_info
from 'C:\SQL Practice\SQL_DATAWAREHOUSE_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> ------------------';
-----------------------------------------------------------------------

SET @START_TIME = GETDATE();

PRINT '>> Truncating Table: crm_sales_details';
Truncate Table bronze.crm_sales_details;

PRINT '>> Inserting data into : crm_sales_details';
BULK INSERT bronze.crm_sales_details
from 'C:\SQL Practice\SQL_DATAWAREHOUSE_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> ------------------';

PRINT '------------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '------------------------------------------------';


SET @START_TIME = GETDATE();

PRINT '>> Truncating Table: bronze.erp_cust_az12';
Truncate Table bronze.erp_cust_az12;

PRINT '>> Inserting data into : bronze.erp_cust_az12';
BULK INSERT bronze.erp_cust_az12
from 'C:\SQL Practice\SQL_DATAWAREHOUSE_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> ------------------';

---------------------------------------------------------------------------
SET @START_TIME = GETDATE();

PRINT '>> Truncating Table: bronze.erp_loc_a101';
Truncate Table bronze.erp_loc_a101;

PRINT '>> Inserting data into : bronze.erp_loc_a101';
BULK INSERT bronze.erp_loc_a101
from 'C:\SQL Practice\SQL_DATAWAREHOUSE_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> ------------------';

-------------------------------------------------------------------------------

SET @START_TIME = GETDATE();

PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
Truncate Table bronze.erp_px_cat_g1v2;

PRINT '>> Inserting data into : bronze.erp_px_cat_g1v2';
BULK INSERT bronze.erp_px_cat_g1v2
from 'C:\SQL Practice\SQL_DATAWAREHOUSE_PROJECT\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
    FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @end_time = GETDATE();
PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> ------------------';

SET @batch_end_time = GETDATE();
PRINT '==================================================================='
PRINT 'loading bronze layuer is completed';
PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '==================================================================='

  END TRY
  BEGIN CATCH

 PRINT '------------------------------------------------';
 PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
 PRINT 'ERROR Message' + ERROR_MESSAGE();
 PRINT 'ERROR Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
 PRINT 'ERROR Message' + CAST (ERROR_STATE() AS NVARCHAR);
 PRINT '------------------------------------------------';

  END CATCH
END
