
/*

===============================================================================
Stored Procedure: Load Bronze Layer ( Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure  perform the ETL (eXTRACT tRANSFORM LOAD) Process populate the 'Silver' schema tabbles from the 'bronze' schema loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates Silver tables
    - Inseryts transformed and cleaned data from bronze into silver tables

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
*/

----Check for Nulls or Duplicates in Primary Key
----Expectation: NO rESULT

SELECT * FROM [bronze].[crm_cus_info]

-----fIND THE DUPLICATES OR NOT
SELECT cst_id,count(*) FROM [bron
ze].[crm_cus_info] Where cst_id = 29456
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id is NULL
Where cst_id = 29456

----------Remove the duplicates

SELECT * FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
from bronze.crm_cus_info)t WHERE flag_last! = 1 and cst_id = 29466

----------------OR----------------------
SELECT * FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
from bronze.crm_cus_info)t WHERE flag_last=1 and cst_id = 29466


---------Check for unwanted spaces

SELECT cst_firstname FROM bronze.crm_cus_info
WHERE cst_firstname ! = TRIM(cst_firstname)

----------OR------------------
SELECT cst_id, 
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
from bronze.crm_cus_info WHERE cst_id IS NOT NULL)t 
WHERE flag_last = 1

-----Data Standerdization & Consistancy

SELECT DISTINCT cst_gndr
FROM bronze.crm_cus_info

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cus_info

-----cHANGE THE DATA-----------
SELECT cst_id, 
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'Male'
	 ELSE 'n/a'
	 END cst_gndr,
cst_create_date
FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
from bronze.crm_cus_info WHERE cst_id IS NOT NULL)t 
WHERE flag_last = 1

------Change Martial Status---------
SELECT cst_id, 
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status))  = 'M' THEN 'Married'
	 ELSE 'n/a'
	 END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'Male'
	 ELSE 'n/a'
	 END cst_gndr,
cst_create_date
FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
from bronze.crm_cus_info WHERE cst_id IS NOT NULL)t 
WHERE flag_last = 1

------Moving data into silvere layer(crm_cus_info)
PRINT '>> truncating table: silver.crm_cus_info';
TRUNCATE TABLE silver.crm_cus_info;
PRINT '>> Inserting DATA INTO .silver.crm_cus_info';
INSERT INTO silver.crm_cus_info (
 cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT cst_id, 
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status))  = 'M' THEN 'Married'
	 ELSE 'n/a'
	 END As cst_marital_status,   ---Normalize martial status values to redable format
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'Male'
	 ELSE 'n/a'
	 END  As cst_gndr,  ----Normalize gender values to readable format
cst_create_date
FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
from bronze.crm_cus_info WHERE cst_id IS NOT NULL)t 
WHERE flag_last = 1

----------Check the silve layer after transformation-----------------

select * from [silver].[crm_cus_info]

SELECT cst_id,count(*) FROM silver.crm_cus_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id is NULL
--Where cst_id = 29456

---Check the unwanted spaces----------
SELECT cst_firstname
FROM silver.crm_cus_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM silver.crm_cus_info
WHERE cst_lastname != TRIM(cst_lastname)

------Data Standerdization & Consistancy

SELECT DISTINCT cst_gndr
FROM silver.crm_cus_info







--Check for nulls or duplicates in Primary Key(crm_prd_info)
--Expectation: NO Result

SELECT
    prd_id,
    prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 Else 'n/a'
		 End AS prd_line,
    CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (Partition by prd_key ORDER BY prd_start_dt)-1 As Date) AS prd_end_dt_test
	FROM bronze.crm_prd_info
	WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

--	where SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
--	SELECT sls_prd_key FROM bronze.crm_sales_details)

--	where REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') NOT IN
--	(SELECT distinct id from bronze.erp_px_cat_g1v2)

SELECT prd_id,count(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id Is NULL 


------Moving data into silvere layer(crm_prd_info)
PRINT '>> truncating table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Inserting DATA INTO .silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
    prd_id,
	cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id, ----Extract Category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, ----Extract product key
    prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 Else 'n/a'
		 End AS prd_line,  ----Map product line codes to descriptive values
    CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (Partition by prd_key ORDER BY prd_start_dt)-1 As Date) AS prd_end_dt ----=Calculate End date as one day before the next start date
	FROM bronze.crm_prd_info

----------Check the silve layer before transformation-----------------

select * from [silver].[crm_cus_info]

SELECT cst_id,count(*) FROM bronze.crm_cus_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: No result

SELECT prd_nm 
FROM bronze.crm_prd_info
WHERE prd_nm !=TRIM(prd_nm)

--Check for Nulls or Negative Numbers
--Expectation: No Results

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

---Data Standardization & Consistancy
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--Check for Invalid Date Orders

SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

----------Check the silve layer after transformation-----------------

SELECT prd_id,count(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: No result

SELECT prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm !=TRIM(prd_nm)


--Check for Nulls or Negative Numbers
--Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

---Data Standardization & Consistancy
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * FROM silver.crm_prd_info


--Check for Invalid Date Orders

SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt







--Check for nulls or duplicates in Primary Key(crm_sales_info)
--Expectation: NO Result

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_order_dt AS VARCHAR) AS DATE)
		 END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_ship_dt AS VARCHAR) AS DATE)
		 END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_due_dt AS VARCHAR) AS DATE)
		 END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
	FROM bronze.crm_sales_details
	--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info
	--WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cus_info
);

-------------------Moving data into silver layer-------------------
PRINT '>> truncating table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting DATA INTO: silver.crm_sales_details';
  INSERT INTO silver.crm_sales_details(
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
	)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_order_dt AS VARCHAR) AS DATE)
		 END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_ship_dt AS VARCHAR) AS DATE)
		 END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_due_dt AS VARCHAR) AS DATE)
		 END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
	FROM bronze.crm_sales_details

----------Check the silve layer before transformation-----------------
---------Check the invalid dates------------------------------------

SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0
OR LEN(sls_order_dt) !=8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

--------------------Check for invalid Date Orders------------------------

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--------Check Data Consistancy: Between sales, Quantity, anmd price--------
--- >> Sales= Quantity * Price
--- >> Values must not be NULL, zERO, poR Negative

SELECT distinct
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
	 END AS sls_sales,----- RECALCULATE SALES IF ORIGINAL VALUE IS MISSING OR INCORRECT
CASE WHEN sls_price iS NULL OR sls_price <=0
    THEN sls_sales/ NULLIF(sls_quantity,0)
	ELSE sls_price  -------DERIVE PRICE IF ORIGINAL VALUE IS INVALID
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS null or sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 or sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

---------------Final Code----------------------

--RULES: 
--if sales is NEGATIVE, ZERO OR NULL DERIVE IT USING QUANTITY AND PRICE
--iF PRICE IS ZERO OR NULL, CALCULATE IT USING SALES AND QUANTITY
-- IF PRICE IS NEGATIVE, CONVERT IT TO APOSITIVE VALUE

----------Check the silve layer before transformation-----------------

--------------------Check for invalid Date Orders------------------------

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--------Check Data Consistancy: Between sales, Quantity, anmd price--------
--- >> Sales= Quantity * Price
--- >> Values must not be NULL, zERO, poR Negative

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS null or sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 or sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details


--Check for nulls or duplicates in Primary Key(erp_cust_az12)
--Expectation: NO Result

SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END as bdate,
gen 
from bronze.erp_cust_az12

SELECT * FROM silver.erp_cust_az12

----Identify Out-of-Range Dates

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE Bdate < '1924-01-01' OR bdate > GETDATE()

---Data Standardization & Consistancy

SELECT DISTINCT gen,
CASE WHEN upper(TRIM(gen)) IN ('F', 'FEMALE')THEN 'Female'
     WHEN upper(TRIM(gen)) IN ('M', 'MALE')THEN 'Male'
   ELSE 'N/A'
END AS gen
FROM bronze.erp_cust_az12

--Final Codde: Moving data into silver layer
PRINT '>> truncating table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting DATA INTO: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(cid, bdate, gen) 
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ---Remove 'NAS' prefix if present
ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END as bdate, --SET future birthdates to NULL
CASE WHEN upper(TRIM(gen)) IN ('F', 'FEMALE')THEN 'Female'
     WHEN upper(TRIM(gen)) IN ('M', 'MALE')THEN 'Male'
   ELSE 'N/A'
END AS gen     ---NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES
from bronze.erp_cust_az12


----Identify Out-of-Range Dates

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE Bdate < '1924-01-01' OR bdate > GETDATE()

---Data Standardization & Consistancy

select distinct
gen
FROM silver.erp_cust_az12


--Check for nulls or duplicates in Primary Key(erp_cust_az12)
--Expectation: NO Result
--Final Codde: Moving data into silver layer
PRINT '>> truncating table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting DATA INTO: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101
(cid, cntry)
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END as cntry ------- Normalize and handle missing or blank country codes
FROM bronze.erp_loc_a101 
--WHERE cid NOT IN
--(SELECT cst_key FROM silver.crm_cus_info)

----------Check the silve layer before transformation-----------------
---DATA STANDERDIZATION & CONSISTANCY
select distinct cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

----------Check the silve layer after transformation-----------------
---DATA STANDERDIZATION & CONSISTANCY
select distinct cntry
FROM silver.erp_loc_a101
ORDER BY cntry 

select * from silver.erp_loc_a101


--Check for nulls or duplicates in Primary Key(erp_px_cat_g1v2)
--Expectation: NO Result
--Final Codde: Moving data into silver layer

------Moving data into silvere layer(silver.erp_px_cat_g1v2)
PRINT '>> truncating table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting DATA INTO .silver.erp_px_cat_g1v2';
  
  INSERT INTO silver.erp_px_cat_g1v2
  (id, cat, subcat, maintenance)
  SELECT
  id,
  cat,
  subcat,
  maintenance
  FROM bronze.erp_px_cat_g1v2

--Check the unwanted spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat !=TRIM(cat) OR subcat !=TRIM(subcat) OR maintenance !=TRIM(maintenance)

--Data Standardization & Consistancy
SELECT distinct
cat
FROM bronze.erp_px_cat_g1v2

SELECT distinct
subcat
FROM bronze.erp_px_cat_g1v2

SELECT distinct
maintenance
FROM bronze.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2
================================================================
-------------------Final codes----------------------------
---------------create Stored_procedure(Silver.layer)

------Moving data into silvere layer(crm_cus_info)
CREATE or ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
SET @batch_start_time = GETDATE();
PRINT '======================================================';
PRINT 'Loading Silver Layer';
PRINT '======================================================';

PRINT '======================================================';
PRINT 'Loading CRM Layer';
PRINT '======================================================';

SET @start_time = GETDATE();
PRINT '>> truncating table: silver.crm_cus_info';
TRUNCATE TABLE silver.crm_cus_info;
PRINT '>> Inserting DATA INTO .silver.crm_cus_info';
INSERT INTO silver.crm_cus_info (
 cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT cst_id, 
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status))  = 'M' THEN 'Married'
	 ELSE 'n/a'
	 END As cst_marital_status,   ---Normalize martial status values to redable format
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'Male'
	 ELSE 'n/a'
	 END  As cst_gndr,  ----Normalize gender values to readable format
cst_create_date
FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
from bronze.crm_cus_info WHERE cst_id IS NOT NULL)t 
WHERE flag_last = 1
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------';

PRINT '======================================================';
PRINT 'Loading CRM Layer';
PRINT '======================================================';

SET @start_time = GETDATE();
PRINT '>> truncating table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Inserting DATA INTO .silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
    prd_id,
	cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id, ----Extract Category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, ----Extract product key
    prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 Else 'n/a'
		 End AS prd_line,  ----Map product line codes to descriptive values
    CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (Partition by prd_key ORDER BY prd_start_dt)-1 As Date) AS prd_end_dt ----=Calculate End date as one day before the next start date
	FROM bronze.crm_prd_info
	SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------';

PRINT '======================================================';
PRINT 'Loading CRM Layer';
PRINT '======================================================';

SET @start_time = GETDATE();

PRINT '>> truncating table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting DATA INTO: silver.crm_sales_details';
  INSERT INTO silver.crm_sales_details(
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
	)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_order_dt AS VARCHAR) AS DATE)
		 END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_ship_dt AS VARCHAR) AS DATE)
		 END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
	     ELSE CAST(CAST( sls_due_dt AS VARCHAR) AS DATE)
		 END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
	FROM bronze.crm_sales_details
	SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------';

PRINT '======================================================';
PRINT 'Loading ERP Layer';
PRINT '======================================================';

SET @start_time = GETDATE();

PRINT '>> truncating table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting DATA INTO: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(cid, bdate, gen) 
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ---Remove 'NAS' prefix if present
ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END as bdate, --SET future birthdates to NULL
CASE WHEN upper(TRIM(gen)) IN ('F', 'FEMALE')THEN 'Female'
     WHEN upper(TRIM(gen)) IN ('M', 'MALE')THEN 'Male'
   ELSE 'N/A'
END AS gen     ---NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES
from bronze.erp_cust_az12
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------';

PRINT '======================================================';
PRINT 'Loading ERP Layer';
PRINT '======================================================';

SET @start_time = GETDATE();

PRINT '>> truncating table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting DATA INTO: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101
(cid, cntry)
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END as cntry ------- Normalize and handle missing or blank country codes
FROM bronze.erp_loc_a101 
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------';

PRINT '======================================================';
PRINT 'Loading ERP Layer';
PRINT '======================================================';

SET @start_time = GETDATE();

PRINT '>> truncating table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting DATA INTO .silver.erp_px_cat_g1v2';
  
  INSERT INTO silver.erp_px_cat_g1v2
  (id, cat, subcat, maintenance)
  SELECT
  id,
  cat,
  subcat,
  maintenance
  FROM bronze.erp_px_cat_g1v2
  SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '>> --------------';

SET @batch_end_time =GETDATE();
PRINT '======================================================='
PRINT 'Loading Silver Layer is completed';
PRINT '  -Total Load Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
PRINT '======================================================='

END TRY
BEGIN CATCH
PRINT '========================================================='
PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
PRINT 'ERROR Message' + ERROR_MESSAGE();
PRINT 'ERROR Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT 'ERROR Message' + CAST (ERROR_STATE() AS NVARCHAR);
PRINT '========================================================='
END CATCH
END


-----check the silver_load_silver & bronze.load_bronze

EXEC bronze.load_bronze
EXEC silver.load_silver

