IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL -- checkinh for table existance
    DROP TABLE bronze.crm_cust_info; -- Drop the table if it already exists to ensure a clean and idempotent table creation process

Create table bronze.crm_cust_info(
	cst_id int,
	cst_key Nvarchar(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
go
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info; 
Create table bronze.crm_prd_info(
	prd_id        INT,
	prd_key       NVARCHAR(50),
	prd_nm        NVARCHAR(100),
	prd_cost      DECIMAL(18,2),
	prd_line      NVARCHAR(50),
	prd_start_dt  DATE,
	prd_end_dt    DATE
);
Go
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
Create table bronze.crm_sales_details(
	sls_ord_num   NVARCHAR(50),
	sls_prd_key   NVARCHAR(50),  -- FK → bronze.crm_prd_info.prd_key
	sls_cust_id   INT,           -- FK → bronze.crm_cust_info.cst_id
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
Create table bronze.erp_cust_az12(
	cid    NVARCHAR(50),
	bdate  DATE,
	gen    NVARCHAR(10)

);
GO
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
Create table bronze.erp_loc_a101(
cid    NVARCHAR(50),
cntry  NVARCHAR(50)

);
GO
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
Create table bronze.erp_px_cat_g1v2(
	id           NVARCHAR(50),
	cat          NVARCHAR(50),
	subcat       NVARCHAR(50),
	maintenance  NVARCHAR(50)
);

