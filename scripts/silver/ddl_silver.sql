USE [DataWarehouse]
GO



CREATE TABLE [silver].[crm_cust_info](
	[cst_id] [int] NULL,
	[cst_key] [nvarchar](50) NULL,
	[cst_firstname] [nvarchar](50) NULL,
	[cst_lastname] [nvarchar](50) NULL,
	[cst_martial_status] [nvarchar](50) NULL,
	[cst_gndr] [nvarchar](50) NULL,
	[cst_create_date] [date] NULL,
	[dwh_create_date] [datetime2](7) NULL
) ON [PRIMARY]
GO

ALTER TABLE [silver].[crm_cust_info] ADD  DEFAULT (getdate()) FOR [dwh_create_date]
GO


CREATE TABLE [silver].[crm_prd_info](
	[prd_id] [int] NULL,
	[cat_id] [nvarchar](50) NULL,
	[prd_key] [nvarchar](50) NULL,
	[prd_nm] [nvarchar](50) NULL,
	[prd_cost] [int] NULL,
	[prd_line] [nvarchar](50) NULL,
	[prd_start_dt] [date] NULL,
	[prd_end_dt] [date] NULL,
	[dwh_create_date] [datetime2](7) NULL
) ON [PRIMARY]
GO

ALTER TABLE [silver].[crm_prd_info] ADD  DEFAULT (getdate()) FOR [dwh_create_date]
GO

USE [DataWarehouse]
GO



CREATE TABLE [silver].[crm_sales_details](
	[sls_ord_num] [nvarchar](50) NULL,
	[sls_prd_key] [nvarchar](50) NULL,
	[sls_cust_id] [int] NULL,
	[sls_order_dt] [date] NULL,
	[sls_ship_dt] [date] NULL,
	[sls_due_dt] [date] NULL,
	[sls_sales] [int] NULL,
	[sls_quantity] [int] NULL,
	[sls_price] [int] NULL,
	[dwh_create_date] [datetime2](7) NULL
) ON [PRIMARY]
GO

ALTER TABLE [silver].[crm_sales_details] ADD  DEFAULT (getdate()) FOR [dwh_create_date]
GO


USE [DataWarehouse]
GO


CREATE TABLE [silver].[erp_cust_az12](
	[cid] [nvarchar](50) NULL,
	[bdate] [date] NULL,
	[gen] [nvarchar](50) NULL,
	[dwh_create_date] [datetime2](7) NULL
) ON [PRIMARY]
GO

ALTER TABLE [silver].[erp_cust_az12] ADD  DEFAULT (getdate()) FOR [dwh_create_date]
GO


USE [DataWarehouse]
GO



CREATE TABLE [silver].[erp_loc_a101](
	[cid] [nvarchar](50) NULL,
	[cntry] [nvarchar](50) NULL,
	[dwh_create_date] [datetime2](7) NULL
) ON [PRIMARY]
GO

ALTER TABLE [silver].[erp_loc_a101] ADD  DEFAULT (getdate()) FOR [dwh_create_date]
GO
USE [DataWarehouse]
GO



CREATE TABLE [silver].[erp_px_cat_glv2](
	[id] [nvarchar](50) NULL,
	[cat] [nvarchar](50) NULL,
	[subcat] [nvarchar](50) NULL,
	[maintenance] [nvarchar](50) NULL,
	[dwh_create_date] [datetime2](7) NULL
) ON [PRIMARY]
GO

ALTER TABLE [silver].[erp_px_cat_glv2] ADD  DEFAULT (getdate()) FOR [dwh_create_date]
GO

