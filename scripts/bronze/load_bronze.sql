USE [DataWarehouse]
GO
/****** Object:  StoredProcedure [bronze].[load_bronze]    Script Date: 6/18/2025 6:24:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 CREATE  OR ALTER    procedure [bronze].[load_bronze] as 
   Begin
   declare @batchstartime datetime, @batchendtime datetime;
   set @batchstartime=GETDATE();
   --select  top 10 * from bronze.crm_cust_info;
   BEGIN  TRY
   PRINT 'Loading  Bronze Layer';
   PRINT 'Loading CRM  Tables';
   TRUNCATE  table bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\user\Downloads\SRC CRM\cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
   
    TABLOCK
);
TRUNCATE  table bronze.crm_prd_info;
 --select  top 10 * from bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\user\Downloads\SRC CRM\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
   
    TABLOCK
);
TRUNCATE  table bronze.crm_sales_details;
 --select  top 10 * from bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\user\Downloads\SRC CRM\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
   
    TABLOCK
);
TRUNCATE  table bronze.erp_cust_az12;
--select  top 10 * from bronze.sales_cust_az12;
PRINT 'Loading ERP  Tables';

BULK INSERT bronze.erp_cust_az12

FROM 'C:\data\New folder\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
   
    TABLOCK
);
TRUNCATE  table bronze.erp_loc_a101;
--select  top 10 * from bronze.sales_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'C:\data\New folder\LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
   
    TABLOCK
);

   
TRUNCATE  table bronze.erp_px_cat_glv2;
--select  top 10 * from bronze.erp_px_cat_glv2;
BULK INSERT bronze.erp_px_cat_glv2
FROM 'C:\data\New folder\PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
   
    TABLOCK
);
set @batchendtime=GETDATE();
PRINT'time to load'+DATEDIFF(second,@batchstartime,@batchendtime);
END TRY
BEGIN CATCH
PRINT'=========================';
PRINT'=========================';
PRINT'===============ERROR OCUURED======';
PRINT'ERROR MESSAGE :'+ CAST(ERROR_MESSAGE() AS  NVARCHAR);
PRINT'ERROR NUMBER :'+ CAST(ERROR_NUMBER() AS NVARCHAR) ;


END  CATCH
end
