USE [DataWarehouse]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER     procedure    [dbo].[load_silver]
as 
 begin
 SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN cst_martial_status = 'M' THEN 'Married'
        WHEN cst_martial_status = 'S' THEN 'Single'
    END AS cst_martial_status,
    CASE 
        WHEN cst_gndr = 'M' THEN 'Male'
        WHEN cst_gndr = 'F' THEN 'Female'
        else 'n/a'
    END AS cst_gndr,
    CAST(cst_create_date AS DATE) AS cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS rnk
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS N
WHERE rnk = 1;
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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE  
        WHEN prd_line = 'M' THEN 'Mountain'
        WHEN prd_line = 'R' THEN 'Road'
        WHEN prd_line = 'S' THEN 'Other Sales'
        WHEN prd_line = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
        AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_info;

INSERT INTO  [DataWarehouse].silver.[crm_sales_details] (
 sls_ord_num ,
  sls_prd_key ,
  sls_cust_id ,
  sls_order_dt ,
  sls_ship_dt ,
  sls_due_dt ,
  sls_sales ,
  sls_quantity ,
  sls_price )


SELECT [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      , case when   sls_order_dt=0 or LEN(sls_order_dt)!=8 then  NULL
      else CAST(CAST(sls_order_dt as varchar) as DATE) end  as sls_order_dt
      , case when   sls_ship_dt=0 or LEN(sls_ship_dt)!=8 then  NULL
       else CAST(CAST(sls_ship_dt as varchar) as DATE) end  as sls_ship_dt
      , case when   sls_due_dt=0 or LEN(sls_due_dt)!=8 then  NULL
      else CAST(CAST(sls_due_dt as varchar) as DATE)  end as sls_due_dt,
      case  when sls_sales<=0     or  sls_sales is NULL  or  sls_sales  !=sls_quantity * abs(sls_price)
              then  sls_quantity * abs(sls_price)
               else  sls_sales end as sls_sales,
               sls_quantity INT,
               
               case  when  sls_price is NULL  or  sls_price<=0
                     then  sls_price/NULLIF(sls_quantity,0)
                     else sls_price end  as sls_price
                     

      
  FROM [DataWarehouse].[bronze].[crm_sales_details]
  insert into silver.erp_cust_az12(
cid,bdate,gen)


select   
case  when  cid='NAS%'  then  substring(cid,4,len(cid))
      else   cid  end  as   cid,
         case  when bdate > getdate()  then  NULL
           else bdate end as   bdate,
            case  when   UPPER(TRIM(gen)) in ('F','FEMALE') then  'Female'
               when UPPER(TRIM(gen)) in ('M','MALE') then  'Male'  
                else  'n/a' end as  gen

                 from bronze.erp_cust_az12 ;




                 insert  into  silver.erp_loc_a101(cid,cntry)
 select   Replace(cid,'-','') as  cid,case  when cntry='DE' then  'Germany'
                                      when  TRIM(cntry)  in ('US','USA')  then  'United States'
                                        when  TRIM(cntry) is NULL  or cntry=''  then 'n/a'
                                        else  cntry  end  as  cntry from  bronze.erp_loc_a101
                                        
insert into   silver.erp_px_cat_glv2(id,cat,subcat,maintenance)


select  id,cat,subcat,maintenance  from bronze.erp_px_cat_glv2
 end;
