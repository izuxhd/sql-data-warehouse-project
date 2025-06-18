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
