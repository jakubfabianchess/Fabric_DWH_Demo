# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1e1cb396-9a6f-4aae-88bc-4bc33507a5ae",
# META       "default_lakehouse_name": "silver_lakehouse",
# META       "default_lakehouse_workspace_id": "b1290856-b169-4f49-8357-bce41ec2c2c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "1e1cb396-9a6f-4aae-88bc-4bc33507a5ae"
# META         },
# META         {
# META           "id": "68429715-6394-4598-98a3-7dded08d08d7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Create or Replace Customer Table and Clean data
# MAGIC CREATE OR REPLACE TABLE silver_lakehouse.`cust_info`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   SELECT
# MAGIC     cst_id,
# MAGIC     cst_key,
# MAGIC     TRIM(cst_firstname) AS cst_firstname,
# MAGIC     TRIM(cst_lastname)  AS cst_lastname,
# MAGIC     CASE 
# MAGIC       WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
# MAGIC       WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
# MAGIC       ELSE 'n/a'
# MAGIC     END AS cst_marital_status,
# MAGIC     CASE 
# MAGIC       WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
# MAGIC       WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
# MAGIC       ELSE 'n/a'
# MAGIC     END AS cst_gndr,
# MAGIC     cst_create_date
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
# MAGIC     FROM bronze_lakehouse.`cust_info.csv`
# MAGIC     WHERE cst_id IS NOT NULL
# MAGIC   ) t
# MAGIC   WHERE flag_last = 1;
# MAGIC 
# MAGIC   

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver_lakehouse.`prd_info`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   prd_id,
# MAGIC   REPLACE(SUBSTRING(t.original_prd_key, 1, 5), '-', '_') AS cat_id,      -- Extract category ID
# MAGIC   SUBSTRING(t.original_prd_key, 7) AS prd_key,                          -- Extract product key
# MAGIC   t.prd_nm,
# MAGIC   COALESCE(t.prd_cost, 0) AS prd_cost,
# MAGIC   CASE 
# MAGIC     WHEN UPPER(TRIM(t.prd_line)) = 'M' THEN 'Mountain'
# MAGIC     WHEN UPPER(TRIM(t.prd_line)) = 'R' THEN 'Road'
# MAGIC     WHEN UPPER(TRIM(t.prd_line)) = 'S' THEN 'Other Sales'
# MAGIC     WHEN UPPER(TRIM(t.prd_line)) = 'T' THEN 'Touring'
# MAGIC     ELSE 'n/a'
# MAGIC   END AS prd_line,                                                      -- Map product line codes
# MAGIC   CAST(t.prd_start_dt AS DATE) AS prd_start_dt,
# MAGIC     DATE_SUB(
# MAGIC     LEAD(t.prd_start_dt) OVER (PARTITION BY t.original_prd_key ORDER BY t.prd_start_dt), 
# MAGIC     1
# MAGIC   ) AS prd_end_dt                                                      -- One day before next start date
# MAGIC FROM (
# MAGIC   -- Sub-select to avoid alias conflicts in window function
# MAGIC   SELECT
# MAGIC     prd_id,
# MAGIC     prd_key        AS original_prd_key,  -- Keep the raw key for partitioning
# MAGIC     prd_nm,
# MAGIC     prd_cost,
# MAGIC     prd_line,
# MAGIC     prd_start_dt
# MAGIC   FROM bronze_lakehouse.`prd_info.csv`
# MAGIC ) t;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver_lakehouse.`sales_details`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC SELECT 
# MAGIC 			sls_ord_num,
# MAGIC 			sls_prd_key,
# MAGIC 			sls_cust_id,
# MAGIC 			CASE 
# MAGIC 				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
# MAGIC 				ELSE CAST(CAST(sls_order_dt AS VARCHAR(50)) AS DATE)
# MAGIC 			END AS sls_order_dt,
# MAGIC 			CASE 
# MAGIC 				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
# MAGIC 				ELSE CAST(CAST(sls_ship_dt AS VARCHAR(50)) AS DATE)
# MAGIC 			END AS sls_ship_dt,
# MAGIC 			CASE 
# MAGIC 				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
# MAGIC 				ELSE CAST(CAST(sls_due_dt AS VARCHAR(50)) AS DATE)
# MAGIC 			END AS sls_due_dt,
# MAGIC 			CASE 
# MAGIC 				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
# MAGIC 					THEN sls_quantity * ABS(sls_price)
# MAGIC 				ELSE sls_sales
# MAGIC 			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
# MAGIC 			sls_quantity,
# MAGIC 			CASE 
# MAGIC 				WHEN sls_price IS NULL OR sls_price <= 0 
# MAGIC 					THEN sls_sales / NULLIF(sls_quantity, 0)
# MAGIC 				ELSE sls_price  -- Derive price if original value is invalid
# MAGIC 			END AS sls_price
# MAGIC 		FROM bronze_lakehouse.`sales_details.csv`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver_lakehouse.`cust_az12`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 			CASE
# MAGIC 				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
# MAGIC 				ELSE cid
# MAGIC 			END AS cid, 
# MAGIC 			CASE
# MAGIC 				WHEN bdate > DATE(CURRENT_TIMESTAMP()) THEN NULL
# MAGIC 				ELSE bdate
# MAGIC 			END AS bdate, -- Set future birthdates to NULL
# MAGIC 			CASE
# MAGIC 				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
# MAGIC 				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
# MAGIC 				ELSE 'n/a'
# MAGIC 			END AS gen -- Normalize gender values and handle unknown cases
# MAGIC 		FROM bronze_lakehouse.`CUST_AZ12.csv`;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver_lakehouse.`loc_a101`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 			REPLACE(cid, '-', '') AS cid, 
# MAGIC 			CASE
# MAGIC 				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
# MAGIC 				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
# MAGIC 				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
# MAGIC 				ELSE TRIM(cntry)
# MAGIC 			END AS cntry -- Normalize and Handle missing or blank country codes
# MAGIC 		FROM bronze_lakehouse.`LOC_A101.csv`

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver_lakehouse.`px_cat_g1v2`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC SELECT
# MAGIC 			id,
# MAGIC 			cat,
# MAGIC 			subcat,
# MAGIC 			maintenance
# MAGIC 		FROM bronze_lakehouse.`PX_CAT_G1V2.csv`

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
