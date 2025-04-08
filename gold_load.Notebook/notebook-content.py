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
# META           "id": "2aa9e99d-b422-47d6-97d8-5487262a71aa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold_lakehouse.`dim_customers`
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
# MAGIC         ci.cst_id                          AS customer_id,
# MAGIC         ci.cst_key                         AS customer_number,
# MAGIC         ci.cst_firstname                   AS first_name,
# MAGIC         ci.cst_lastname                    AS last_name,
# MAGIC         la.cntry                           AS country,
# MAGIC         ci.cst_marital_status              AS marital_status,
# MAGIC         CASE 
# MAGIC             WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
# MAGIC             ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
# MAGIC         END                                AS gender,
# MAGIC         ca.bdate                           AS birthdate,
# MAGIC         ci.cst_create_date                 AS create_date
# MAGIC     FROM silver_lakehouse.cust_info ci
# MAGIC     LEFT JOIN silver_lakehouse.cust_az12 ca
# MAGIC         ON ci.cst_key = ca.cid
# MAGIC     LEFT JOIN silver_lakehouse.loc_a101 la
# MAGIC         ON ci.cst_key = la.cid;
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
# MAGIC CREATE OR REPLACE TABLE gold_lakehouse.dim_products
# MAGIC USING DELTA
# MAGIC AS
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
# MAGIC         pn.prd_id       AS product_id,
# MAGIC         pn.prd_key      AS product_number,
# MAGIC         pn.prd_nm       AS product_name,
# MAGIC         pn.cat_id       AS category_id,
# MAGIC         pc.cat          AS category,
# MAGIC         pc.subcat       AS subcategory,
# MAGIC         pc.maintenance  AS maintenance,
# MAGIC         pn.prd_cost     AS cost,
# MAGIC         pn.prd_line     AS product_line,
# MAGIC         pn.prd_start_dt AS start_date
# MAGIC     FROM silver_lakehouse.prd_info pn
# MAGIC     LEFT JOIN silver_lakehouse.px_cat_g1v2 pc
# MAGIC         ON pn.cat_id = pc.id
# MAGIC     WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold_lakehouse.fact_sales_details
# MAGIC USING DELTA
# MAGIC AS
# MAGIC     SELECT
# MAGIC         sd.sls_ord_num                  AS order_number,
# MAGIC         pr.product_key                  AS product_key,
# MAGIC         cu.customer_key                 AS customer_key,
# MAGIC         CAST(sd.sls_order_dt AS DATE)   AS order_date,
# MAGIC         CAST(sd.sls_ship_dt  AS DATE)   AS shipping_date,
# MAGIC         CAST(sd.sls_due_dt   AS DATE)   AS due_date,
# MAGIC         CAST(sd.sls_sales    AS DECIMAL(18,2)) AS sales_amount,
# MAGIC         CAST(sd.sls_quantity AS INT)           AS quantity,
# MAGIC         CAST(sd.sls_price    AS DECIMAL(18,2)) AS price
# MAGIC     FROM silver_lakehouse.sales_details sd
# MAGIC     LEFT JOIN gold_lakehouse.dim_products pr
# MAGIC         ON sd.sls_prd_key = pr.product_number
# MAGIC     LEFT JOIN gold_lakehouse.dim_customers cu
# MAGIC         ON sd.sls_cust_id = cu.customer_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
