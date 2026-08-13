# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6354d19d-5e6d-489a-8c38-996ae35d53f7",
# META       "default_lakehouse_name": "retail_lakehouse",
# META       "default_lakehouse_workspace_id": "6f842650-a4ae-47b0-8ca1-b01e50f0be29",
# META       "known_lakehouses": [
# META         {
# META           "id": "6354d19d-5e6d-489a-8c38-996ae35d53f7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# STEP 28A: Confirm curated Lakehouse tables
# ============================================================

required_tables = [
    "dim_customers",
    "dim_products",
    "fact_order_lines",
    "fact_orders"
]

for table_name in required_tables:
    exists = spark.catalog.tableExists(table_name)
    print(f"{table_name}: {'AVAILABLE' if exists else 'MISSING'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 1: Daily revenue
# MAGIC -- Excludes cancelled orders
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC     COUNT(DISTINCT order_id) AS order_count,
# MAGIC     ROUND(SUM(gross_sales), 2) AS gross_sales,
# MAGIC     ROUND(SUM(discount_amount), 2) AS discount_amount,
# MAGIC     ROUND(SUM(net_sales), 2) AS net_sales,
# MAGIC     ROUND(SUM(tax_amount), 2) AS tax_amount,
# MAGIC     ROUND(SUM(shipping_cost), 2) AS shipping_revenue,
# MAGIC     ROUND(SUM(order_total_amount), 2) AS total_order_value
# MAGIC FROM fact_orders
# MAGIC WHERE order_status <> 'CANCELLED'
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 2: Top products by net sales
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     sku,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     brand,
# MAGIC     SUM(quantity) AS units_sold,
# MAGIC     COUNT(DISTINCT order_id) AS order_count,
# MAGIC     ROUND(SUM(gross_sales), 2) AS gross_sales,
# MAGIC     ROUND(SUM(discount_amount), 2) AS discount_amount,
# MAGIC     ROUND(SUM(net_sales), 2) AS net_sales
# MAGIC FROM fact_order_lines
# MAGIC WHERE order_status <> 'CANCELLED'
# MAGIC GROUP BY
# MAGIC     product_id,
# MAGIC     sku,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     brand
# MAGIC ORDER BY net_sales DESC
# MAGIC LIMIT 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 3: Customer lifetime value
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_full_name,
# MAGIC     customer_segment,
# MAGIC     region,
# MAGIC     COUNT(DISTINCT order_id) AS lifetime_orders,
# MAGIC     MIN(order_date) AS first_order_date,
# MAGIC     MAX(order_date) AS latest_order_date,
# MAGIC     ROUND(SUM(net_sales), 2) AS lifetime_net_sales,
# MAGIC     ROUND(SUM(order_total_amount), 2) AS lifetime_order_value,
# MAGIC     ROUND(AVG(order_total_amount), 2) AS average_order_value
# MAGIC FROM fact_orders
# MAGIC WHERE order_status <> 'CANCELLED'
# MAGIC GROUP BY
# MAGIC     customer_id,
# MAGIC     customer_full_name,
# MAGIC     customer_segment,
# MAGIC     region
# MAGIC ORDER BY lifetime_order_value DESC
# MAGIC LIMIT 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 4: Payment failures by method and reason
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     payment_method,
# MAGIC     COALESCE(failure_reason, 'UNKNOWN_REASON') AS failure_reason,
# MAGIC     COUNT(DISTINCT order_id) AS failed_order_count,
# MAGIC     ROUND(SUM(payment_amount), 2) AS failed_payment_amount,
# MAGIC     ROUND(AVG(payment_amount), 2) AS average_failed_amount
# MAGIC FROM fact_orders
# MAGIC WHERE payment_status = 'FAILED'
# MAGIC GROUP BY
# MAGIC     payment_method,
# MAGIC     COALESCE(failure_reason, 'UNKNOWN_REASON')
# MAGIC ORDER BY failed_order_count DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 5: Cancellation rate by order channel
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     order_channel,
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC 
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'CANCELLED' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS cancelled_orders,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         100.0
# MAGIC         * SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status = 'CANCELLED' THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         )
# MAGIC         / COUNT(DISTINCT order_id),
# MAGIC         2
# MAGIC     ) AS cancellation_rate_pct
# MAGIC FROM fact_orders
# MAGIC GROUP BY order_channel
# MAGIC ORDER BY cancellation_rate_pct DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 6: Late-delivery performance by carrier
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     carrier,
# MAGIC     COUNT(DISTINCT order_id) AS shipped_orders,
# MAGIC 
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN is_late_delivery = TRUE THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS late_orders,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         100.0
# MAGIC         * SUM(
# MAGIC             CASE
# MAGIC                 WHEN is_late_delivery = TRUE THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         )
# MAGIC         / COUNT(DISTINCT order_id),
# MAGIC         2
# MAGIC     ) AS late_delivery_rate_pct,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN delivery_delay_days > 0
# MAGIC                 THEN delivery_delay_days
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS average_late_days,
# MAGIC 
# MAGIC     MAX(delivery_delay_days) AS maximum_delay_days
# MAGIC FROM fact_orders
# MAGIC WHERE has_shipment = TRUE
# MAGIC GROUP BY carrier
# MAGIC ORDER BY late_delivery_rate_pct DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 7: Regional business performance
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     region,
# MAGIC     COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status <> 'CANCELLED'
# MAGIC                 THEN net_sales
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS net_sales,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN order_status <> 'CANCELLED'
# MAGIC                 THEN order_total_amount
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS total_order_value,
# MAGIC 
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN order_status <> 'CANCELLED'
# MAGIC                 THEN order_total_amount
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS average_order_value,
# MAGIC 
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN order_status = 'CANCELLED' THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS cancelled_orders,
# MAGIC 
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN is_late_delivery = TRUE THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS late_deliveries
# MAGIC FROM fact_orders
# MAGIC GROUP BY region
# MAGIC ORDER BY net_sales DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- QUERY 8: Monthly revenue trends and month-over-month growth
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC WITH monthly_sales AS (
# MAGIC     SELECT
# MAGIC         order_year,
# MAGIC         order_month,
# MAGIC         COUNT(DISTINCT order_id) AS order_count,
# MAGIC         COUNT(DISTINCT customer_id) AS customer_count,
# MAGIC         ROUND(SUM(net_sales), 2) AS monthly_net_sales,
# MAGIC         ROUND(SUM(order_total_amount), 2) AS monthly_order_value,
# MAGIC         ROUND(AVG(order_total_amount), 2) AS average_order_value
# MAGIC     FROM fact_orders
# MAGIC     WHERE order_status <> 'CANCELLED'
# MAGIC     GROUP BY
# MAGIC         order_year,
# MAGIC         order_month
# MAGIC ),
# MAGIC 
# MAGIC monthly_with_previous AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         LAG(monthly_net_sales) OVER (
# MAGIC             ORDER BY order_year, order_month
# MAGIC         ) AS previous_month_net_sales
# MAGIC     FROM monthly_sales
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC     order_year,
# MAGIC     order_month,
# MAGIC     order_count,
# MAGIC     customer_count,
# MAGIC     monthly_net_sales,
# MAGIC     monthly_order_value,
# MAGIC     average_order_value,
# MAGIC     previous_month_net_sales,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN previous_month_net_sales IS NULL
# MAGIC              OR previous_month_net_sales = 0
# MAGIC         THEN NULL
# MAGIC 
# MAGIC         ELSE ROUND(
# MAGIC             100.0
# MAGIC             * (
# MAGIC                 monthly_net_sales
# MAGIC                 - previous_month_net_sales
# MAGIC             )
# MAGIC             / previous_month_net_sales,
# MAGIC             2
# MAGIC         )
# MAGIC     END AS month_over_month_growth_pct
# MAGIC FROM monthly_with_previous
# MAGIC ORDER BY
# MAGIC     order_year,
# MAGIC     order_month;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- BONUS QUERY 9: Orders missing lines, payments or shipments
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN has_valid_order_lines = FALSE THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS orders_without_valid_lines,
# MAGIC 
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN has_payment = FALSE THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS orders_without_payment,
# MAGIC 
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN has_shipment = FALSE THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS orders_without_shipment
# MAGIC FROM fact_orders;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- ==========================================================
# MAGIC -- BONUS QUERY 10: Product-category performance
# MAGIC -- ==========================================================
# MAGIC 
# MAGIC SELECT
# MAGIC     category,
# MAGIC     COUNT(DISTINCT product_id) AS products_sold,
# MAGIC     COUNT(DISTINCT order_id) AS order_count,
# MAGIC     SUM(quantity) AS units_sold,
# MAGIC     ROUND(SUM(gross_sales), 2) AS gross_sales,
# MAGIC     ROUND(SUM(discount_amount), 2) AS discount_amount,
# MAGIC     ROUND(SUM(net_sales), 2) AS net_sales
# MAGIC FROM fact_order_lines
# MAGIC WHERE order_status <> 'CANCELLED'
# MAGIC GROUP BY category
# MAGIC ORDER BY net_sales DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
