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
# STEP 23A: Curated transformation pipeline setup
# Notebook: 03_transform_curate
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timezone

CURATED_RUN_ID = datetime.now(timezone.utc).strftime(
    "CURATED_%Y%m%d_%H%M%S"
)

CURATED_STARTED_AT = datetime.now(timezone.utc)

STAGING_PATHS = {
    "customers": "Files/curated/staging/customers",
    "products": "Files/curated/staging/products",
    "orders": "Files/curated/staging/orders",
    "order_items": "Files/curated/staging/order_items",
    "payments": "Files/curated/staging/payments",
    "shipments": "Files/curated/staging/shipments"
}

CURATED_FILE_PATHS = {
    "dim_customers": "Files/curated/dim_customers",
    "dim_products": "Files/curated/dim_products",
    "fact_order_lines": "Files/curated/fact_order_lines",
    "fact_orders": "Files/curated/fact_orders"
}

print(f"Curated run ID: {CURATED_RUN_ID}")
print(f"Curated pipeline started at: {CURATED_STARTED_AT}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 23B: Read validated staging datasets
# ============================================================

customers_staging = (
    spark.read
    .format("delta")
    .load(STAGING_PATHS["customers"])
)

products_staging = (
    spark.read
    .format("delta")
    .load(STAGING_PATHS["products"])
)

orders_staging = (
    spark.read
    .format("delta")
    .load(STAGING_PATHS["orders"])
)

order_items_staging = (
    spark.read
    .format("delta")
    .load(STAGING_PATHS["order_items"])
)

payments_staging = (
    spark.read
    .format("delta")
    .load(STAGING_PATHS["payments"])
)

shipments_staging = (
    spark.read
    .format("delta")
    .load(STAGING_PATHS["shipments"])
)

print("All validated staging datasets loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 23C: Verify staging input counts
# ============================================================

staging_counts = [
    ("customers", customers_staging.count()),
    ("products", products_staging.count()),
    ("orders", orders_staging.count()),
    ("order_items", order_items_staging.count()),
    ("payments", payments_staging.count()),
    ("shipments", shipments_staging.count())
]

staging_counts_df = spark.createDataFrame(
    staging_counts,
    ["dataset_name", "row_count"]
)

display(
    staging_counts_df
    .orderBy("dataset_name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 24A: Create customer dimension
# Grain: One row per customer_id
# ============================================================

dim_customers = (
    customers_staging

    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "state",
        "region",
        "country",
        "customer_segment",
        "customer_status",
        "signup_date",
        "updated_at",
        "load_date",
        "pipeline_run_id"
    )

    .withColumn(
        "customer_full_name",
        F.concat_ws(
            " ",
            F.col("first_name"),
            F.col("last_name")
        )
    )

    .withColumn(
        "curated_run_id",
        F.lit(CURATED_RUN_ID)
    )

    .withColumn(
        "curated_at",
        F.current_timestamp()
    )
)

customer_dimension_count = dim_customers.count()

customer_dimension_distinct_count = (
    dim_customers
    .select("customer_id")
    .distinct()
    .count()
)

print(
    f"Customer dimension rows: "
    f"{customer_dimension_count:,}"
)

print(
    f"Distinct customer IDs: "
    f"{customer_dimension_distinct_count:,}"
)

print(
    "Customer key uniqueness passed:",
    customer_dimension_count
    == customer_dimension_distinct_count
)

display(
    dim_customers
    .orderBy("customer_id")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 24B: Write customer dimension
# ============================================================

(
    dim_customers
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_customers")
)

print("Table created: dim_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 25A: Create product dimension
# Grain: One row per product_id
# ============================================================

dim_products = (
    products_staging

    .select(
        "product_id",
        "sku",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "unit_price",
        "unit_cost",
        "product_status",
        "updated_at",
        "load_date",
        "pipeline_run_id"
    )

    # Useful product profitability field
    .withColumn(
        "unit_margin",
        F.round(
            F.col("unit_price")
            - F.col("unit_cost"),
            2
        )
    )

    .withColumn(
        "margin_pct",
        F.when(
            F.col("unit_price") > 0,
            F.round(
                (
                    F.col("unit_price")
                    - F.col("unit_cost")
                )
                / F.col("unit_price")
                * 100,
                2
            )
        )
    )

    .withColumn(
        "curated_run_id",
        F.lit(CURATED_RUN_ID)
    )

    .withColumn(
        "curated_at",
        F.current_timestamp()
    )
)

product_dimension_count = dim_products.count()

product_dimension_distinct_count = (
    dim_products
    .select("product_id")
    .distinct()
    .count()
)

print(
    f"Product dimension rows: "
    f"{product_dimension_count:,}"
)

print(
    f"Distinct product IDs: "
    f"{product_dimension_distinct_count:,}"
)

print(
    "Product key uniqueness passed:",
    product_dimension_count
    == product_dimension_distinct_count
)

display(
    dim_products
    .orderBy("product_id")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 25B: Write product dimension
# ============================================================

(
    dim_products
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_products")
)

print("Table created: dim_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 26A: Prepare datasets for the order-line fact table
# ============================================================

orders_for_lines = (
    orders_staging
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "order_channel",
        "currency_code",

        F.col("discount_pct").alias(
            "order_discount_pct"
        ),

        "tax_rate",
        "shipping_cost"
    )
)

order_items_for_lines = (
    order_items_staging
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "line_number",
        "quantity",

        # This is the price charged when the order occurred.
        F.col("unit_price").alias(
            "transaction_unit_price"
        ),

        F.col("discount_pct").alias(
            "line_discount_pct"
        ),

        F.col("updated_at").alias(
            "order_item_updated_at"
        ),

        F.col("load_date").alias(
            "order_item_load_date"
        )
    )
)

products_for_lines = (
    products_staging
    .select(
        "product_id",
        "sku",
        "product_name",
        "category",
        "subcategory",
        "brand",

        # Current master-data price, retained for comparison.
        F.col("unit_price").alias(
            "current_product_price"
        )
    )
)

print("Fact-table source datasets prepared.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 26B: Join order lines with orders and products
# ============================================================

order_lines_joined = (
    order_items_for_lines.alias("items")

    # Every valid order item should have a valid order.
    .join(
        orders_for_lines.alias("orders"),
        on="order_id",
        how="inner"
    )

    # Use a left join so missing product attributes can still
    # be detected during reconciliation.
    .join(
        products_for_lines.alias("products"),
        on="product_id",
        how="left"
    )
)

joined_order_line_count = order_lines_joined.count()

print(
    f"Joined order-line rows: "
    f"{joined_order_line_count:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 26C: Calculate line-level financial measures
# Grain: One row per order_item_id
# ============================================================

fact_order_lines = (
    order_lines_joined

    # --------------------------------------------------------
    # Gross sales = quantity × transaction unit price
    # --------------------------------------------------------
    .withColumn(
        "gross_sales",
        F.round(
            F.col("quantity")
            * F.col("transaction_unit_price"),
            2
        ).cast(
            T.DecimalType(18, 2)
        )
    )

    # --------------------------------------------------------
    # Discount amount
    #
    # We use the line-level discount percentage.
    # The order-level percentage is retained for auditing but
    # is not applied again, which would double-count discounts.
    # --------------------------------------------------------
    .withColumn(
        "discount_amount",
        F.round(
            F.col("gross_sales")
            * (
                F.col("line_discount_pct")
                / F.lit(100)
            ),
            2
        ).cast(
            T.DecimalType(18, 2)
        )
    )

    # --------------------------------------------------------
    # Net sales = gross sales - discount
    # --------------------------------------------------------
    .withColumn(
        "net_sales",
        F.round(
            F.col("gross_sales")
            - F.col("discount_amount"),
            2
        ).cast(
            T.DecimalType(18, 2)
        )
    )

    # --------------------------------------------------------
    # Tax amount = net sales × tax rate
    # --------------------------------------------------------
    .withColumn(
        "tax_amount",
        F.round(
            F.col("net_sales")
            * F.col("tax_rate"),
            2
        ).cast(
            T.DecimalType(18, 2)
        )
    )

    # --------------------------------------------------------
    # Line total after discount and tax
    # Shipping is not included here because it belongs once
    # to the complete order, not once per product line.
    # --------------------------------------------------------
    .withColumn(
        "line_total_after_tax",
        F.round(
            F.col("net_sales")
            + F.col("tax_amount"),
            2
        ).cast(
            T.DecimalType(18, 2)
        )
    )

    # Partition and analytical fields
    .withColumn(
        "order_year",
        F.year("order_date")
    )
    .withColumn(
        "order_month",
        F.month("order_date")
    )

    # Curated operational metadata
    .withColumn(
        "curated_run_id",
        F.lit(CURATED_RUN_ID)
    )
    .withColumn(
        "curated_at",
        F.current_timestamp()
    )

    .select(
        "order_item_id",
        "order_id",
        "customer_id",
        "product_id",
        "line_number",
        "order_date",
        "order_year",
        "order_month",
        "order_status",
        "order_channel",
        "currency_code",
        "sku",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "quantity",
        "transaction_unit_price",
        "current_product_price",
        "line_discount_pct",
        "order_discount_pct",
        "gross_sales",
        "discount_amount",
        "net_sales",
        "tax_rate",
        "tax_amount",
        "line_total_after_tax",
        "order_item_updated_at",
        "order_item_load_date",
        "curated_run_id",
        "curated_at"
    )
)

print("Line-level financial calculations completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 26D: Validate fact_order_lines
# ============================================================

source_order_item_count = (
    order_items_staging.count()
)

fact_order_line_count = (
    fact_order_lines.count()
)

distinct_fact_order_item_count = (
    fact_order_lines
    .select("order_item_id")
    .distinct()
    .count()
)

missing_product_attributes_count = (
    fact_order_lines
    .filter(
        F.col("product_name").isNull()
        | F.col("category").isNull()
    )
    .count()
)

print(
    f"Valid staging order-item rows: "
    f"{source_order_item_count:,}"
)

print(
    f"Fact order-line rows: "
    f"{fact_order_line_count:,}"
)

print(
    f"Distinct order-item IDs: "
    f"{distinct_fact_order_item_count:,}"
)

print(
    f"Rows missing product attributes: "
    f"{missing_product_attributes_count:,}"
)

print(
    "Source-to-fact row-count reconciliation passed:",
    source_order_item_count
    == fact_order_line_count
)

print(
    "Fact grain uniqueness passed:",
    fact_order_line_count
    == distinct_fact_order_item_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 26E: Review fact-order-line calculations
# ============================================================

display(
    fact_order_lines
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "product_name",
        "quantity",
        "transaction_unit_price",
        "line_discount_pct",
        "gross_sales",
        "discount_amount",
        "net_sales",
        "tax_rate",
        "tax_amount",
        "line_total_after_tax"
    )
    .orderBy("order_item_id")
    .limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 26F: Write fact_order_lines
# ============================================================

# Lakehouse Delta table for SQL and analytical access
(
    fact_order_lines
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy(
        "order_year",
        "order_month"
    )
    .saveAsTable("fact_order_lines")
)

# Explicit partitioned Parquet output for the assignment
(
    fact_order_lines
    .write
    .mode("overwrite")
    .partitionBy(
        "order_year",
        "order_month"
    )
    .parquet(
        CURATED_FILE_PATHS["fact_order_lines"]
    )
)

print("Table created: fact_order_lines")

print(
    "Partitioned Parquet output:",
    CURATED_FILE_PATHS["fact_order_lines"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27A: Aggregate order-line measures to order level
# ============================================================

order_line_summary = (
    fact_order_lines
    .groupBy("order_id")
    .agg(
        F.count("order_item_id").alias("order_line_count"),

        F.countDistinct("product_id").alias(
            "distinct_product_count"
        ),

        F.sum("quantity").alias("total_quantity"),

        F.round(
            F.sum("gross_sales"),
            2
        ).cast(
            T.DecimalType(20, 2)
        ).alias("gross_sales"),

        F.round(
            F.sum("discount_amount"),
            2
        ).cast(
            T.DecimalType(20, 2)
        ).alias("discount_amount"),

        F.round(
            F.sum("net_sales"),
            2
        ).cast(
            T.DecimalType(20, 2)
        ).alias("net_sales"),

        F.round(
            F.sum("tax_amount"),
            2
        ).cast(
            T.DecimalType(20, 2)
        ).alias("tax_amount"),

        F.round(
            F.sum("line_total_after_tax"),
            2
        ).cast(
            T.DecimalType(20, 2)
        ).alias("line_total_after_tax")
    )
)

print(
    f"Order-line summaries created: "
    f"{order_line_summary.count():,}"
)

display(
    order_line_summary
    .orderBy("order_id")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27B: Select latest payment for each order
# ============================================================

from pyspark.sql.window import Window

payment_order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("payment_id").desc()
    )
)

latest_payment_per_order = (
    payments_staging

    .withColumn(
        "_payment_rank",
        F.row_number().over(payment_order_window)
    )

    .filter(
        F.col("_payment_rank") == 1
    )

    .select(
        "order_id",
        "payment_id",
        "transaction_id",
        "payment_method",
        "payment_amount",
        "payment_status",
        "failure_reason",
        "payment_date",

        F.col("updated_at").alias(
            "payment_updated_at"
        )
    )
)

payment_summary_per_order = (
    payments_staging
    .groupBy("order_id")
    .agg(
        F.count("payment_id").alias(
            "payment_attempt_count"
        ),

        F.round(
            F.sum(
                F.when(
                    F.col("payment_status") == "PAID",
                    F.col("payment_amount")
                ).otherwise(
                    F.lit(0)
                )
            ),
            2
        ).cast(
            T.DecimalType(20, 2)
        ).alias("total_paid_amount"),

        F.round(
            F.sum(
                F.when(
                    F.col("payment_status") == "REFUNDED",
                    F.col("payment_amount")
                ).otherwise(
                    F.lit(0)
                )
            ),
            2
        ).cast(
            T.DecimalType(20, 2)
        ).alias("total_refunded_amount")
    )
)

payments_for_fact = (
    latest_payment_per_order
    .join(
        payment_summary_per_order,
        on="order_id",
        how="left"
    )
)

print(
    f"Orders with valid payment records: "
    f"{payments_for_fact.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27C: Select latest shipment for each order
# ============================================================

shipment_order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("shipment_id").desc()
    )
)

latest_shipment_per_order = (
    shipments_staging

    .withColumn(
        "_shipment_rank",
        F.row_number().over(
            shipment_order_window
        )
    )

    .filter(
        F.col("_shipment_rank") == 1
    )

    .select(
        "order_id",
        "shipment_id",
        "tracking_number",
        "carrier",
        "shipment_status",
        "shipped_date",
        "expected_delivery_date",
        "actual_delivery_date",
        "delivery_delay_days",
        "is_late_delivery",

        F.col("updated_at").alias(
            "shipment_updated_at"
        )
    )
)

shipment_summary_per_order = (
    shipments_staging
    .groupBy("order_id")
    .agg(
        F.count("shipment_id").alias(
            "shipment_count"
        ),

        F.max("delivery_delay_days").alias(
            "maximum_delivery_delay_days"
        ),

        F.max(
            F.when(
                F.col("is_late_delivery") == True,
                F.lit(1)
            ).otherwise(
                F.lit(0)
            )
        ).alias("_any_late_delivery")
    )

    .withColumn(
        "has_any_late_delivery",
        F.col("_any_late_delivery") == 1
    )

    .drop("_any_late_delivery")
)

shipments_for_fact = (
    latest_shipment_per_order
    .join(
        shipment_summary_per_order,
        on="order_id",
        how="left"
    )
)

print(
    f"Orders with valid shipment records: "
    f"{shipments_for_fact.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27D: Prepare customer attributes for the fact table
# ============================================================

customers_for_fact = (
    spark.table("dim_customers")
    .select(
        "customer_id",
        "customer_full_name",
        "customer_segment",
        "customer_status",
        "state",
        "region",
        "country",
        "signup_date"
    )
)

print("Customer dimension attributes prepared.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27E: Build final fact_orders table
# Grain: One row per order_id
# ============================================================

zero_decimal = F.lit(0).cast(
    T.DecimalType(20, 2)
)

fact_orders = (
    orders_staging.alias("orders")

    # Add customer attributes
    .join(
        customers_for_fact.alias("customers"),
        on="customer_id",
        how="left"
    )

    # Add aggregated sales values
    .join(
        order_line_summary.alias("lines"),
        on="order_id",
        how="left"
    )

    # Add payment information
    .join(
        payments_for_fact.alias("payments"),
        on="order_id",
        how="left"
    )

    # Add shipment information
    .join(
        shipments_for_fact.alias("shipments"),
        on="order_id",
        how="left"
    )

    # Replace missing financial aggregates with zero
    .withColumn(
        "gross_sales",
        F.coalesce(
            F.col("gross_sales"),
            zero_decimal
        )
    )

    .withColumn(
        "discount_amount",
        F.coalesce(
            F.col("discount_amount"),
            zero_decimal
        )
    )

    .withColumn(
        "net_sales",
        F.coalesce(
            F.col("net_sales"),
            zero_decimal
        )
    )

    .withColumn(
        "tax_amount",
        F.coalesce(
            F.col("tax_amount"),
            zero_decimal
        )
    )

    .withColumn(
        "line_total_after_tax",
        F.coalesce(
            F.col("line_total_after_tax"),
            zero_decimal
        )
    )

    .withColumn(
        "shipping_cost",
        F.coalesce(
            F.col("shipping_cost").cast(
                T.DecimalType(20, 2)
            ),
            zero_decimal
        )
    )

    # Shipping cost is added once per order
    .withColumn(
        "order_total_amount",
        F.round(
            F.col("net_sales")
            + F.col("tax_amount")
            + F.col("shipping_cost"),
            2
        ).cast(
            T.DecimalType(20, 2)
        )
    )

    # Flags for missing downstream events
    .withColumn(
        "has_valid_order_lines",
        F.coalesce(
            F.col("order_line_count") > 0,
            F.lit(False)
        )
    )

    .withColumn(
        "has_payment",
        F.col("payment_id").isNotNull()
    )

    .withColumn(
        "has_shipment",
        F.col("shipment_id").isNotNull()
    )

    # Friendly status defaults
    .withColumn(
        "payment_status",
        F.coalesce(
            F.col("payment_status"),
            F.lit("NO_PAYMENT")
        )
    )

    .withColumn(
        "shipment_status",
        F.coalesce(
            F.col("shipment_status"),
            F.lit("NOT_SHIPPED")
        )
    )

    # Useful analytics flags
    .withColumn(
        "is_cancelled",
        F.col("order_status") == "CANCELLED"
    )

    .withColumn(
        "is_payment_failure",
        F.col("payment_status") == "FAILED"
    )

    # Partition fields
    .withColumn(
        "order_year",
        F.year("order_date")
    )

    .withColumn(
        "order_month",
        F.month("order_date")
    )

    # Curated metadata
    .withColumn(
        "curated_run_id",
        F.lit(CURATED_RUN_ID)
    )

    .withColumn(
        "curated_at",
        F.current_timestamp()
    )

    .select(
        "order_id",
        "customer_id",
        "customer_full_name",
        "customer_segment",
        "customer_status",
        "state",
        "region",
        "country",
        "signup_date",

        "order_date",
        "order_year",
        "order_month",
        "order_status",
        "order_channel",
        "currency_code",

        "order_line_count",
        "distinct_product_count",
        "total_quantity",

        "gross_sales",
        "discount_amount",
        "net_sales",
        "tax_amount",
        "shipping_cost",
        "order_total_amount",

        "payment_id",
        "transaction_id",
        "payment_method",
        "payment_amount",
        "payment_status",
        "failure_reason",
        "payment_date",
        "payment_attempt_count",
        "total_paid_amount",
        "total_refunded_amount",

        "shipment_id",
        "tracking_number",
        "carrier",
        "shipment_status",
        "shipped_date",
        "expected_delivery_date",
        "actual_delivery_date",
        "delivery_delay_days",
        "is_late_delivery",
        "maximum_delivery_delay_days",
        "has_any_late_delivery",

        "has_valid_order_lines",
        "has_payment",
        "has_shipment",
        "is_cancelled",
        "is_payment_failure",

        "curated_run_id",
        "curated_at"
    )
)

print("Final fact_orders DataFrame created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27F: Validate fact_orders
# ============================================================

valid_order_source_count = (
    orders_staging.count()
)

fact_order_count = (
    fact_orders.count()
)

distinct_fact_order_count = (
    fact_orders
    .select("order_id")
    .distinct()
    .count()
)

orders_without_valid_lines = (
    fact_orders
    .filter(
        F.col("has_valid_order_lines") == False
    )
    .count()
)

orders_without_payment = (
    fact_orders
    .filter(
        F.col("has_payment") == False
    )
    .count()
)

orders_without_shipment = (
    fact_orders
    .filter(
        F.col("has_shipment") == False
    )
    .count()
)

print(
    f"Valid staging orders: "
    f"{valid_order_source_count:,}"
)

print(
    f"Fact order rows: "
    f"{fact_order_count:,}"
)

print(
    f"Distinct fact order IDs: "
    f"{distinct_fact_order_count:,}"
)

print(
    "Order source-to-target reconciliation passed:",
    valid_order_source_count
    == fact_order_count
)

print(
    "Fact order grain uniqueness passed:",
    fact_order_count
    == distinct_fact_order_count
)

print(
    f"Orders without valid order lines: "
    f"{orders_without_valid_lines:,}"
)

print(
    f"Orders without valid payments: "
    f"{orders_without_payment:,}"
)

print(
    f"Orders without shipments: "
    f"{orders_without_shipment:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27G: Review fact_orders results
# ============================================================

display(
    fact_orders
    .select(
        "order_id",
        "customer_id",
        "region",
        "order_date",
        "order_status",
        "order_line_count",
        "total_quantity",
        "gross_sales",
        "discount_amount",
        "net_sales",
        "tax_amount",
        "shipping_cost",
        "order_total_amount",
        "payment_status",
        "shipment_status",
        "delivery_delay_days",
        "is_late_delivery"
    )
    .orderBy("order_id")
    .limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 27H: Write final fact_orders table
# ============================================================

# Delta table for Fabric SQL and Lakehouse analytics
(
    fact_orders
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy(
        "order_year",
        "order_month"
    )
    .saveAsTable("fact_orders")
)

# Explicit partitioned Parquet output required by assignment
(
    fact_orders
    .write
    .mode("overwrite")
    .partitionBy(
        "order_year",
        "order_month"
    )
    .parquet(
        CURATED_FILE_PATHS["fact_orders"]
    )
)

print("Table created: fact_orders")

print(
    "Partitioned Parquet output:",
    CURATED_FILE_PATHS["fact_orders"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
