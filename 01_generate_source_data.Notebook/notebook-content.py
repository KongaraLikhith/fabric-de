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
# Retail High-Volume Data Engineering Assessment
# Notebook: 01_generate_source_data
# Purpose: Create the Lakehouse folder structure
# ============================================================

print(f"Spark version: {spark.version}")
print("Default Lakehouse: retail_lakehouse")

folders = [
    "Files/raw/customers",
    "Files/raw/products",
    "Files/raw/orders",
    "Files/raw/order_items",
    "Files/raw/payments",
    "Files/raw/shipments",
    "Files/quarantine",
    "Files/curated",
    "Files/metadata",
    "Files/reports",
    "Files/sample_outputs"
]

for folder in folders:
    notebookutils.fs.mkdirs(folder)
    print(f"Created or verified: {folder}")

print("\nProject folder setup completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 2: Pipeline configuration
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

# Keep this True while developing and testing.
# Later, change it to False for the final 20-million-row run.
DEV_MODE = False

RANDOM_SEED = 42

# Development-scale data
DEV_CONFIG = {
    "customers": 10_000,
    "products": 2_000,
    "orders": 50_000,
    "order_items": 200_000,
    "payments": 50_000,
    "shipments": 45_000,
    "number_of_days": 5
}

# Final assessment-scale data
FULL_CONFIG = {
    "customers": 1_000_000,
    "products": 100_000,
    "orders": 5_000_000,
    "order_items": 20_000_000,
    "payments": 5_000_000,
    "shipments": 4_500_000,
    "number_of_days": 30
}

CONFIG = DEV_CONFIG if DEV_MODE else FULL_CONFIG

START_DATE = "2026-01-01"

PATHS = {
    "customers": "Files/raw/customers",
    "products": "Files/raw/products",
    "orders": "Files/raw/orders",
    "order_items": "Files/raw/order_items",
    "payments": "Files/raw/payments",
    "shipments": "Files/raw/shipments",
    "quarantine": "Files/quarantine",
    "curated": "Files/curated",
    "metadata": "Files/metadata",
    "reports": "Files/reports",
    "sample_outputs": "Files/sample_outputs"
}

print("Execution mode:", "DEVELOPMENT" if DEV_MODE else "FULL SCALE")
print("Start date:", START_DATE)
print("Random seed:", RANDOM_SEED)

for dataset, row_count in CONFIG.items():
    print(f"{dataset}: {row_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 3: Generate raw customers data
# ============================================================

from pyspark.sql import functions as F

customer_count = CONFIG["customers"]
number_of_days = CONFIG["number_of_days"]

# Small reference lists used to generate realistic values
first_names = [
    "Aarav", "Emma", "Liam", "Olivia", "Noah",
    "Sophia", "Ethan", "Mia", "Lucas", "Ava"
]

last_names = [
    "Patel", "Smith", "Johnson", "Williams", "Brown",
    "Jones", "Garcia", "Miller", "Davis", "Wilson"
]

states = [
    "NY", "NJ", "PA",
    "TX", "FL", "GA",
    "OH", "IL", "MI",
    "CA", "WA", "AZ"
]

regions = [
    "Northeast", "Northeast", "Northeast",
    "South", "South", "South",
    "Midwest", "Midwest", "Midwest",
    "West", "West", "West"
]

segments = [
    "CONSUMER",
    "CORPORATE",
    "SMALL_BUSINESS"
]

first_name_values = F.array(*[F.lit(value) for value in first_names])
last_name_values = F.array(*[F.lit(value) for value in last_names])
state_values = F.array(*[F.lit(value) for value in states])
region_values = F.array(*[F.lit(value) for value in regions])
segment_values = F.array(*[F.lit(value) for value in segments])

# Create one base record for every customer
customers_base = (
    spark.range(1, customer_count + 1)
    .withColumnRenamed("id", "customer_id")

    # Temporary lookup indexes
    .withColumn(
        "_first_name_index",
        (
            F.pmod(F.col("customer_id"), F.lit(len(first_names))) + 1
        ).cast("int")
    )
    .withColumn(
        "_last_name_index",
        (
            F.pmod(F.col("customer_id") * 3, F.lit(len(last_names))) + 1
        ).cast("int")
    )
    .withColumn(
        "_state_index",
        (
            F.pmod(F.col("customer_id"), F.lit(len(states))) + 1
        ).cast("int")
    )
    .withColumn(
        "_segment_index",
        (
            F.pmod(F.col("customer_id"), F.lit(len(segments))) + 1
        ).cast("int")
    )

    # Customer descriptive fields
    .withColumn(
        "first_name",
        F.element_at(first_name_values, F.col("_first_name_index"))
    )
    .withColumn(
        "last_name",
        F.element_at(last_name_values, F.col("_last_name_index"))
    )
    .withColumn(
        "state",
        F.element_at(state_values, F.col("_state_index"))
    )
    .withColumn(
        "region",
        F.element_at(region_values, F.col("_state_index"))
    )
    .withColumn(
        "customer_segment",
        F.element_at(segment_values, F.col("_segment_index"))
    )
    .withColumn("country", F.lit("US"))

    # Generate email and phone
    .withColumn(
        "email",
        F.concat(
            F.lower(F.col("first_name")),
            F.lit("."),
            F.lower(F.col("last_name")),
            F.col("customer_id").cast("string"),
            F.lit("@example.com")
        )
    )
    .withColumn(
        "phone",
        F.concat(
            F.lit("+1-555-"),
            F.lpad(
                F.pmod(
                    F.col("customer_id"),
                    F.lit(10_000)
                ).cast("string"),
                4,
                "0"
            )
        )
    )

    # Assign every record to a daily source partition
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("customer_id") - 1,
                F.lit(number_of_days)
            ).cast("int")
        )
    )

    # Generate a clean signup date temporarily
    .withColumn(
        "_signup_date_clean",
        F.date_sub(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("customer_id"),
                F.lit(1_095)
            ).cast("int")
        )
    )

    # Intentionally create inconsistent source date formats
    .withColumn(
        "signup_date",
        F.when(
            F.pmod(F.col("customer_id"), F.lit(10)) < 7,
            F.date_format(F.col("_signup_date_clean"), "yyyy-MM-dd")
        )
        .when(
            F.pmod(F.col("customer_id"), F.lit(10)) < 9,
            F.date_format(F.col("_signup_date_clean"), "MM/dd/yyyy")
        )
        .otherwise(
            F.date_format(F.col("_signup_date_clean"), "dd-MMM-yyyy")
        )
    )

    # Record update timestamp
    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.concat(
                F.date_format(F.col("load_date"), "yyyy-MM-dd"),
                F.lit(" "),
                F.lpad(
                    F.pmod(
                        F.col("customer_id"),
                        F.lit(24)
                    ).cast("string"),
                    2,
                    "0"
                ),
                F.lit(":00:00")
            )
        )
    )

    # Valid status distribution
    .withColumn(
        "customer_status",
        F.when(
            F.pmod(F.col("customer_id"), F.lit(10)) < 8,
            F.lit("ACTIVE")
        )
        .when(
            F.pmod(F.col("customer_id"), F.lit(10)) < 9,
            F.lit("INACTIVE")
        )
        .otherwise(F.lit("SUSPENDED"))
    )

    # Intentionally introduce missing and invalid values
    .withColumn(
        "email",
        F.when(
            F.pmod(F.col("customer_id"), F.lit(200)) == 0,
            F.lit(None).cast("string")
        ).otherwise(F.col("email"))
    )
    .withColumn(
        "region",
        F.when(
            F.pmod(F.col("customer_id"), F.lit(333)) == 0,
            F.lit(None).cast("string")
        ).otherwise(F.col("region"))
    )
    .withColumn(
        "customer_status",
        F.when(
            F.pmod(F.col("customer_id"), F.lit(500)) == 0,
            F.lit("INVALID_STATUS")
        ).otherwise(F.col("customer_status"))
    )

    # Remove temporary working columns
    .drop(
        "_first_name_index",
        "_last_name_index",
        "_state_index",
        "_segment_index",
        "_signup_date_clean"
    )
)

# Create duplicate customer records for approximately 1% of customers.
# These represent later updates to an existing customer.
customer_updates = (
    customers_base
    .filter(
        F.pmod(F.col("customer_id"), F.lit(100)) == 0
    )
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("customer_id"),
                F.lit(number_of_days)
            ).cast("int")
        )
    )
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 1 DAY")
    )
    .withColumn(
        "email",
        F.when(
            F.col("email").isNotNull(),
            F.concat(F.lit("updated_"), F.col("email"))
        ).otherwise(F.col("email"))
    )
)

# Combine original records and duplicate updates
customers_raw = customers_base.unionByName(customer_updates)

# Write the raw source as CSV files partitioned by load date
(
    customers_raw
    .repartition(number_of_days, "load_date")
    .write
    .mode("overwrite")
    .option("header", "true")
    .partitionBy("load_date")
    .csv(PATHS["customers"])
)

print("Customers dataset generated successfully.")
print(f"Base customers: {customer_count:,}")
print(f"Expected duplicate updates: approximately {customer_count // 100:,}")
print(f"Output location: {PATHS['customers']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 4: Validate raw customers output
# ============================================================

customers_check = (
    spark.read
    .option("header", "true")
    .csv(PATHS["customers"])
)

# Basic row-count checks
total_rows = customers_check.count()
distinct_customers = (
    customers_check
    .select("customer_id")
    .distinct()
    .count()
)

duplicate_rows = total_rows - distinct_customers

print(f"Total raw rows: {total_rows:,}")
print(f"Distinct customer IDs: {distinct_customers:,}")
print(f"Duplicate rows: {duplicate_rows:,}")

# Check records per daily partition
print("\nRows by load date:")

display(
    customers_check
    .groupBy("load_date")
    .count()
    .orderBy("load_date")
)

# Check intentional data-quality problems
quality_summary = customers_check.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(F.col("email").isNull(), 1).otherwise(0)
    ).alias("missing_email_rows"),

    F.sum(
        F.when(F.col("region").isNull(), 1).otherwise(0)
    ).alias("missing_region_rows"),

    F.sum(
        F.when(
            F.col("customer_status") == "INVALID_STATUS",
            1
        ).otherwise(0)
    ).alias("invalid_status_rows")
)

display(quality_summary)

# Show customer IDs that appear more than once
display(
    customers_check
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy("customer_id")
    .limit(10)
)

# Show a sample of the raw source data
display(
    customers_check
    .orderBy("customer_id", "updated_at")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 5: Generate raw products data
# ============================================================

product_count = CONFIG["products"]
number_of_days = CONFIG["number_of_days"]

categories = [
    "Electronics",
    "Home",
    "Clothing",
    "Sports",
    "Beauty"
]

subcategories = [
    "Accessories",
    "Kitchen",
    "Apparel",
    "Fitness",
    "Personal Care"
]

brands = [
    "Nova",
    "Vertex",
    "Prime",
    "Summit",
    "Metro",
    "Apex"
]

category_values = F.array(
    *[F.lit(value) for value in categories]
)

subcategory_values = F.array(
    *[F.lit(value) for value in subcategories]
)

brand_values = F.array(
    *[F.lit(value) for value in brands]
)

products_base = (
    spark.range(1, product_count + 1)
    .withColumnRenamed("id", "product_id")

    # Temporary indexes used to choose values from reference lists
    .withColumn(
        "_category_index",
        (
            F.pmod(
                F.col("product_id"),
                F.lit(len(categories))
            ) + 1
        ).cast("int")
    )
    .withColumn(
        "_brand_index",
        (
            F.pmod(
                F.col("product_id"),
                F.lit(len(brands))
            ) + 1
        ).cast("int")
    )

    # Product identifiers
    .withColumn(
        "sku",
        F.concat(
            F.lit("SKU-"),
            F.lpad(
                F.col("product_id").cast("string"),
                8,
                "0"
            )
        )
    )
    .withColumn(
        "product_name",
        F.concat(
            F.lit("Product "),
            F.col("product_id").cast("string")
        )
    )

    # Product descriptive attributes
    .withColumn(
        "category",
        F.element_at(
            category_values,
            F.col("_category_index")
        )
    )
    .withColumn(
        "subcategory",
        F.element_at(
            subcategory_values,
            F.col("_category_index")
        )
    )
    .withColumn(
        "brand",
        F.element_at(
            brand_values,
            F.col("_brand_index")
        )
    )

    # Generate prices between approximately $5 and $1,000
    .withColumn(
        "unit_price",
        F.round(
            F.lit(5.00)
            + (
                F.pmod(
                    F.col("product_id") * 37,
                    F.lit(99_500)
                ).cast("double") / 100
            ),
            2
        )
    )
    .withColumn(
        "unit_cost",
        F.round(
            F.col("unit_price") * F.lit(0.60),
            2
        )
    )

    # Normal product statuses
    .withColumn(
        "product_status",
        F.when(
            F.pmod(
                F.col("product_id"),
                F.lit(10)
            ) < 9,
            F.lit("ACTIVE")
        ).otherwise(
            F.lit("DISCONTINUED")
        )
    )

    # Distribute source records across daily partitions
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("product_id") - 1,
                F.lit(number_of_days)
            ).cast("int")
        )
    )

    # Create a source update timestamp
    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.concat(
                F.date_format(
                    F.col("load_date"),
                    "yyyy-MM-dd"
                ),
                F.lit(" "),
                F.lpad(
                    F.pmod(
                        F.col("product_id"),
                        F.lit(24)
                    ).cast("string"),
                    2,
                    "0"
                ),
                F.lit(":00:00")
            )
        )
    )

    # Intentionally introduce negative prices
    .withColumn(
        "unit_price",
        F.when(
            F.pmod(
                F.col("product_id"),
                F.lit(400)
            ) == 0,
            -F.col("unit_price")
        ).otherwise(
            F.col("unit_price")
        )
    )

    # Intentionally introduce missing categories
    .withColumn(
        "category",
        F.when(
            F.pmod(
                F.col("product_id"),
                F.lit(333)
            ) == 0,
            F.lit(None).cast("string")
        ).otherwise(
            F.col("category")
        )
    )

    # Intentionally introduce invalid statuses
    .withColumn(
        "product_status",
        F.when(
            F.pmod(
                F.col("product_id"),
                F.lit(500)
            ) == 0,
            F.lit("INVALID_STATUS")
        ).otherwise(
            F.col("product_status")
        )
    )

    # Remove temporary working columns
    .drop(
        "_category_index",
        "_brand_index"
    )
)

# Create updated versions for approximately 1% of products
product_updates = (
    products_base
    .filter(
        F.pmod(
            F.col("product_id"),
            F.lit(100)
        ) == 0
    )
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 1 HOUR")
    )
    .withColumn(
        "unit_price",
        F.round(
            F.col("unit_price") * F.lit(1.05),
            2
        )
    )
)

# Combine original products with later updates
products_raw = products_base.unionByName(product_updates)

# Write raw product source files
(
    products_raw
    .repartition(number_of_days, "load_date")
    .write
    .mode("overwrite")
    .option("header", "true")
    .partitionBy("load_date")
    .csv(PATHS["products"])
)

print("Products dataset generated successfully.")
print(f"Base products: {product_count:,}")
print(
    f"Expected duplicate updates: "
    f"approximately {product_count // 100:,}"
)
print(f"Output location: {PATHS['products']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 6: Validate raw products dataset
# ============================================================

products_check = (
    spark.read
    .option("header", "true")
    .csv(PATHS["products"])
)

# ------------------------------------------------------------
# 1. Row-count and duplicate checks
# ------------------------------------------------------------

total_product_rows = products_check.count()

distinct_product_ids = (
    products_check
    .select("product_id")
    .distinct()
    .count()
)

duplicate_product_rows = (
    total_product_rows - distinct_product_ids
)

print(f"Total raw product rows: {total_product_rows:,}")
print(f"Distinct product IDs: {distinct_product_ids:,}")
print(f"Duplicate product records: {duplicate_product_rows:,}")

# ------------------------------------------------------------
# 2. Check records across daily load partitions
# ------------------------------------------------------------

print("\nProduct rows by load date:")

display(
    products_check
    .groupBy("load_date")
    .count()
    .orderBy("load_date")
)

# ------------------------------------------------------------
# 3. Check intentional data-quality problems
# ------------------------------------------------------------

product_quality_summary = products_check.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("category").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_category_rows"),

    F.sum(
        F.when(
            F.col("unit_price").cast("double") <= 0,
            1
        ).otherwise(0)
    ).alias("invalid_price_rows"),

    F.sum(
        F.when(
            F.col("product_status") == "INVALID_STATUS",
            1
        ).otherwise(0)
    ).alias("invalid_status_rows")
)

display(product_quality_summary)

# ------------------------------------------------------------
# 4. Show product IDs appearing more than once
# ------------------------------------------------------------

display(
    products_check
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy("product_id")
    .limit(20)
)

# ------------------------------------------------------------
# 5. Compare duplicate versions using updated_at
# ------------------------------------------------------------

duplicate_product_ids = (
    products_check
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .select("product_id")
)

display(
    products_check
    .join(
        duplicate_product_ids,
        on="product_id",
        how="inner"
    )
    .orderBy("product_id", "updated_at")
    .limit(20)
)

# ------------------------------------------------------------
# 6. Display sample product records
# ------------------------------------------------------------

display(
    products_check
    .orderBy("product_id", "updated_at")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 7: Generate raw orders data
# ============================================================

order_count = CONFIG["orders"]
customer_count = CONFIG["customers"]
number_of_days = CONFIG["number_of_days"]

order_channels = [
    "WEB",
    "MOBILE",
    "STORE",
    "MARKETPLACE"
]

currencies = [
    "USD"
]

channel_values = F.array(
    *[F.lit(value) for value in order_channels]
)

currency_values = F.array(
    *[F.lit(value) for value in currencies]
)

orders_base = (
    spark.range(1, order_count + 1)
    .withColumnRenamed("id", "order_id")

    # --------------------------------------------------------
    # Assign each order to an existing customer
    # --------------------------------------------------------
    .withColumn(
        "customer_id",
        (
            F.pmod(
                F.col("order_id") - 1,
                F.lit(customer_count)
            ) + 1
        ).cast("long")
    )

    # --------------------------------------------------------
    # Assign source load date
    # --------------------------------------------------------
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("order_id") - 1,
                F.lit(number_of_days)
            ).cast("int")
        )
    )

    # --------------------------------------------------------
    # Create clean business order date
    # Order may have occurred 0–2 days before ingestion
    # --------------------------------------------------------
    .withColumn(
        "_order_date_clean",
        F.date_sub(
            F.col("load_date"),
            F.pmod(
                F.col("order_id"),
                F.lit(3)
            ).cast("int")
        )
    )

    # --------------------------------------------------------
    # Intentionally create inconsistent source date formats
    # --------------------------------------------------------
    .withColumn(
        "order_date",
        F.when(
            F.pmod(F.col("order_id"), F.lit(10)) < 7,
            F.date_format(
                F.col("_order_date_clean"),
                "yyyy-MM-dd"
            )
        )
        .when(
            F.pmod(F.col("order_id"), F.lit(10)) < 9,
            F.date_format(
                F.col("_order_date_clean"),
                "MM/dd/yyyy"
            )
        )
        .otherwise(
            F.date_format(
                F.col("_order_date_clean"),
                "dd-MMM-yyyy"
            )
        )
    )

    # --------------------------------------------------------
    # Order channel
    # --------------------------------------------------------
    .withColumn(
        "_channel_index",
        (
            F.pmod(
                F.col("order_id"),
                F.lit(len(order_channels))
            ) + 1
        ).cast("int")
    )
    .withColumn(
        "order_channel",
        F.element_at(
            channel_values,
            F.col("_channel_index")
        )
    )

    # --------------------------------------------------------
    # Currency
    # --------------------------------------------------------
    .withColumn(
        "currency_code",
        F.lit("USD")
    )

    # --------------------------------------------------------
    # Create normal order statuses
    # --------------------------------------------------------
    .withColumn(
        "order_status",
        F.when(
            F.pmod(F.col("order_id"), F.lit(20)) < 12,
            F.lit("COMPLETED")
        )
        .when(
            F.pmod(F.col("order_id"), F.lit(20)) < 16,
            F.lit("SHIPPED")
        )
        .when(
            F.pmod(F.col("order_id"), F.lit(20)) < 18,
            F.lit("CANCELLED")
        )
        .otherwise(
            F.lit("PENDING")
        )
    )

    # --------------------------------------------------------
    # Discount percentage applied at order level
    # Values: 0%, 5%, 10%, or 15%
    # --------------------------------------------------------
    .withColumn(
        "discount_pct",
        (
            F.pmod(
                F.col("order_id"),
                F.lit(4)
            ) * 5
        ).cast("double")
    )

    # --------------------------------------------------------
    # Tax rate
    # --------------------------------------------------------
    .withColumn(
        "tax_rate",
        F.when(
            F.pmod(F.col("order_id"), F.lit(3)) == 0,
            F.lit(0.06)
        )
        .when(
            F.pmod(F.col("order_id"), F.lit(3)) == 1,
            F.lit(0.07)
        )
        .otherwise(
            F.lit(0.08)
        )
    )

    # --------------------------------------------------------
    # Shipping cost
    # --------------------------------------------------------
    .withColumn(
        "shipping_cost",
        F.when(
            F.pmod(F.col("order_id"), F.lit(10)) == 0,
            F.lit(0.00)
        )
        .otherwise(
            F.round(
                F.lit(4.99) +
                (
                    F.pmod(
                        F.col("order_id"),
                        F.lit(1_000)
                    ).cast("double") / 100
                ),
                2
            )
        )
    )

    # --------------------------------------------------------
    # Source update timestamp
    # --------------------------------------------------------
    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.concat(
                F.date_format(
                    F.col("load_date"),
                    "yyyy-MM-dd"
                ),
                F.lit(" "),
                F.lpad(
                    F.pmod(
                        F.col("order_id"),
                        F.lit(24)
                    ).cast("string"),
                    2,
                    "0"
                ),
                F.lit(":00:00")
            )
        )
    )

    # --------------------------------------------------------
    # Intentionally create missing customer IDs
    # --------------------------------------------------------
    .withColumn(
        "customer_id",
        F.when(
            F.pmod(
                F.col("order_id"),
                F.lit(1_000)
            ) == 0,
            F.lit(None).cast("long")
        )
        .otherwise(
            F.col("customer_id")
        )
    )

    # --------------------------------------------------------
    # Intentionally create nonexistent customer IDs
    # --------------------------------------------------------
    .withColumn(
        "customer_id",
        F.when(
            F.pmod(
                F.col("order_id"),
                F.lit(777)
            ) == 0,
            (
                F.lit(customer_count) +
                F.col("order_id")
            ).cast("long")
        )
        .otherwise(
            F.col("customer_id")
        )
    )

    # --------------------------------------------------------
    # Intentionally create invalid statuses
    # --------------------------------------------------------
    .withColumn(
        "order_status",
        F.when(
            F.pmod(
                F.col("order_id"),
                F.lit(2_500)
            ) == 0,
            F.lit("UNKNOWN_STATUS")
        )
        .otherwise(
            F.col("order_status")
        )
    )

    # Remove temporary columns
    .drop(
        "_order_date_clean",
        "_channel_index"
    )
)

# ------------------------------------------------------------
# Create duplicate order updates for approximately 1% of orders
# ------------------------------------------------------------

order_updates = (
    orders_base
    .filter(
        F.pmod(
            F.col("order_id"),
            F.lit(100)
        ) == 0
    )

    # Simulate the update arriving in the next daily load
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("order_id"),
                F.lit(number_of_days)
            ).cast("int")
        )
    )

    # Updated version has a later timestamp
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 2 HOURS")
    )

    # Simulate an order status progressing
    .withColumn(
        "order_status",
        F.when(
            F.col("order_status") == "PENDING",
            F.lit("SHIPPED")
        )
        .when(
            F.col("order_status") == "SHIPPED",
            F.lit("COMPLETED")
        )
        .otherwise(
            F.col("order_status")
        )
    )
)

# Combine original orders and updated versions
orders_raw = orders_base.unionByName(order_updates)

# ------------------------------------------------------------
# Write raw orders across daily partitions
# ------------------------------------------------------------

(
    orders_raw
    .repartition(number_of_days, "load_date")
    .write
    .mode("overwrite")
    .option("header", "true")
    .partitionBy("load_date")
    .csv(PATHS["orders"])
)

print("Orders dataset generated successfully.")
print(f"Base orders: {order_count:,}")
print(
    f"Expected duplicate updates: "
    f"approximately {order_count // 100:,}"
)
print(f"Output location: {PATHS['orders']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 8: Validate raw orders dataset
# ============================================================

orders_check = (
    spark.read
    .option("header", "true")
    .csv(PATHS["orders"])
)

# ------------------------------------------------------------
# 1. Row-count and duplicate reconciliation
# ------------------------------------------------------------

total_order_rows = orders_check.count()

distinct_order_ids = (
    orders_check
    .select("order_id")
    .distinct()
    .count()
)

duplicate_order_rows = total_order_rows - distinct_order_ids

print(f"Total raw order rows: {total_order_rows:,}")
print(f"Distinct order IDs: {distinct_order_ids:,}")
print(f"Duplicate order records: {duplicate_order_rows:,}")

# ------------------------------------------------------------
# 2. Confirm daily storage partitions
# ------------------------------------------------------------

print("\nOrder rows by load date:")

display(
    orders_check
    .groupBy("load_date")
    .count()
    .orderBy("load_date")
)

# ------------------------------------------------------------
# 3. Check source-level data-quality issues
# ------------------------------------------------------------

valid_order_date_format = (
    F.col("order_date").rlike(r"^\d{4}-\d{2}-\d{2}$")
    | F.col("order_date").rlike(r"^\d{2}/\d{2}/\d{4}$")
    | F.col("order_date").rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$")
)

order_quality_summary = orders_check.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("customer_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_customer_id_rows"),

    F.sum(
        F.when(
            F.col("order_status") == "UNKNOWN_STATUS",
            1
        ).otherwise(0)
    ).alias("invalid_status_rows"),

    F.sum(
        F.when(
            ~valid_order_date_format,
            1
        ).otherwise(0)
    ).alias("unsupported_date_format_rows")
)

display(order_quality_summary)

# ------------------------------------------------------------
# 4. Referential-integrity check:
#    Find customer IDs in orders that do not exist in customers
# ------------------------------------------------------------

customer_keys = (
    customers_check
    .select("customer_id")
    .filter(F.col("customer_id").isNotNull())
    .distinct()
)

orders_with_customer_id = (
    orders_check
    .filter(F.col("customer_id").isNotNull())
)

unmatched_customer_orders = (
    orders_with_customer_id
    .join(
        customer_keys,
        on="customer_id",
        how="left_anti"
    )
)

unmatched_customer_count = unmatched_customer_orders.count()

print(
    f"Orders referencing nonexistent customers: "
    f"{unmatched_customer_count:,}"
)

display(
    unmatched_customer_orders
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "load_date"
    )
    .orderBy("order_id")
    .limit(20)
)

# ------------------------------------------------------------
# 5. Show duplicated business keys
# ------------------------------------------------------------

duplicate_order_ids = (
    orders_check
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

display(
    duplicate_order_ids
    .orderBy("order_id")
    .limit(20)
)

# ------------------------------------------------------------
# 6. Compare the original and updated order versions
# ------------------------------------------------------------

display(
    orders_check
    .join(
        duplicate_order_ids.select("order_id"),
        on="order_id",
        how="inner"
    )
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_date",
        "updated_at",
        "load_date"
    )
    .orderBy("order_id", "updated_at")
    .limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 9: Generate raw order_items data
# ============================================================

order_item_count = CONFIG["order_items"]
order_count = CONFIG["orders"]
product_count = CONFIG["products"]
number_of_days = CONFIG["number_of_days"]

order_items_base = (
    spark.range(1, order_item_count + 1)
    .withColumnRenamed("id", "order_item_id")

    # --------------------------------------------------------
    # Assign each line item to an order
    # --------------------------------------------------------
    .withColumn(
        "order_id",
        (
            F.pmod(
                F.col("order_item_id") - 1,
                F.lit(order_count)
            ) + 1
        ).cast("long")
    )

    # --------------------------------------------------------
    # Assign each line item to a product
    # --------------------------------------------------------
    .withColumn(
        "product_id",
        (
            F.pmod(
                F.col("order_item_id") * 17,
                F.lit(product_count)
            ) + 1
        ).cast("long")
    )

    # --------------------------------------------------------
    # Generate line number within each order
    # Development mode averages four lines per order
    # --------------------------------------------------------
    .withColumn(
        "line_number",
        (
            F.pmod(
                F.col("order_item_id") - 1,
                F.lit(10)
            ) + 1
        ).cast("int")
    )

    # --------------------------------------------------------
    # Generate normal quantities between 1 and 5
    # --------------------------------------------------------
    .withColumn(
        "quantity",
        (
            F.pmod(
                F.col("order_item_id"),
                F.lit(5)
            ) + 1
        ).cast("int")
    )

    # --------------------------------------------------------
    # Generate a realistic item price
    # Approximately $5 to $1,000
    # --------------------------------------------------------
    .withColumn(
        "unit_price",
        F.round(
            F.lit(5.00)
            + (
                F.pmod(
                    F.col("product_id") * 37,
                    F.lit(99_500)
                ).cast("double") / 100
            ),
            2
        )
    )

    # --------------------------------------------------------
    # Item-level discount percentage
    # Values: 0%, 5%, 10%, or 15%
    # --------------------------------------------------------
    .withColumn(
        "discount_pct",
        (
            F.pmod(
                F.col("order_item_id"),
                F.lit(4)
            ) * 5
        ).cast("double")
    )

    # --------------------------------------------------------
    # Assign source load date
    # --------------------------------------------------------
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("order_item_id") - 1,
                F.lit(number_of_days)
            ).cast("int")
        )
    )

    # --------------------------------------------------------
    # Create source update timestamp
    # --------------------------------------------------------
    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.concat(
                F.date_format(
                    F.col("load_date"),
                    "yyyy-MM-dd"
                ),
                F.lit(" "),
                F.lpad(
                    F.pmod(
                        F.col("order_item_id"),
                        F.lit(24)
                    ).cast("string"),
                    2,
                    "0"
                ),
                F.lit(":00:00")
            )
        )
    )

    # --------------------------------------------------------
    # Intentionally create negative quantities
    # --------------------------------------------------------
    .withColumn(
        "quantity",
        F.when(
            F.pmod(
                F.col("order_item_id"),
                F.lit(1_000)
            ) == 0,
            -F.col("quantity")
        ).otherwise(
            F.col("quantity")
        )
    )

    # --------------------------------------------------------
    # Intentionally create zero quantities
    # --------------------------------------------------------
    .withColumn(
        "quantity",
        F.when(
            F.pmod(
                F.col("order_item_id"),
                F.lit(1_500)
            ) == 0,
            F.lit(0)
        ).otherwise(
            F.col("quantity")
        )
    )

    # --------------------------------------------------------
    # Intentionally create negative prices
    # --------------------------------------------------------
    .withColumn(
        "unit_price",
        F.when(
            F.pmod(
                F.col("order_item_id"),
                F.lit(2_000)
            ) == 0,
            -F.col("unit_price")
        ).otherwise(
            F.col("unit_price")
        )
    )

    # --------------------------------------------------------
    # Intentionally create missing product IDs
    # --------------------------------------------------------
    .withColumn(
        "product_id",
        F.when(
            F.pmod(
                F.col("order_item_id"),
                F.lit(2_500)
            ) == 0,
            F.lit(None).cast("long")
        ).otherwise(
            F.col("product_id")
        )
    )

    # --------------------------------------------------------
    # Intentionally create nonexistent product IDs
    # --------------------------------------------------------
    .withColumn(
        "product_id",
        F.when(
            F.pmod(
                F.col("order_item_id"),
                F.lit(1_777)
            ) == 0,
            (
                F.lit(product_count)
                + F.col("order_item_id")
            ).cast("long")
        ).otherwise(
            F.col("product_id")
        )
    )

    # --------------------------------------------------------
    # Intentionally create missing order IDs
    # --------------------------------------------------------
    .withColumn(
        "order_id",
        F.when(
            F.pmod(
                F.col("order_item_id"),
                F.lit(3_000)
            ) == 0,
            F.lit(None).cast("long")
        ).otherwise(
            F.col("order_id")
        )
    )

    # --------------------------------------------------------
    # Intentionally create nonexistent order IDs
    # --------------------------------------------------------
    .withColumn(
        "order_id",
        F.when(
            F.pmod(
                F.col("order_item_id"),
                F.lit(2_333)
            ) == 0,
            (
                F.lit(order_count)
                + F.col("order_item_id")
            ).cast("long")
        ).otherwise(
            F.col("order_id")
        )
    )
)

# ------------------------------------------------------------
# Create duplicate order-item records for approximately 0.5%
# ------------------------------------------------------------

order_item_duplicates = (
    order_items_base
    .filter(
        F.pmod(
            F.col("order_item_id"),
            F.lit(200)
        ) == 0
    )
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 1 HOUR")
    )
)

order_items_raw = (
    order_items_base
    .unionByName(order_item_duplicates)
)

# ------------------------------------------------------------
# Write raw order-items across daily partitions
# ------------------------------------------------------------

(
    order_items_raw
    .repartition(
        number_of_days * 4,
        "load_date"
    )
    .write
    .mode("overwrite")
    .option("header", "true")
    .partitionBy("load_date")
    .csv(PATHS["order_items"])
)

print("Order-items dataset generated successfully.")
print(f"Base order-item rows: {order_item_count:,}")
print(
    f"Expected duplicate records: "
    f"approximately {order_item_count // 200:,}"
)
print(f"Output location: {PATHS['order_items']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 10: Validate raw order_items dataset
# ============================================================

order_items_check = (
    spark.read
    .option("header", "true")
    .csv(PATHS["order_items"])
)

# ------------------------------------------------------------
# 1. Row counts and duplicates
# ------------------------------------------------------------

total_order_item_rows = order_items_check.count()

distinct_order_item_ids = (
    order_items_check
    .select("order_item_id")
    .distinct()
    .count()
)

duplicate_order_item_rows = (
    total_order_item_rows - distinct_order_item_ids
)

print(f"Total raw order-item rows: {total_order_item_rows:,}")
print(f"Distinct order-item IDs: {distinct_order_item_ids:,}")
print(f"Duplicate order-item records: {duplicate_order_item_rows:,}")

# ------------------------------------------------------------
# 2. Verify rows across daily partitions
# ------------------------------------------------------------

print("\nOrder-item rows by load date:")

display(
    order_items_check
    .groupBy("load_date")
    .count()
    .orderBy("load_date")
)

# ------------------------------------------------------------
# 3. Check invalid values
# CSV values are strings, so numeric fields must be cast.
# ------------------------------------------------------------

order_item_quality_summary = order_items_check.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("quantity").cast("int") <= 0,
            1
        ).otherwise(0)
    ).alias("invalid_quantity_rows"),

    F.sum(
        F.when(
            F.col("unit_price").cast("double") <= 0,
            1
        ).otherwise(0)
    ).alias("invalid_price_rows"),

    F.sum(
        F.when(
            F.col("order_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_order_id_rows"),

    F.sum(
        F.when(
            F.col("product_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_product_id_rows")
)

display(order_item_quality_summary)

# ------------------------------------------------------------
# 4. Build unique parent-key datasets
# ------------------------------------------------------------

valid_order_keys = (
    orders_check
    .select("order_id")
    .filter(F.col("order_id").isNotNull())
    .distinct()
)

valid_product_keys = (
    products_check
    .select("product_id")
    .filter(F.col("product_id").isNotNull())
    .distinct()
)

# ------------------------------------------------------------
# 5. Find order items referencing nonexistent orders
# ------------------------------------------------------------

unmatched_order_items = (
    order_items_check
    .filter(F.col("order_id").isNotNull())
    .join(
        valid_order_keys,
        on="order_id",
        how="left_anti"
    )
)

unmatched_order_count = unmatched_order_items.count()

print(
    f"Order items referencing nonexistent orders: "
    f"{unmatched_order_count:,}"
)

display(
    unmatched_order_items
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "load_date"
    )
    .orderBy("order_item_id")
    .limit(20)
)

# ------------------------------------------------------------
# 6. Find order items referencing nonexistent products
# ------------------------------------------------------------

unmatched_product_items = (
    order_items_check
    .filter(F.col("product_id").isNotNull())
    .join(
        valid_product_keys,
        on="product_id",
        how="left_anti"
    )
)

unmatched_product_count = unmatched_product_items.count()

print(
    f"Order items referencing nonexistent products: "
    f"{unmatched_product_count:,}"
)

display(
    unmatched_product_items
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "load_date"
    )
    .orderBy("order_item_id")
    .limit(20)
)

# ------------------------------------------------------------
# 7. Show duplicate order-item IDs
# ------------------------------------------------------------

duplicate_order_item_ids = (
    order_items_check
    .groupBy("order_item_id")
    .count()
    .filter(F.col("count") > 1)
)

display(
    duplicate_order_item_ids
    .orderBy("order_item_id")
    .limit(20)
)

# ------------------------------------------------------------
# 8. Verify line structure for sample orders
# ------------------------------------------------------------

display(
    order_items_check
    .filter(
        F.col("order_id").cast("long").between(1, 5)
    )
    .select(
        "order_item_id",
        "order_id",
        "line_number",
        "product_id",
        "quantity",
        "unit_price"
    )
    .orderBy(
        F.col("order_id").cast("long"),
        F.col("line_number").cast("int")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 11: Generate raw payments data
# ============================================================

payment_count = CONFIG["payments"]
order_count = CONFIG["orders"]
number_of_days = CONFIG["number_of_days"]

payment_methods = [
    "CREDIT_CARD",
    "DEBIT_CARD",
    "PAYPAL",
    "APPLE_PAY",
    "GIFT_CARD"
]

payment_method_values = F.array(
    *[F.lit(value) for value in payment_methods]
)

payments_base = (
    spark.range(1, payment_count + 1)
    .withColumnRenamed("id", "payment_id")

    # --------------------------------------------------------
    # Connect every payment to an order
    # --------------------------------------------------------
    .withColumn(
        "order_id",
        (
            F.pmod(
                F.col("payment_id") - 1,
                F.lit(order_count)
            ) + 1
        ).cast("long")
    )

    # --------------------------------------------------------
    # Create a unique gateway transaction reference
    # --------------------------------------------------------
    .withColumn(
        "transaction_id",
        F.concat(
            F.lit("TXN-"),
            F.lpad(
                F.col("payment_id").cast("string"),
                10,
                "0"
            )
        )
    )

    # --------------------------------------------------------
    # Select payment method
    # --------------------------------------------------------
    .withColumn(
        "_payment_method_index",
        (
            F.pmod(
                F.col("payment_id"),
                F.lit(len(payment_methods))
            ) + 1
        ).cast("int")
    )
    .withColumn(
        "payment_method",
        F.element_at(
            payment_method_values,
            F.col("_payment_method_index")
        )
    )

    # --------------------------------------------------------
    # Generate payment amount
    # Approximately $20 to $2,020
    # --------------------------------------------------------
    .withColumn(
        "payment_amount",
        F.round(
            F.lit(20.00)
            + (
                F.pmod(
                    F.col("order_id") * 53,
                    F.lit(200_000)
                ).cast("double") / 100
            ),
            2
        )
    )

    # --------------------------------------------------------
    # Generate normal payment statuses
    # --------------------------------------------------------
    .withColumn(
        "payment_status",
        F.when(
            F.pmod(F.col("payment_id"), F.lit(20)) < 15,
            F.lit("PAID")
        )
        .when(
            F.pmod(F.col("payment_id"), F.lit(20)) < 17,
            F.lit("FAILED")
        )
        .when(
            F.pmod(F.col("payment_id"), F.lit(20)) < 19,
            F.lit("PENDING")
        )
        .otherwise(
            F.lit("REFUNDED")
        )
    )

    # --------------------------------------------------------
    # Failure reason only applies to failed payments
    # --------------------------------------------------------
    .withColumn(
        "failure_reason",
        F.when(
            F.col("payment_status") == "FAILED",
            F.when(
                F.pmod(F.col("payment_id"), F.lit(3)) == 0,
                F.lit("INSUFFICIENT_FUNDS")
            )
            .when(
                F.pmod(F.col("payment_id"), F.lit(3)) == 1,
                F.lit("CARD_DECLINED")
            )
            .otherwise(
                F.lit("GATEWAY_TIMEOUT")
            )
        ).otherwise(
            F.lit(None).cast("string")
        )
    )

    # --------------------------------------------------------
    # Assign source load date
    # --------------------------------------------------------
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("payment_id") - 1,
                F.lit(number_of_days)
            ).cast("int")
        )
    )

    # --------------------------------------------------------
    # Generate payment date in mixed source formats
    # --------------------------------------------------------
    .withColumn(
        "payment_date",
        F.when(
            F.pmod(F.col("payment_id"), F.lit(10)) < 7,
            F.date_format(F.col("load_date"), "yyyy-MM-dd")
        )
        .when(
            F.pmod(F.col("payment_id"), F.lit(10)) < 9,
            F.date_format(F.col("load_date"), "MM/dd/yyyy")
        )
        .otherwise(
            F.date_format(F.col("load_date"), "dd-MMM-yyyy")
        )
    )

    # --------------------------------------------------------
    # Create update timestamp
    # --------------------------------------------------------
    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.concat(
                F.date_format(F.col("load_date"), "yyyy-MM-dd"),
                F.lit(" "),
                F.lpad(
                    F.pmod(
                        F.col("payment_id"),
                        F.lit(24)
                    ).cast("string"),
                    2,
                    "0"
                ),
                F.lit(":00:00")
            )
        )
    )

    # --------------------------------------------------------
    # Intentionally create negative payment amounts
    # --------------------------------------------------------
    .withColumn(
        "payment_amount",
        F.when(
            F.pmod(
                F.col("payment_id"),
                F.lit(2_000)
            ) == 0,
            -F.col("payment_amount")
        ).otherwise(
            F.col("payment_amount")
        )
    )

    # --------------------------------------------------------
    # Intentionally create missing order IDs
    # --------------------------------------------------------
    .withColumn(
        "order_id",
        F.when(
            F.pmod(
                F.col("payment_id"),
                F.lit(2_500)
            ) == 0,
            F.lit(None).cast("long")
        ).otherwise(
            F.col("order_id")
        )
    )

    # --------------------------------------------------------
    # Intentionally create nonexistent order IDs
    # --------------------------------------------------------
    .withColumn(
        "order_id",
        F.when(
            F.pmod(
                F.col("payment_id"),
                F.lit(1_777)
            ) == 0,
            (
                F.lit(order_count)
                + F.col("payment_id")
            ).cast("long")
        ).otherwise(
            F.col("order_id")
        )
    )

    # --------------------------------------------------------
    # Intentionally create invalid payment statuses
    # --------------------------------------------------------
    .withColumn(
        "payment_status",
        F.when(
            F.pmod(
                F.col("payment_id"),
                F.lit(3_000)
            ) == 0,
            F.lit("UNKNOWN_STATUS")
        ).otherwise(
            F.col("payment_status")
        )
    )

    # --------------------------------------------------------
    # Intentionally create missing payment methods
    # --------------------------------------------------------
    .withColumn(
        "payment_method",
        F.when(
            F.pmod(
                F.col("payment_id"),
                F.lit(4_000)
            ) == 0,
            F.lit(None).cast("string")
        ).otherwise(
            F.col("payment_method")
        )
    )

    .drop("_payment_method_index")
)

# ------------------------------------------------------------
# Create updated payment records for approximately 1%
# These simulate gateway callbacks or payment retries.
# ------------------------------------------------------------

payment_updates = (
    payments_base
    .filter(
        F.pmod(
            F.col("payment_id"),
            F.lit(100)
        ) == 0
    )
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 2 HOURS")
    )
    .withColumn(
        "payment_status",
        F.when(
            F.col("payment_status").isin("FAILED", "PENDING"),
            F.lit("PAID")
        ).otherwise(
            F.col("payment_status")
        )
    )
    .withColumn(
        "failure_reason",
        F.when(
            F.col("payment_status") == "PAID",
            F.lit(None).cast("string")
        ).otherwise(
            F.col("failure_reason")
        )
    )
)

payments_raw = payments_base.unionByName(payment_updates)

# ------------------------------------------------------------
# Write raw payment files across daily partitions
# ------------------------------------------------------------

(
    payments_raw
    .repartition(
        number_of_days * 2,
        "load_date"
    )
    .write
    .mode("overwrite")
    .option("header", "true")
    .partitionBy("load_date")
    .csv(PATHS["payments"])
)

print("Payments dataset generated successfully.")
print(f"Base payment rows: {payment_count:,}")
print(
    f"Expected payment updates: "
    f"approximately {payment_count // 100:,}"
)
print(f"Output location: {PATHS['payments']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 12: Validate raw payments dataset
# ============================================================

payments_check = (
    spark.read
    .option("header", "true")
    .csv(PATHS["payments"])
)

# ------------------------------------------------------------
# 1. Row-count and duplicate checks
# ------------------------------------------------------------

total_payment_rows = payments_check.count()

distinct_payment_ids = (
    payments_check
    .select("payment_id")
    .distinct()
    .count()
)

duplicate_payment_rows = (
    total_payment_rows - distinct_payment_ids
)

print(f"Total raw payment rows: {total_payment_rows:,}")
print(f"Distinct payment IDs: {distinct_payment_ids:,}")
print(f"Duplicate payment records: {duplicate_payment_rows:,}")

# ------------------------------------------------------------
# 2. Confirm daily source partitions
# ------------------------------------------------------------

print("\nPayment rows by load date:")

display(
    payments_check
    .groupBy("load_date")
    .count()
    .orderBy("load_date")
)

# ------------------------------------------------------------
# 3. Check intentional data-quality problems
# CSV columns are strings, so payment_amount must be cast.
# ------------------------------------------------------------

payment_quality_summary = payments_check.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("payment_amount").cast("double") <= 0,
            1
        ).otherwise(0)
    ).alias("invalid_payment_amount_rows"),

    F.sum(
        F.when(
            F.col("order_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_order_id_rows"),

    F.sum(
        F.when(
            F.col("payment_method").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_payment_method_rows"),

    F.sum(
        F.when(
            F.col("payment_status") == "UNKNOWN_STATUS",
            1
        ).otherwise(0)
    ).alias("invalid_payment_status_rows")
)

display(payment_quality_summary)

# ------------------------------------------------------------
# 4. Count payment statuses
# ------------------------------------------------------------

display(
    payments_check
    .groupBy("payment_status")
    .count()
    .orderBy(F.desc("count"))
)

# ------------------------------------------------------------
# 5. Build valid order-key dataset
# ------------------------------------------------------------

valid_order_keys = (
    orders_check
    .select("order_id")
    .filter(F.col("order_id").isNotNull())
    .distinct()
)

# ------------------------------------------------------------
# 6. Find payments referencing nonexistent orders
# ------------------------------------------------------------

unmatched_payment_orders = (
    payments_check
    .filter(F.col("order_id").isNotNull())
    .join(
        valid_order_keys,
        on="order_id",
        how="left_anti"
    )
)

unmatched_payment_order_count = (
    unmatched_payment_orders.count()
)

print(
    f"Payments referencing nonexistent orders: "
    f"{unmatched_payment_order_count:,}"
)

display(
    unmatched_payment_orders
    .select(
        "payment_id",
        "order_id",
        "transaction_id",
        "payment_amount",
        "payment_status",
        "load_date"
    )
    .orderBy("payment_id")
    .limit(20)
)

# ------------------------------------------------------------
# 7. Show duplicated payment IDs
# ------------------------------------------------------------

duplicate_payment_ids = (
    payments_check
    .groupBy("payment_id")
    .count()
    .filter(F.col("count") > 1)
)

display(
    duplicate_payment_ids
    .orderBy("payment_id")
    .limit(20)
)

# ------------------------------------------------------------
# 8. Compare original and updated payment versions
# ------------------------------------------------------------

display(
    payments_check
    .join(
        duplicate_payment_ids.select("payment_id"),
        on="payment_id",
        how="inner"
    )
    .select(
        "payment_id",
        "order_id",
        "payment_method",
        "payment_amount",
        "payment_status",
        "failure_reason",
        "updated_at",
        "load_date"
    )
    .orderBy("payment_id", "updated_at")
    .limit(30)
)

# ------------------------------------------------------------
# 9. Examine failed payments by reason
# ------------------------------------------------------------

display(
    payments_check
    .filter(F.col("payment_status") == "FAILED")
    .groupBy("failure_reason")
    .count()
    .orderBy(F.desc("count"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 13: Generate raw shipments data
# ============================================================

shipment_count = CONFIG["shipments"]
order_count = CONFIG["orders"]
number_of_days = CONFIG["number_of_days"]

carriers = [
    "UPS",
    "FEDEX",
    "USPS",
    "DHL"
]

carrier_values = F.array(
    *[F.lit(value) for value in carriers]
)

# ------------------------------------------------------------
# 1. Create base shipment records
# ------------------------------------------------------------

shipments_base = (
    spark.range(1, shipment_count + 1)
    .withColumnRenamed("id", "shipment_id")

    # Connect each shipment to an order
    .withColumn(
        "order_id",
        (
            F.pmod(
                F.col("shipment_id") - 1,
                F.lit(order_count)
            ) + 1
        ).cast("long")
    )

    # Generate a tracking number
    .withColumn(
        "tracking_number",
        F.concat(
            F.lit("TRK-"),
            F.lpad(
                F.col("shipment_id").cast("string"),
                12,
                "0"
            )
        )
    )

    # Select a carrier
    .withColumn(
        "_carrier_index",
        (
            F.pmod(
                F.col("shipment_id"),
                F.lit(len(carriers))
            ) + 1
        ).cast("int")
    )
    .withColumn(
        "carrier",
        F.element_at(
            carrier_values,
            F.col("_carrier_index")
        )
    )

    # Assign each record to a daily raw-data partition
    .withColumn(
        "load_date",
        F.date_add(
            F.to_date(F.lit(START_DATE)),
            F.pmod(
                F.col("shipment_id") - 1,
                F.lit(number_of_days)
            ).cast("int")
        )
    )

    # Shipment happens on load date or one day afterward
    .withColumn(
        "_shipped_date_clean",
        F.date_add(
            F.col("load_date"),
            F.pmod(
                F.col("shipment_id"),
                F.lit(2)
            ).cast("int")
        )
    )

    # Expected delivery is 3–7 days after shipment
    # The cast to int fixes the DATATYPE_MISMATCH error.
    .withColumn(
        "_expected_delivery_date_clean",
        F.date_add(
            F.col("_shipped_date_clean"),
            (
                F.lit(3)
                + F.pmod(
                    F.col("shipment_id"),
                    F.lit(5)
                )
            ).cast("int")
        )
    )

    # Generate normal shipment statuses
    .withColumn(
        "shipment_status",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(20)
            ) < 14,
            F.lit("DELIVERED")
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(20)
            ) < 17,
            F.lit("IN_TRANSIT")
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(20)
            ) < 19,
            F.lit("LABEL_CREATED")
        )
        .otherwise(
            F.lit("RETURNED")
        )
    )

    # Generate delivery delays
    # 70% on time
    # 10% one day late
    # 10% three days late
    # 10% five days late
    .withColumn(
        "_delivery_delay_days",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) < 7,
            F.lit(0)
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) == 7,
            F.lit(1)
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) == 8,
            F.lit(3)
        )
        .otherwise(
            F.lit(5)
        )
        .cast("int")
    )

    # Intentionally create some missing expected-delivery dates
    .withColumn(
        "_expected_delivery_date_clean",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(4_500)
            ) == 0,
            F.lit(None).cast("date")
        )
        .otherwise(
            F.col("_expected_delivery_date_clean")
        )
    )

    # Only delivered and returned shipments receive an actual date
    .withColumn(
        "_actual_delivery_date_clean",
        F.when(
            F.col("shipment_status").isin(
                "DELIVERED",
                "RETURNED"
            )
            & F.col("_expected_delivery_date_clean").isNotNull(),

            F.date_add(
                F.col("_expected_delivery_date_clean"),
                F.col("_delivery_delay_days").cast("int")
            )
        )
        .otherwise(
            F.lit(None).cast("date")
        )
    )

    # Create source update timestamp
    .withColumn(
        "updated_at",
        F.to_timestamp(
            F.concat(
                F.date_format(
                    F.col("load_date"),
                    "yyyy-MM-dd"
                ),
                F.lit(" "),
                F.lpad(
                    F.pmod(
                        F.col("shipment_id"),
                        F.lit(24)
                    ).cast("string"),
                    2,
                    "0"
                ),
                F.lit(":00:00")
            )
        )
    )

    # Intentionally create missing order IDs
    .withColumn(
        "order_id",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(2_500)
            ) == 0,
            F.lit(None).cast("long")
        )
        .otherwise(
            F.col("order_id")
        )
    )

    # Intentionally create nonexistent order IDs
    .withColumn(
        "order_id",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(1_777)
            ) == 0,
            (
                F.lit(order_count)
                + F.col("shipment_id")
            ).cast("long")
        )
        .otherwise(
            F.col("order_id")
        )
    )

    # Intentionally create invalid shipment statuses
    .withColumn(
        "shipment_status",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(3_000)
            ) == 0,
            F.lit("UNKNOWN_STATUS")
        )
        .otherwise(
            F.col("shipment_status")
        )
    )

    # Intentionally create missing carriers
    .withColumn(
        "carrier",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(4_000)
            ) == 0,
            F.lit(None).cast("string")
        )
        .otherwise(
            F.col("carrier")
        )
    )

    .drop("_carrier_index")
)

# ------------------------------------------------------------
# 2. Create shipment updates
#
# Simulates carrier updates such as:
# LABEL_CREATED → IN_TRANSIT
# IN_TRANSIT    → DELIVERED
# ------------------------------------------------------------

shipment_updates = (
    shipments_base
    .filter(
        F.pmod(
            F.col("shipment_id"),
            F.lit(101)
        ) == 0
    )

    # Updated version has a later timestamp
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 2 HOURS")
    )

    # Advance the shipment status
    .withColumn(
        "shipment_status",
        F.when(
            F.col("shipment_status") == "LABEL_CREATED",
            F.lit("IN_TRANSIT")
        )
        .when(
            F.col("shipment_status") == "IN_TRANSIT",
            F.lit("DELIVERED")
        )
        .otherwise(
            F.col("shipment_status")
        )
    )

    # If the updated status becomes delivered,
    # assign an actual delivery date
    .withColumn(
        "_actual_delivery_date_clean",
        F.when(
            (F.col("shipment_status") == "DELIVERED")
            & F.col("_actual_delivery_date_clean").isNull()
            & F.col("_expected_delivery_date_clean").isNotNull(),

            F.date_add(
                F.col("_expected_delivery_date_clean"),
                F.lit(1)
            )
        )
        .otherwise(
            F.col("_actual_delivery_date_clean")
        )
    )
)

# Combine original shipment records and later updates
shipments_combined = shipments_base.unionByName(
    shipment_updates
)

# ------------------------------------------------------------
# 3. Convert the internal date columns into mixed source formats
# ------------------------------------------------------------

shipments_raw = (
    shipments_combined

    # Mixed shipped-date formats
    .withColumn(
        "shipped_date",
        F.when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) < 7,
            F.date_format(
                F.col("_shipped_date_clean"),
                "yyyy-MM-dd"
            )
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) < 9,
            F.date_format(
                F.col("_shipped_date_clean"),
                "MM/dd/yyyy"
            )
        )
        .otherwise(
            F.date_format(
                F.col("_shipped_date_clean"),
                "dd-MMM-yyyy"
            )
        )
    )

    # Mixed expected-delivery-date formats
    .withColumn(
        "expected_delivery_date",
        F.when(
            F.col("_expected_delivery_date_clean").isNull(),
            F.lit(None).cast("string")
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) < 7,
            F.date_format(
                F.col("_expected_delivery_date_clean"),
                "yyyy-MM-dd"
            )
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) < 9,
            F.date_format(
                F.col("_expected_delivery_date_clean"),
                "MM/dd/yyyy"
            )
        )
        .otherwise(
            F.date_format(
                F.col("_expected_delivery_date_clean"),
                "dd-MMM-yyyy"
            )
        )
    )

    # Mixed actual-delivery-date formats
    .withColumn(
        "actual_delivery_date",
        F.when(
            F.col("_actual_delivery_date_clean").isNull(),
            F.lit(None).cast("string")
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) < 7,
            F.date_format(
                F.col("_actual_delivery_date_clean"),
                "yyyy-MM-dd"
            )
        )
        .when(
            F.pmod(
                F.col("shipment_id"),
                F.lit(10)
            ) < 9,
            F.date_format(
                F.col("_actual_delivery_date_clean"),
                "MM/dd/yyyy"
            )
        )
        .otherwise(
            F.date_format(
                F.col("_actual_delivery_date_clean"),
                "dd-MMM-yyyy"
            )
        )
    )

    # Keep only the raw source columns
    .select(
        "shipment_id",
        "order_id",
        "tracking_number",
        "carrier",
        "shipment_status",
        "shipped_date",
        "expected_delivery_date",
        "actual_delivery_date",
        "load_date",
        "updated_at"
    )
)

# ------------------------------------------------------------
# 4. Write raw shipment files across daily partitions
# ------------------------------------------------------------

(
    shipments_raw
    .repartition(
        number_of_days * 2,
        "load_date"
    )
    .write
    .mode("overwrite")
    .option("header", "true")
    .partitionBy("load_date")
    .csv(PATHS["shipments"])
)

print("Shipments dataset generated successfully.")
print(f"Base shipment rows: {shipment_count:,}")
print(
    f"Expected shipment updates: "
    f"approximately {shipment_count // 101:,}"
)
print(f"Output location: {PATHS['shipments']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 14: Validate raw shipments dataset
# ============================================================

shipments_check = (
    spark.read
    .option("header", "true")
    .csv(PATHS["shipments"])
)

# ------------------------------------------------------------
# 1. Row-count and duplicate checks
# ------------------------------------------------------------

total_shipment_rows = shipments_check.count()

distinct_shipment_ids = (
    shipments_check
    .select("shipment_id")
    .distinct()
    .count()
)

duplicate_shipment_rows = (
    total_shipment_rows - distinct_shipment_ids
)

print(f"Total raw shipment rows: {total_shipment_rows:,}")
print(f"Distinct shipment IDs: {distinct_shipment_ids:,}")
print(f"Duplicate shipment records: {duplicate_shipment_rows:,}")

# ------------------------------------------------------------
# 2. Confirm daily source partitions
# ------------------------------------------------------------

print("\nShipment rows by load date:")

display(
    shipments_check
    .groupBy("load_date")
    .count()
    .orderBy("load_date")
)

# ------------------------------------------------------------
# 3. Check intentional data-quality problems
# ------------------------------------------------------------

shipment_quality_summary = shipments_check.select(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(
            F.col("order_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_order_id_rows"),

    F.sum(
        F.when(
            F.col("carrier").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_carrier_rows"),

    F.sum(
        F.when(
            F.col("expected_delivery_date").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_expected_delivery_date_rows"),

    F.sum(
        F.when(
            F.col("shipment_status") == "UNKNOWN_STATUS",
            1
        ).otherwise(0)
    ).alias("invalid_shipment_status_rows")
)

display(shipment_quality_summary)

# ------------------------------------------------------------
# 4. Count records by shipment status
# ------------------------------------------------------------

display(
    shipments_check
    .groupBy("shipment_status")
    .count()
    .orderBy(F.desc("count"))
)

# ------------------------------------------------------------
# 5. Build the valid order-key dataset
# ------------------------------------------------------------

valid_order_keys = (
    orders_check
    .select("order_id")
    .filter(F.col("order_id").isNotNull())
    .distinct()
)

# ------------------------------------------------------------
# 6. Find shipments referencing nonexistent orders
# ------------------------------------------------------------

unmatched_shipment_orders = (
    shipments_check
    .filter(F.col("order_id").isNotNull())
    .join(
        valid_order_keys,
        on="order_id",
        how="left_anti"
    )
)

unmatched_shipment_order_count = (
    unmatched_shipment_orders.count()
)

print(
    f"Shipments referencing nonexistent orders: "
    f"{unmatched_shipment_order_count:,}"
)

display(
    unmatched_shipment_orders
    .select(
        "shipment_id",
        "order_id",
        "tracking_number",
        "carrier",
        "shipment_status",
        "load_date"
    )
    .orderBy(
        F.col("shipment_id").cast("long")
    )
    .limit(20)
)

# ------------------------------------------------------------
# 7. Show duplicated shipment IDs
# ------------------------------------------------------------

duplicate_shipment_ids = (
    shipments_check
    .groupBy("shipment_id")
    .count()
    .filter(F.col("count") > 1)
)

display(
    duplicate_shipment_ids
    .orderBy(
        F.col("shipment_id").cast("long")
    )
    .limit(20)
)

# ------------------------------------------------------------
# 8. Compare original and updated shipment versions
# ------------------------------------------------------------

display(
    shipments_check
    .join(
        duplicate_shipment_ids.select("shipment_id"),
        on="shipment_id",
        how="inner"
    )
    .select(
        "shipment_id",
        "order_id",
        "carrier",
        "shipment_status",
        "expected_delivery_date",
        "actual_delivery_date",
        "updated_at",
        "load_date"
    )
    .orderBy(
        F.col("shipment_id").cast("long"),
        F.col("updated_at").cast("timestamp")
    )
    .limit(30)
)

# ------------------------------------------------------------
# 9. Standardize the mixed source-date formats temporarily
#    so we can validate delivery delays
# ------------------------------------------------------------

shipments_with_parsed_dates = (
    shipments_check

    .withColumn(
        "expected_delivery_date_parsed",
        F.coalesce(
            F.to_date(
                "expected_delivery_date",
                "yyyy-MM-dd"
            ),
            F.to_date(
                "expected_delivery_date",
                "MM/dd/yyyy"
            ),
            F.to_date(
                "expected_delivery_date",
                "dd-MMM-yyyy"
            )
        )
    )

    .withColumn(
        "actual_delivery_date_parsed",
        F.coalesce(
            F.to_date(
                "actual_delivery_date",
                "yyyy-MM-dd"
            ),
            F.to_date(
                "actual_delivery_date",
                "MM/dd/yyyy"
            ),
            F.to_date(
                "actual_delivery_date",
                "dd-MMM-yyyy"
            )
        )
    )

    .withColumn(
        "delivery_delay_days",
        F.datediff(
            F.col("actual_delivery_date_parsed"),
            F.col("expected_delivery_date_parsed")
        )
    )
)

# ------------------------------------------------------------
# 10. Examine delivery-delay distribution
# ------------------------------------------------------------

display(
    shipments_with_parsed_dates
    .filter(
        F.col("delivery_delay_days").isNotNull()
    )
    .groupBy("delivery_delay_days")
    .count()
    .orderBy("delivery_delay_days")
)

# ------------------------------------------------------------
# 11. Show sample delayed shipments
# ------------------------------------------------------------

display(
    shipments_with_parsed_dates
    .filter(
        F.col("delivery_delay_days") > 0
    )
    .select(
        "shipment_id",
        "order_id",
        "carrier",
        "shipment_status",
        "expected_delivery_date",
        "actual_delivery_date",
        "delivery_delay_days"
    )
    .orderBy(
        F.desc("delivery_delay_days")
    )
    .limit(20)
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
