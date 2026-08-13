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
# STEP 15: Ingestion and validation pipeline setup
# Notebook: 02_ingest_validate
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from datetime import datetime, timezone

# Unique identifier for this pipeline execution
RUN_ID = datetime.now(timezone.utc).strftime(
    "RUN_%Y%m%d_%H%M%S"
)

PIPELINE_STARTED_AT = datetime.now(timezone.utc)

RAW_PATHS = {
    "customers": "Files/raw/customers",
    "products": "Files/raw/products",
    "orders": "Files/raw/orders",
    "order_items": "Files/raw/order_items",
    "payments": "Files/raw/payments",
    "shipments": "Files/raw/shipments"
}

QUARANTINE_PATHS = {
    "customers": "Files/quarantine/customers",
    "products": "Files/quarantine/products",
    "orders": "Files/quarantine/orders",
    "order_items": "Files/quarantine/order_items",
    "payments": "Files/quarantine/payments",
    "shipments": "Files/quarantine/shipments"
}

STAGING_PATHS = {
    "customers": "Files/curated/staging/customers",
    "products": "Files/curated/staging/products",
    "orders": "Files/curated/staging/orders",
    "order_items": "Files/curated/staging/order_items",
    "payments": "Files/curated/staging/payments",
    "shipments": "Files/curated/staging/shipments"
}

print(f"Pipeline run ID: {RUN_ID}")
print(f"Pipeline started at: {PIPELINE_STARTED_AT}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Helper function: Standardize mixed source date formats
# ============================================================

def parse_mixed_date(column_name: str):
    """
    Convert supported source date strings into a Spark DATE.

    Supported source formats:
    1. yyyy-MM-dd
    2. MM/dd/yyyy
    3. dd-MMM-yyyy
    """

    return F.coalesce(
        F.to_date(
            F.col(column_name),
            "yyyy-MM-dd"
        ),
        F.to_date(
            F.col(column_name),
            "MM/dd/yyyy"
        ),
        F.to_date(
            F.col(column_name),
            "dd-MMM-yyyy"
        )
    )


print("Mixed-date parsing function created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 16: Ingest raw customers with an explicit source schema
# ============================================================

# Define the columns that physically exist inside each CSV file.
# load_date is not included because Spark discovers it from folders
# such as: load_date=2026-01-01
customers_source_schema = T.StructType([
    T.StructField("customer_id", T.LongType(), True),
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("state", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("customer_segment", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("phone", T.StringType(), True),
    T.StructField("signup_date", T.StringType(), True),
    T.StructField("updated_at", T.StringType(), True),
    T.StructField("customer_status", T.StringType(), True)
])

# Read all daily customer partitions
customers_ingested = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(customers_source_schema)
    .csv(RAW_PATHS["customers"])

    # Preserve original source values before converting them
    .withColumnRenamed(
        "signup_date",
        "signup_date_raw"
    )
    .withColumnRenamed(
        "updated_at",
        "updated_at_raw"
    )

    # Standardize the mixed signup-date formats
    .withColumn(
        "signup_date",
        parse_mixed_date("signup_date_raw")
    )

    # Convert source timestamp text into Spark TIMESTAMP
    .withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp("updated_at_raw"),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    # load_date was discovered from the partition-folder name
    .withColumn(
        "load_date",
        F.col("load_date").cast("date")
    )

    # Operational metadata
    .withColumn(
        "pipeline_run_id",
        F.lit(RUN_ID)
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
)

# ------------------------------------------------------------
# Validate ingestion results
# ------------------------------------------------------------

customer_ingested_count = customers_ingested.count()

print(f"Customer rows ingested: {customer_ingested_count:,}")

print("\nCustomer ingestion schema:")
customers_ingested.printSchema()

display(
    customers_ingested
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "signup_date_raw",
        "signup_date",
        "updated_at_raw",
        "updated_at",
        "customer_status",
        "load_date",
        "pipeline_run_id"
    )
    .orderBy(
        F.col("customer_id"),
        F.col("updated_at")
    )
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 17: Validate customers and route rejected records
# ============================================================

VALID_CUSTOMER_STATUSES = [
    "ACTIVE",
    "INACTIVE",
    "SUSPENDED"
]

VALID_CUSTOMER_SEGMENTS = [
    "CONSUMER",
    "CORPORATE",
    "SMALL_BUSINESS"
]

VALID_REGIONS = [
    "Northeast",
    "South",
    "Midwest",
    "West"
]

EMAIL_PATTERN = (
    r"^[A-Za-z0-9._%+-]+@"
    r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

# ------------------------------------------------------------
# 1. Apply customer validation rules
# ------------------------------------------------------------

customers_validated = (
    customers_ingested

    .withColumn(
        "rejection_reason",
        F.concat_ws(
            " | ",

            # Business key must exist
            F.when(
                F.col("customer_id").isNull(),
                F.lit("MISSING_CUSTOMER_ID")
            ),

            # Required names must exist
            F.when(
                F.col("first_name").isNull()
                | (F.trim(F.col("first_name")) == ""),
                F.lit("MISSING_FIRST_NAME")
            ),

            F.when(
                F.col("last_name").isNull()
                | (F.trim(F.col("last_name")) == ""),
                F.lit("MISSING_LAST_NAME")
            ),

            # Email completeness and format
            F.when(
                F.col("email").isNull()
                | (F.trim(F.col("email")) == ""),
                F.lit("MISSING_EMAIL")
            ),

            F.when(
                F.col("email").isNotNull()
                & ~F.col("email").rlike(EMAIL_PATTERN),
                F.lit("INVALID_EMAIL_FORMAT")
            ),

            # Region completeness and accepted values
            F.when(
                F.col("region").isNull()
                | (F.trim(F.col("region")) == ""),
                F.lit("MISSING_REGION")
            ),

            F.when(
                F.col("region").isNotNull()
                & ~F.col("region").isin(VALID_REGIONS),
                F.lit("INVALID_REGION")
            ),

            # Segment must contain an approved value
            F.when(
                F.col("customer_segment").isNull(),
                F.lit("MISSING_CUSTOMER_SEGMENT")
            ),

            F.when(
                F.col("customer_segment").isNotNull()
                & ~F.col("customer_segment").isin(
                    VALID_CUSTOMER_SEGMENTS
                ),
                F.lit("INVALID_CUSTOMER_SEGMENT")
            ),

            # Status accepted-values validation
            F.when(
                F.col("customer_status").isNull(),
                F.lit("MISSING_CUSTOMER_STATUS")
            ),

            F.when(
                F.col("customer_status").isNotNull()
                & ~F.col("customer_status").isin(
                    VALID_CUSTOMER_STATUSES
                ),
                F.lit("INVALID_CUSTOMER_STATUS")
            ),

            # Date parsing checks
            F.when(
                F.col("signup_date_raw").isNull(),
                F.lit("MISSING_SIGNUP_DATE")
            ),

            F.when(
                F.col("signup_date_raw").isNotNull()
                & F.col("signup_date").isNull(),
                F.lit("INVALID_SIGNUP_DATE")
            ),

            # Signup date should not be later than ingestion date
            F.when(
                F.col("signup_date").isNotNull()
                & F.col("load_date").isNotNull()
                & (F.col("signup_date") > F.col("load_date")),
                F.lit("SIGNUP_DATE_AFTER_LOAD_DATE")
            ),

            # Timestamp and partition checks
            F.when(
                F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            ),

            F.when(
                F.col("load_date").isNull(),
                F.lit("MISSING_LOAD_DATE")
            )
        )
    )

    # Empty rejection_reason means the record passed all rules
    .withColumn(
        "validation_status",
        F.when(
            F.length(F.col("rejection_reason")) > 0,
            F.lit("REJECTED")
        ).otherwise(
            F.lit("VALID")
        )
    )
    .cache()
)

print("Customer validation rules applied successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Split valid and rejected customer records
# ============================================================

customers_rejected = (
    customers_validated
    .filter(F.col("validation_status") == "REJECTED")
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
)

customers_valid_before_dedupe = (
    customers_validated
    .filter(F.col("validation_status") == "VALID")
    .drop(
        "rejection_reason",
        "validation_status"
    )
)

ingested_customer_count = customers_validated.count()
rejected_customer_count = customers_rejected.count()
valid_customer_count_before_dedupe = (
    customers_valid_before_dedupe.count()
)

print(f"Customer rows ingested: {ingested_customer_count:,}")
print(f"Valid rows before deduplication: "
      f"{valid_customer_count_before_dedupe:,}")
print(f"Rejected rows: {rejected_customer_count:,}")

print(
    "Reconciliation passed:",
    ingested_customer_count
    == valid_customer_count_before_dedupe
    + rejected_customer_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Deduplicate valid customers using the latest-update rule
# ============================================================

customer_dedup_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("load_date").desc_nulls_last()
    )
)

customers_valid_deduplicated = (
    customers_valid_before_dedupe

    .withColumn(
        "_record_rank",
        F.row_number().over(customer_dedup_window)
    )

    # Keep the latest version of each customer
    .filter(F.col("_record_rank") == 1)

    .drop("_record_rank")
)

valid_customer_count_after_dedupe = (
    customers_valid_deduplicated.count()
)

customer_duplicates_removed = (
    valid_customer_count_before_dedupe
    - valid_customer_count_after_dedupe
)

print(
    f"Valid rows after deduplication: "
    f"{valid_customer_count_after_dedupe:,}"
)

print(
    f"Duplicate customer records removed: "
    f"{customer_duplicates_removed:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Write customer quarantine and staging outputs
# ============================================================

# Invalid records, including rejection reasons
(
    customers_rejected
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("load_date")
    .save(QUARANTINE_PATHS["customers"])
)

# Clean and deduplicated customer records
(
    customers_valid_deduplicated
    .write
    .format("delta")
    .mode("overwrite")
    .save(STAGING_PATHS["customers"])
)

print(
    "Customer quarantine output:",
    QUARANTINE_PATHS["customers"]
)

print(
    "Customer staging output:",
    STAGING_PATHS["customers"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Review customer validation results
# ============================================================

print("Rejected customers by reason:")

display(
    customers_rejected
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

print("Sample rejected customer records:")

display(
    customers_rejected
    .select(
        "customer_id",
        "email",
        "region",
        "customer_status",
        "signup_date_raw",
        "rejection_reason",
        "load_date",
        "pipeline_run_id"
    )
    .orderBy("customer_id")
    .limit(30)
)

print("Sample valid, deduplicated customers:")

display(
    customers_valid_deduplicated
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "region",
        "customer_segment",
        "customer_status",
        "signup_date",
        "updated_at",
        "load_date"
    )
    .orderBy("customer_id")
    .limit(20)
)

customers_validated.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 18A: Ingest raw products with an explicit source schema
# ============================================================

products_source_schema = T.StructType([
    T.StructField("product_id", T.LongType(), True),
    T.StructField("sku", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("subcategory", T.StringType(), True),
    T.StructField("brand", T.StringType(), True),

    # Read numeric source values as strings first.
    # This preserves malformed source values for auditing.
    T.StructField("unit_price", T.StringType(), True),
    T.StructField("unit_cost", T.StringType(), True),

    T.StructField("product_status", T.StringType(), True),
    T.StructField("updated_at", T.StringType(), True)
])

products_ingested = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(products_source_schema)
    .csv(RAW_PATHS["products"])

    # Preserve original source values
    .withColumnRenamed(
        "unit_price",
        "unit_price_raw"
    )
    .withColumnRenamed(
        "unit_cost",
        "unit_cost_raw"
    )
    .withColumnRenamed(
        "updated_at",
        "updated_at_raw"
    )

    # Convert source values into target data types
    .withColumn(
        "unit_price",
        F.col("unit_price_raw").cast(
            T.DecimalType(12, 2)
        )
    )
    .withColumn(
        "unit_cost",
        F.col("unit_cost_raw").cast(
            T.DecimalType(12, 2)
        )
    )
    .withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp("updated_at_raw"),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    # Discovered from folders such as load_date=2026-01-01
    .withColumn(
        "load_date",
        F.col("load_date").cast("date")
    )

    # Operational metadata
    .withColumn(
        "pipeline_run_id",
        F.lit(RUN_ID)
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
)

product_ingested_count = products_ingested.count()

print(
    f"Product rows ingested: "
    f"{product_ingested_count:,}"
)

print("\nProduct ingestion schema:")
products_ingested.printSchema()

display(
    products_ingested
    .select(
        "product_id",
        "sku",
        "product_name",
        "category",
        "brand",
        "unit_price_raw",
        "unit_price",
        "unit_cost_raw",
        "unit_cost",
        "product_status",
        "updated_at",
        "load_date"
    )
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
# STEP 18B: Apply product validation rules
# ============================================================

VALID_PRODUCT_STATUSES = [
    "ACTIVE",
    "DISCONTINUED"
]

products_validated = (
    products_ingested

    .withColumn(
        "rejection_reason",
        F.concat_ws(
            " | ",

            # Product business key
            F.when(
                F.col("product_id").isNull(),
                F.lit("MISSING_PRODUCT_ID")
            ),

            # Required identifying fields
            F.when(
                F.col("sku").isNull()
                | (F.trim(F.col("sku")) == ""),
                F.lit("MISSING_SKU")
            ),

            F.when(
                F.col("product_name").isNull()
                | (F.trim(F.col("product_name")) == ""),
                F.lit("MISSING_PRODUCT_NAME")
            ),

            # Category completeness
            F.when(
                F.col("category").isNull()
                | (F.trim(F.col("category")) == ""),
                F.lit("MISSING_CATEGORY")
            ),

            # Unit-price checks
            F.when(
                F.col("unit_price_raw").isNull()
                | (F.trim(F.col("unit_price_raw")) == ""),
                F.lit("MISSING_UNIT_PRICE")
            ),

            F.when(
                F.col("unit_price_raw").isNotNull()
                & F.col("unit_price").isNull(),
                F.lit("INVALID_UNIT_PRICE_FORMAT")
            ),

            F.when(
                F.col("unit_price").isNotNull()
                & (F.col("unit_price") <= 0),
                F.lit("UNIT_PRICE_MUST_BE_POSITIVE")
            ),

            # Unit-cost checks
            F.when(
                F.col("unit_cost_raw").isNull()
                | (F.trim(F.col("unit_cost_raw")) == ""),
                F.lit("MISSING_UNIT_COST")
            ),

            F.when(
                F.col("unit_cost_raw").isNotNull()
                & F.col("unit_cost").isNull(),
                F.lit("INVALID_UNIT_COST_FORMAT")
            ),

            F.when(
                F.col("unit_cost").isNotNull()
                & (F.col("unit_cost") < 0),
                F.lit("UNIT_COST_CANNOT_BE_NEGATIVE")
            ),

            # Optional business rule:
            # cost should generally not exceed selling price
            F.when(
                F.col("unit_cost").isNotNull()
                & F.col("unit_price").isNotNull()
                & (F.col("unit_cost") > F.col("unit_price")),
                F.lit("UNIT_COST_EXCEEDS_UNIT_PRICE")
            ),

            # Accepted product statuses
            F.when(
                F.col("product_status").isNull()
                | (F.trim(F.col("product_status")) == ""),
                F.lit("MISSING_PRODUCT_STATUS")
            ),

            F.when(
                F.col("product_status").isNotNull()
                & ~F.col("product_status").isin(
                    VALID_PRODUCT_STATUSES
                ),
                F.lit("INVALID_PRODUCT_STATUS")
            ),

            # Operational fields
            F.when(
                F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            ),

            F.when(
                F.col("load_date").isNull(),
                F.lit("MISSING_LOAD_DATE")
            )
        )
    )

    .withColumn(
        "validation_status",
        F.when(
            F.length(F.col("rejection_reason")) > 0,
            F.lit("REJECTED")
        ).otherwise(
            F.lit("VALID")
        )
    )

    .cache()
)

print("Product validation rules applied successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 18C: Split valid and rejected product records
# ============================================================

products_rejected = (
    products_validated
    .filter(
        F.col("validation_status") == "REJECTED"
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
)

products_valid_before_dedupe = (
    products_validated
    .filter(
        F.col("validation_status") == "VALID"
    )
    .drop(
        "rejection_reason",
        "validation_status"
    )
)

ingested_product_count = products_validated.count()

rejected_product_count = products_rejected.count()

valid_product_count_before_dedupe = (
    products_valid_before_dedupe.count()
)

print(
    f"Product rows ingested: "
    f"{ingested_product_count:,}"
)

print(
    f"Valid rows before deduplication: "
    f"{valid_product_count_before_dedupe:,}"
)

print(
    f"Rejected product rows: "
    f"{rejected_product_count:,}"
)

print(
    "Reconciliation passed:",
    ingested_product_count
    == valid_product_count_before_dedupe
    + rejected_product_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 18D: Deduplicate valid products
# ============================================================

product_dedup_window = (
    Window
    .partitionBy("product_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("load_date").desc_nulls_last()
    )
)

products_valid_deduplicated = (
    products_valid_before_dedupe

    .withColumn(
        "_record_rank",
        F.row_number().over(product_dedup_window)
    )

    .filter(
        F.col("_record_rank") == 1
    )

    .drop("_record_rank")
)

valid_product_count_after_dedupe = (
    products_valid_deduplicated.count()
)

product_duplicates_removed = (
    valid_product_count_before_dedupe
    - valid_product_count_after_dedupe
)

print(
    f"Valid products after deduplication: "
    f"{valid_product_count_after_dedupe:,}"
)

print(
    f"Duplicate product records removed: "
    f"{product_duplicates_removed:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 18E: Write product quarantine and staging outputs
# ============================================================

(
    products_rejected
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(QUARANTINE_PATHS["products"])
)

(
    products_valid_deduplicated
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(STAGING_PATHS["products"])
)

print(
    "Product quarantine output:",
    QUARANTINE_PATHS["products"]
)

print(
    "Product staging output:",
    STAGING_PATHS["products"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 18F: Review product validation results
# ============================================================

print("Rejected products by reason:")

display(
    products_rejected
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

print("Sample rejected products:")

display(
    products_rejected
    .select(
        "product_id",
        "sku",
        "category",
        "unit_price_raw",
        "unit_price",
        "unit_cost",
        "product_status",
        "rejection_reason",
        "load_date"
    )
    .orderBy("product_id")
    .limit(30)
)

print("Sample valid, deduplicated products:")

display(
    products_valid_deduplicated
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
        "updated_at"
    )
    .orderBy("product_id")
    .limit(20)
)

products_validated.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 19A: Ingest raw orders with an explicit source schema
# ============================================================

orders_source_schema = T.StructType([
    T.StructField("order_id", T.LongType(), True),
    T.StructField("customer_id", T.LongType(), True),

    # Preserve source values as strings before conversion
    T.StructField("order_date", T.StringType(), True),
    T.StructField("order_channel", T.StringType(), True),
    T.StructField("currency_code", T.StringType(), True),
    T.StructField("order_status", T.StringType(), True),
    T.StructField("discount_pct", T.StringType(), True),
    T.StructField("tax_rate", T.StringType(), True),
    T.StructField("shipping_cost", T.StringType(), True),
    T.StructField("updated_at", T.StringType(), True)
])

orders_ingested = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(orders_source_schema)
    .csv(RAW_PATHS["orders"])

    # Preserve original source values
    .withColumnRenamed(
        "order_date",
        "order_date_raw"
    )
    .withColumnRenamed(
        "discount_pct",
        "discount_pct_raw"
    )
    .withColumnRenamed(
        "tax_rate",
        "tax_rate_raw"
    )
    .withColumnRenamed(
        "shipping_cost",
        "shipping_cost_raw"
    )
    .withColumnRenamed(
        "updated_at",
        "updated_at_raw"
    )

    # Standardize mixed order-date formats
    .withColumn(
        "order_date",
        parse_mixed_date("order_date_raw")
    )

    # Convert numeric values
    .withColumn(
        "discount_pct",
        F.col("discount_pct_raw").cast(
            T.DecimalType(5, 2)
        )
    )
    .withColumn(
        "tax_rate",
        F.col("tax_rate_raw").cast(
            T.DecimalType(5, 4)
        )
    )
    .withColumn(
        "shipping_cost",
        F.col("shipping_cost_raw").cast(
            T.DecimalType(12, 2)
        )
    )

    # Convert update timestamp
    .withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp("updated_at_raw"),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    # Discovered from load_date=... folders
    .withColumn(
        "load_date",
        F.col("load_date").cast("date")
    )

    # Operational metadata
    .withColumn(
        "pipeline_run_id",
        F.lit(RUN_ID)
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
)

order_ingested_count = orders_ingested.count()

print(f"Order rows ingested: {order_ingested_count:,}")

print("\nOrder ingestion schema:")
orders_ingested.printSchema()

display(
    orders_ingested
    .select(
        "order_id",
        "customer_id",
        "order_date_raw",
        "order_date",
        "order_channel",
        "order_status",
        "discount_pct",
        "tax_rate",
        "shipping_cost",
        "updated_at",
        "load_date"
    )
    .orderBy("order_id", "updated_at")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 19B: Check customer referential integrity
# ============================================================

valid_customer_keys = (
    customers_valid_deduplicated
    .select("customer_id")
    .filter(F.col("customer_id").isNotNull())
    .distinct()
    .withColumn(
        "_customer_exists",
        F.lit(True)
    )
)

orders_with_customer_check = (
    orders_ingested.alias("orders")

    .join(
        valid_customer_keys.alias("customers"),
        on="customer_id",
        how="left"
    )
)

print("Customer referential-integrity check completed.")

display(
    orders_with_customer_check
    .filter(
        F.col("customer_id").isNotNull()
        & F.col("_customer_exists").isNull()
    )
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 19C: Apply order validation rules
# ============================================================

VALID_ORDER_STATUSES = [
    "COMPLETED",
    "SHIPPED",
    "CANCELLED",
    "PENDING"
]

VALID_ORDER_CHANNELS = [
    "WEB",
    "MOBILE",
    "STORE",
    "MARKETPLACE"
]

VALID_CURRENCIES = [
    "USD"
]

orders_validated = (
    orders_with_customer_check

    .withColumn(
        "rejection_reason",
        F.concat_ws(
            " | ",

            # Business key
            F.when(
                F.col("order_id").isNull(),
                F.lit("MISSING_ORDER_ID")
            ),

            # Customer foreign key
            F.when(
                F.col("customer_id").isNull(),
                F.lit("MISSING_CUSTOMER_ID")
            ),

            F.when(
                F.col("customer_id").isNotNull()
                & F.col("_customer_exists").isNull(),
                F.lit("CUSTOMER_NOT_FOUND")
            ),

            # Order date validation
            F.when(
                F.col("order_date_raw").isNull()
                | (F.trim(F.col("order_date_raw")) == ""),
                F.lit("MISSING_ORDER_DATE")
            ),

            F.when(
                F.col("order_date_raw").isNotNull()
                & F.col("order_date").isNull(),
                F.lit("INVALID_ORDER_DATE")
            ),

            F.when(
                F.col("order_date").isNotNull()
                & F.col("load_date").isNotNull()
                & (F.col("order_date") > F.col("load_date")),
                F.lit("ORDER_DATE_AFTER_LOAD_DATE")
            ),

            # Order status
            F.when(
                F.col("order_status").isNull()
                | (F.trim(F.col("order_status")) == ""),
                F.lit("MISSING_ORDER_STATUS")
            ),

            F.when(
                F.col("order_status").isNotNull()
                & ~F.col("order_status").isin(
                    VALID_ORDER_STATUSES
                ),
                F.lit("INVALID_ORDER_STATUS")
            ),

            # Order channel
            F.when(
                F.col("order_channel").isNull()
                | (F.trim(F.col("order_channel")) == ""),
                F.lit("MISSING_ORDER_CHANNEL")
            ),

            F.when(
                F.col("order_channel").isNotNull()
                & ~F.col("order_channel").isin(
                    VALID_ORDER_CHANNELS
                ),
                F.lit("INVALID_ORDER_CHANNEL")
            ),

            # Currency
            F.when(
                F.col("currency_code").isNull()
                | (F.trim(F.col("currency_code")) == ""),
                F.lit("MISSING_CURRENCY_CODE")
            ),

            F.when(
                F.col("currency_code").isNotNull()
                & ~F.col("currency_code").isin(
                    VALID_CURRENCIES
                ),
                F.lit("INVALID_CURRENCY_CODE")
            ),

            # Discount validation
            F.when(
                F.col("discount_pct_raw").isNull(),
                F.lit("MISSING_DISCOUNT_PERCENTAGE")
            ),

            F.when(
                F.col("discount_pct_raw").isNotNull()
                & F.col("discount_pct").isNull(),
                F.lit("INVALID_DISCOUNT_FORMAT")
            ),

            F.when(
                F.col("discount_pct").isNotNull()
                & (
                    (F.col("discount_pct") < 0)
                    | (F.col("discount_pct") > 100)
                ),
                F.lit("DISCOUNT_OUT_OF_RANGE")
            ),

            # Tax-rate validation
            F.when(
                F.col("tax_rate_raw").isNull(),
                F.lit("MISSING_TAX_RATE")
            ),

            F.when(
                F.col("tax_rate_raw").isNotNull()
                & F.col("tax_rate").isNull(),
                F.lit("INVALID_TAX_RATE_FORMAT")
            ),

            F.when(
                F.col("tax_rate").isNotNull()
                & (
                    (F.col("tax_rate") < 0)
                    | (F.col("tax_rate") > 1)
                ),
                F.lit("TAX_RATE_OUT_OF_RANGE")
            ),

            # Shipping-cost validation
            F.when(
                F.col("shipping_cost_raw").isNull(),
                F.lit("MISSING_SHIPPING_COST")
            ),

            F.when(
                F.col("shipping_cost_raw").isNotNull()
                & F.col("shipping_cost").isNull(),
                F.lit("INVALID_SHIPPING_COST_FORMAT")
            ),

            F.when(
                F.col("shipping_cost").isNotNull()
                & (F.col("shipping_cost") < 0),
                F.lit("SHIPPING_COST_CANNOT_BE_NEGATIVE")
            ),

            # Operational fields
            F.when(
                F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            ),

            F.when(
                F.col("load_date").isNull(),
                F.lit("MISSING_LOAD_DATE")
            )
        )
    )

    .withColumn(
        "validation_status",
        F.when(
            F.length(F.col("rejection_reason")) > 0,
            F.lit("REJECTED")
        ).otherwise(
            F.lit("VALID")
        )
    )

    .drop("_customer_exists")
    .cache()
)

print("Order validation rules applied successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 19D: Split valid and rejected order records
# ============================================================

orders_rejected = (
    orders_validated
    .filter(
        F.col("validation_status") == "REJECTED"
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
)

orders_valid_before_dedupe = (
    orders_validated
    .filter(
        F.col("validation_status") == "VALID"
    )
    .drop(
        "rejection_reason",
        "validation_status"
    )
)

ingested_order_count = orders_validated.count()

rejected_order_count = orders_rejected.count()

valid_order_count_before_dedupe = (
    orders_valid_before_dedupe.count()
)

print(f"Order rows ingested: {ingested_order_count:,}")

print(
    f"Valid orders before deduplication: "
    f"{valid_order_count_before_dedupe:,}"
)

print(
    f"Rejected order rows: "
    f"{rejected_order_count:,}"
)

print(
    "Reconciliation passed:",
    ingested_order_count
    == valid_order_count_before_dedupe
    + rejected_order_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 19E: Deduplicate orders using latest-update rule
# ============================================================

order_dedup_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("load_date").desc_nulls_last()
    )
)

orders_valid_deduplicated = (
    orders_valid_before_dedupe

    .withColumn(
        "_record_rank",
        F.row_number().over(order_dedup_window)
    )

    .filter(
        F.col("_record_rank") == 1
    )

    .drop("_record_rank")
)

valid_order_count_after_dedupe = (
    orders_valid_deduplicated.count()
)

order_duplicates_removed = (
    valid_order_count_before_dedupe
    - valid_order_count_after_dedupe
)

print(
    f"Valid orders after deduplication: "
    f"{valid_order_count_after_dedupe:,}"
)

print(
    f"Duplicate order records removed: "
    f"{order_duplicates_removed:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 19F: Write order quarantine and staging outputs
# ============================================================

(
    orders_rejected
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(QUARANTINE_PATHS["orders"])
)

(
    orders_valid_deduplicated
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(STAGING_PATHS["orders"])
)

print(
    "Order quarantine output:",
    QUARANTINE_PATHS["orders"]
)

print(
    "Order staging output:",
    STAGING_PATHS["orders"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 19G: Review order validation results
# ============================================================

print("Rejected orders by reason:")

display(
    orders_rejected
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

print("Sample rejected orders:")

display(
    orders_rejected
    .select(
        "order_id",
        "customer_id",
        "order_date_raw",
        "order_status",
        "order_channel",
        "discount_pct",
        "tax_rate",
        "shipping_cost",
        "rejection_reason",
        "load_date"
    )
    .orderBy("order_id")
    .limit(30)
)

print("Sample valid, deduplicated orders:")

display(
    orders_valid_deduplicated
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "order_channel",
        "currency_code",
        "discount_pct",
        "tax_rate",
        "shipping_cost",
        "updated_at"
    )
    .orderBy("order_id")
    .limit(20)
)

orders_validated.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20A: Ingest raw order_items with an explicit schema
# ============================================================

order_items_source_schema = T.StructType([
    T.StructField("order_item_id", T.LongType(), True),
    T.StructField("order_id", T.LongType(), True),
    T.StructField("product_id", T.LongType(), True),

    # Read numeric source values as strings first so malformed
    # source values can be preserved and audited.
    T.StructField("line_number", T.StringType(), True),
    T.StructField("quantity", T.StringType(), True),
    T.StructField("unit_price", T.StringType(), True),
    T.StructField("discount_pct", T.StringType(), True),
    T.StructField("updated_at", T.StringType(), True)
])

order_items_ingested = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(order_items_source_schema)
    .csv(RAW_PATHS["order_items"])

    # Preserve original source values
    .withColumnRenamed("line_number", "line_number_raw")
    .withColumnRenamed("quantity", "quantity_raw")
    .withColumnRenamed("unit_price", "unit_price_raw")
    .withColumnRenamed("discount_pct", "discount_pct_raw")
    .withColumnRenamed("updated_at", "updated_at_raw")

    # Convert source values into target data types
    .withColumn(
        "line_number",
        F.col("line_number_raw").cast("int")
    )
    .withColumn(
        "quantity",
        F.col("quantity_raw").cast("int")
    )
    .withColumn(
        "unit_price",
        F.col("unit_price_raw").cast(
            T.DecimalType(12, 2)
        )
    )
    .withColumn(
        "discount_pct",
        F.col("discount_pct_raw").cast(
            T.DecimalType(5, 2)
        )
    )
    .withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp("updated_at_raw"),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    # load_date is discovered from partition folders
    .withColumn(
        "load_date",
        F.col("load_date").cast("date")
    )

    # Operational metadata
    .withColumn(
        "pipeline_run_id",
        F.lit(RUN_ID)
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
)

order_item_ingested_count = order_items_ingested.count()

print(
    f"Order-item rows ingested: "
    f"{order_item_ingested_count:,}"
)

print("\nOrder-item ingestion schema:")
order_items_ingested.printSchema()

display(
    order_items_ingested
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "line_number",
        "quantity",
        "unit_price",
        "discount_pct",
        "updated_at",
        "load_date"
    )
    .orderBy("order_item_id", "updated_at")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20B: Check order and product referential integrity
# ============================================================

valid_order_keys = (
    orders_valid_deduplicated
    .select("order_id")
    .filter(F.col("order_id").isNotNull())
    .distinct()
    .withColumn(
        "_order_exists",
        F.lit(True)
    )
)

valid_product_keys = (
    products_valid_deduplicated
    .select("product_id")
    .filter(F.col("product_id").isNotNull())
    .distinct()
    .withColumn(
        "_product_exists",
        F.lit(True)
    )
)

order_items_with_key_checks = (
    order_items_ingested

    .join(
        valid_order_keys,
        on="order_id",
        how="left"
    )

    .join(
        valid_product_keys,
        on="product_id",
        how="left"
    )
)

print("Order and product referential-integrity checks completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20C: Apply order-item validation rules
# ============================================================

order_items_validated = (
    order_items_with_key_checks

    .withColumn(
        "rejection_reason",
        F.concat_ws(
            " | ",

            # Business key
            F.when(
                F.col("order_item_id").isNull(),
                F.lit("MISSING_ORDER_ITEM_ID")
            ),

            # Order foreign key
            F.when(
                F.col("order_id").isNull(),
                F.lit("MISSING_ORDER_ID")
            ),

            F.when(
                F.col("order_id").isNotNull()
                & F.col("_order_exists").isNull(),
                F.lit("ORDER_NOT_FOUND")
            ),

            # Product foreign key
            F.when(
                F.col("product_id").isNull(),
                F.lit("MISSING_PRODUCT_ID")
            ),

            F.when(
                F.col("product_id").isNotNull()
                & F.col("_product_exists").isNull(),
                F.lit("PRODUCT_NOT_FOUND")
            ),

            # Line-number validation
            F.when(
                F.col("line_number_raw").isNull()
                | (F.trim(F.col("line_number_raw")) == ""),
                F.lit("MISSING_LINE_NUMBER")
            ),

            F.when(
                F.col("line_number_raw").isNotNull()
                & F.col("line_number").isNull(),
                F.lit("INVALID_LINE_NUMBER_FORMAT")
            ),

            F.when(
                F.col("line_number").isNotNull()
                & (F.col("line_number") <= 0),
                F.lit("LINE_NUMBER_MUST_BE_POSITIVE")
            ),

            # Quantity validation
            F.when(
                F.col("quantity_raw").isNull()
                | (F.trim(F.col("quantity_raw")) == ""),
                F.lit("MISSING_QUANTITY")
            ),

            F.when(
                F.col("quantity_raw").isNotNull()
                & F.col("quantity").isNull(),
                F.lit("INVALID_QUANTITY_FORMAT")
            ),

            F.when(
                F.col("quantity").isNotNull()
                & (F.col("quantity") <= 0),
                F.lit("QUANTITY_MUST_BE_POSITIVE")
            ),

            # Unit-price validation
            F.when(
                F.col("unit_price_raw").isNull()
                | (F.trim(F.col("unit_price_raw")) == ""),
                F.lit("MISSING_UNIT_PRICE")
            ),

            F.when(
                F.col("unit_price_raw").isNotNull()
                & F.col("unit_price").isNull(),
                F.lit("INVALID_UNIT_PRICE_FORMAT")
            ),

            F.when(
                F.col("unit_price").isNotNull()
                & (F.col("unit_price") <= 0),
                F.lit("UNIT_PRICE_MUST_BE_POSITIVE")
            ),

            # Discount validation
            F.when(
                F.col("discount_pct_raw").isNull()
                | (F.trim(F.col("discount_pct_raw")) == ""),
                F.lit("MISSING_DISCOUNT_PERCENTAGE")
            ),

            F.when(
                F.col("discount_pct_raw").isNotNull()
                & F.col("discount_pct").isNull(),
                F.lit("INVALID_DISCOUNT_FORMAT")
            ),

            F.when(
                F.col("discount_pct").isNotNull()
                & (
                    (F.col("discount_pct") < 0)
                    | (F.col("discount_pct") > 100)
                ),
                F.lit("DISCOUNT_OUT_OF_RANGE")
            ),

            # Operational values
            F.when(
                F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            ),

            F.when(
                F.col("load_date").isNull(),
                F.lit("MISSING_LOAD_DATE")
            )
        )
    )

    .withColumn(
        "validation_status",
        F.when(
            F.length(F.col("rejection_reason")) > 0,
            F.lit("REJECTED")
        ).otherwise(
            F.lit("VALID")
        )
    )

    .drop(
        "_order_exists",
        "_product_exists"
    )

    .cache()
)

print("Order-item validation rules applied successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20D: Split valid and rejected order-item records
# ============================================================

order_items_rejected = (
    order_items_validated
    .filter(
        F.col("validation_status") == "REJECTED"
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
)

order_items_valid_before_dedupe = (
    order_items_validated
    .filter(
        F.col("validation_status") == "VALID"
    )
    .drop(
        "rejection_reason",
        "validation_status"
    )
)

ingested_order_item_count = (
    order_items_validated.count()
)

rejected_order_item_count = (
    order_items_rejected.count()
)

valid_order_item_count_before_dedupe = (
    order_items_valid_before_dedupe.count()
)

print(
    f"Order-item rows ingested: "
    f"{ingested_order_item_count:,}"
)

print(
    f"Valid rows before deduplication: "
    f"{valid_order_item_count_before_dedupe:,}"
)

print(
    f"Rejected rows: "
    f"{rejected_order_item_count:,}"
)

print(
    "Reconciliation passed:",
    ingested_order_item_count
    == valid_order_item_count_before_dedupe
    + rejected_order_item_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20E: Deduplicate valid order-item records
# ============================================================

order_item_dedup_window = (
    Window
    .partitionBy("order_item_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("load_date").desc_nulls_last()
    )
)

order_items_valid_deduplicated = (
    order_items_valid_before_dedupe

    .withColumn(
        "_record_rank",
        F.row_number().over(
            order_item_dedup_window
        )
    )

    .filter(
        F.col("_record_rank") == 1
    )

    .drop("_record_rank")
)

valid_order_item_count_after_dedupe = (
    order_items_valid_deduplicated.count()
)

order_item_duplicates_removed = (
    valid_order_item_count_before_dedupe
    - valid_order_item_count_after_dedupe
)

print(
    f"Valid rows after deduplication: "
    f"{valid_order_item_count_after_dedupe:,}"
)

print(
    f"Duplicate order-item records removed: "
    f"{order_item_duplicates_removed:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20F: Check composite line uniqueness
# ============================================================

duplicate_order_lines = (
    order_items_valid_deduplicated

    .groupBy(
        "order_id",
        "line_number"
    )

    .count()

    .filter(
        F.col("count") > 1
    )
)

duplicate_order_line_count = (
    duplicate_order_lines.count()
)

print(
    f"Duplicate order_id + line_number combinations: "
    f"{duplicate_order_line_count:,}"
)

display(
    duplicate_order_lines
    .orderBy(
        "order_id",
        "line_number"
    )
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20G: Write order-item quarantine and staging outputs
# ============================================================

(
    order_items_rejected
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(QUARANTINE_PATHS["order_items"])
)

(
    order_items_valid_deduplicated
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(STAGING_PATHS["order_items"])
)

print(
    "Order-item quarantine output:",
    QUARANTINE_PATHS["order_items"]
)

print(
    "Order-item staging output:",
    STAGING_PATHS["order_items"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 20H: Review order-item validation results
# ============================================================

print("Rejected order items by reason:")

display(
    order_items_rejected
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

print("Sample rejected order items:")

display(
    order_items_rejected
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "line_number",
        "quantity",
        "unit_price",
        "discount_pct",
        "rejection_reason",
        "load_date"
    )
    .orderBy("order_item_id")
    .limit(30)
)

print("Sample valid order items:")

display(
    order_items_valid_deduplicated
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "line_number",
        "quantity",
        "unit_price",
        "discount_pct",
        "updated_at",
        "load_date"
    )
    .orderBy("order_item_id")
    .limit(20)
)

order_items_validated.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 21A: Ingest raw payments with an explicit schema
# ============================================================

payments_source_schema = T.StructType([
    T.StructField("payment_id", T.LongType(), True),
    T.StructField("order_id", T.LongType(), True),
    T.StructField("transaction_id", T.StringType(), True),
    T.StructField("payment_method", T.StringType(), True),

    # Read conversion-sensitive values as strings first
    T.StructField("payment_amount", T.StringType(), True),
    T.StructField("payment_status", T.StringType(), True),
    T.StructField("failure_reason", T.StringType(), True),
    T.StructField("payment_date", T.StringType(), True),
    T.StructField("updated_at", T.StringType(), True)
])

payments_ingested = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(payments_source_schema)
    .csv(RAW_PATHS["payments"])

    # Preserve original source values
    .withColumnRenamed(
        "payment_amount",
        "payment_amount_raw"
    )
    .withColumnRenamed(
        "payment_date",
        "payment_date_raw"
    )
    .withColumnRenamed(
        "updated_at",
        "updated_at_raw"
    )

    # Convert payment amount into decimal
    .withColumn(
        "payment_amount",
        F.col("payment_amount_raw").cast(
            T.DecimalType(14, 2)
        )
    )

    # Standardize mixed payment-date formats
    .withColumn(
        "payment_date",
        parse_mixed_date("payment_date_raw")
    )

    # Convert update timestamp
    .withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp("updated_at_raw"),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    # Discover and cast the folder partition column
    .withColumn(
        "load_date",
        F.col("load_date").cast("date")
    )

    # Operational metadata
    .withColumn(
        "pipeline_run_id",
        F.lit(RUN_ID)
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
)

payment_ingested_count = payments_ingested.count()

print(
    f"Payment rows ingested: "
    f"{payment_ingested_count:,}"
)

print("\nPayment ingestion schema:")
payments_ingested.printSchema()

display(
    payments_ingested
    .select(
        "payment_id",
        "order_id",
        "transaction_id",
        "payment_method",
        "payment_amount_raw",
        "payment_amount",
        "payment_status",
        "failure_reason",
        "payment_date_raw",
        "payment_date",
        "updated_at",
        "load_date"
    )
    .orderBy("payment_id", "updated_at")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 21B: Check payment-to-order referential integrity
# ============================================================

valid_payment_order_keys = (
    orders_valid_deduplicated
    .select("order_id")
    .filter(
        F.col("order_id").isNotNull()
    )
    .distinct()
    .withColumn(
        "_order_exists",
        F.lit(True)
    )
)

payments_with_order_check = (
    payments_ingested
    .join(
        valid_payment_order_keys,
        on="order_id",
        how="left"
    )
)

print(
    "Payment-to-order referential-integrity "
    "check completed."
)

display(
    payments_with_order_check
    .filter(
        F.col("order_id").isNotNull()
        & F.col("_order_exists").isNull()
    )
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 21C: Apply payment validation rules
# ============================================================

VALID_PAYMENT_STATUSES = [
    "PAID",
    "FAILED",
    "PENDING",
    "REFUNDED"
]

VALID_PAYMENT_METHODS = [
    "CREDIT_CARD",
    "DEBIT_CARD",
    "PAYPAL",
    "APPLE_PAY",
    "GIFT_CARD"
]

payments_validated = (
    payments_with_order_check

    .withColumn(
        "rejection_reason",
        F.concat_ws(
            " | ",

            # Payment business key
            F.when(
                F.col("payment_id").isNull(),
                F.lit("MISSING_PAYMENT_ID")
            ),

            # Order foreign key
            F.when(
                F.col("order_id").isNull(),
                F.lit("MISSING_ORDER_ID")
            ),

            F.when(
                F.col("order_id").isNotNull()
                & F.col("_order_exists").isNull(),
                F.lit("ORDER_NOT_FOUND")
            ),

            # Gateway transaction reference
            F.when(
                F.col("transaction_id").isNull()
                | (
                    F.trim(
                        F.col("transaction_id")
                    ) == ""
                ),
                F.lit("MISSING_TRANSACTION_ID")
            ),

            # Payment method
            F.when(
                F.col("payment_method").isNull()
                | (
                    F.trim(
                        F.col("payment_method")
                    ) == ""
                ),
                F.lit("MISSING_PAYMENT_METHOD")
            ),

            F.when(
                F.col("payment_method").isNotNull()
                & ~F.col("payment_method").isin(
                    VALID_PAYMENT_METHODS
                ),
                F.lit("INVALID_PAYMENT_METHOD")
            ),

            # Payment amount
            F.when(
                F.col("payment_amount_raw").isNull()
                | (
                    F.trim(
                        F.col("payment_amount_raw")
                    ) == ""
                ),
                F.lit("MISSING_PAYMENT_AMOUNT")
            ),

            F.when(
                F.col("payment_amount_raw").isNotNull()
                & F.col("payment_amount").isNull(),
                F.lit("INVALID_PAYMENT_AMOUNT_FORMAT")
            ),

            F.when(
                F.col("payment_amount").isNotNull()
                & (
                    F.col("payment_amount") <= 0
                ),
                F.lit("PAYMENT_AMOUNT_MUST_BE_POSITIVE")
            ),

            # Payment status
            F.when(
                F.col("payment_status").isNull()
                | (
                    F.trim(
                        F.col("payment_status")
                    ) == ""
                ),
                F.lit("MISSING_PAYMENT_STATUS")
            ),

            F.when(
                F.col("payment_status").isNotNull()
                & ~F.col("payment_status").isin(
                    VALID_PAYMENT_STATUSES
                ),
                F.lit("INVALID_PAYMENT_STATUS")
            ),

            # Failed payments should explain why they failed
            F.when(
                (
                    F.col("payment_status") == "FAILED"
                )
                & (
                    F.col("failure_reason").isNull()
                    | (
                        F.trim(
                            F.col("failure_reason")
                        ) == ""
                    )
                ),
                F.lit(
                    "MISSING_PAYMENT_FAILURE_REASON"
                )
            ),

            # Payment date
            F.when(
                F.col("payment_date_raw").isNull()
                | (
                    F.trim(
                        F.col("payment_date_raw")
                    ) == ""
                ),
                F.lit("MISSING_PAYMENT_DATE")
            ),

            F.when(
                F.col("payment_date_raw").isNotNull()
                & F.col("payment_date").isNull(),
                F.lit("INVALID_PAYMENT_DATE")
            ),

            # A payment should not occur after its load date
            # in this generated source model
            F.when(
                F.col("payment_date").isNotNull()
                & F.col("load_date").isNotNull()
                & (
                    F.col("payment_date")
                    > F.col("load_date")
                ),
                F.lit("PAYMENT_DATE_AFTER_LOAD_DATE")
            ),

            # Operational fields
            F.when(
                F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            ),

            F.when(
                F.col("load_date").isNull(),
                F.lit("MISSING_LOAD_DATE")
            )
        )
    )

    .withColumn(
        "validation_status",
        F.when(
            F.length(
                F.col("rejection_reason")
            ) > 0,
            F.lit("REJECTED")
        ).otherwise(
            F.lit("VALID")
        )
    )

    .drop("_order_exists")
    .cache()
)

print(
    "Payment validation rules applied successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 21D: Split valid and rejected payment records
# ============================================================

payments_rejected = (
    payments_validated
    .filter(
        F.col("validation_status") == "REJECTED"
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
)

payments_valid_before_dedupe = (
    payments_validated
    .filter(
        F.col("validation_status") == "VALID"
    )
    .drop(
        "rejection_reason",
        "validation_status"
    )
)

ingested_payment_count = (
    payments_validated.count()
)

rejected_payment_count = (
    payments_rejected.count()
)

valid_payment_count_before_dedupe = (
    payments_valid_before_dedupe.count()
)

print(
    f"Payment rows ingested: "
    f"{ingested_payment_count:,}"
)

print(
    f"Valid payments before deduplication: "
    f"{valid_payment_count_before_dedupe:,}"
)

print(
    f"Rejected payment rows: "
    f"{rejected_payment_count:,}"
)

print(
    "Reconciliation passed:",
    ingested_payment_count
    == valid_payment_count_before_dedupe
    + rejected_payment_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 21E: Deduplicate payments using latest-update rule
# ============================================================

payment_dedup_window = (
    Window
    .partitionBy("payment_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("load_date").desc_nulls_last()
    )
)

payments_valid_deduplicated = (
    payments_valid_before_dedupe

    .withColumn(
        "_record_rank",
        F.row_number().over(
            payment_dedup_window
        )
    )

    .filter(
        F.col("_record_rank") == 1
    )

    .drop("_record_rank")
)

valid_payment_count_after_dedupe = (
    payments_valid_deduplicated.count()
)

payment_duplicates_removed = (
    valid_payment_count_before_dedupe
    - valid_payment_count_after_dedupe
)

print(
    f"Valid payments after deduplication: "
    f"{valid_payment_count_after_dedupe:,}"
)

print(
    f"Duplicate payment records removed: "
    f"{payment_duplicates_removed:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 21F: Write payment quarantine and staging outputs
# ============================================================

(
    payments_rejected
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(QUARANTINE_PATHS["payments"])
)

(
    payments_valid_deduplicated
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(STAGING_PATHS["payments"])
)

print(
    "Payment quarantine output:",
    QUARANTINE_PATHS["payments"]
)

print(
    "Payment staging output:",
    STAGING_PATHS["payments"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 21G: Review payment validation results
# ============================================================

print("Rejected payments by reason:")

display(
    payments_rejected
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

print("Sample rejected payments:")

display(
    payments_rejected
    .select(
        "payment_id",
        "order_id",
        "transaction_id",
        "payment_method",
        "payment_amount_raw",
        "payment_status",
        "failure_reason",
        "payment_date_raw",
        "rejection_reason",
        "load_date"
    )
    .orderBy("payment_id")
    .limit(30)
)

print("Payment statuses after cleaning:")

display(
    payments_valid_deduplicated
    .groupBy("payment_status")
    .count()
    .orderBy(F.desc("count"))
)

print("Failed payments by reason:")

display(
    payments_valid_deduplicated
    .filter(
        F.col("payment_status") == "FAILED"
    )
    .groupBy("failure_reason")
    .count()
    .orderBy(F.desc("count"))
)

payments_validated.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22A: Ingest raw shipments with an explicit schema
# ============================================================

shipments_source_schema = T.StructType([
    T.StructField("shipment_id", T.LongType(), True),
    T.StructField("order_id", T.LongType(), True),
    T.StructField("tracking_number", T.StringType(), True),
    T.StructField("carrier", T.StringType(), True),
    T.StructField("shipment_status", T.StringType(), True),

    # Preserve source date values as strings before conversion
    T.StructField("shipped_date", T.StringType(), True),
    T.StructField("expected_delivery_date", T.StringType(), True),
    T.StructField("actual_delivery_date", T.StringType(), True),

    T.StructField("updated_at", T.StringType(), True)
])

shipments_ingested = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(shipments_source_schema)
    .csv(RAW_PATHS["shipments"])

    # Preserve original source values
    .withColumnRenamed(
        "shipped_date",
        "shipped_date_raw"
    )
    .withColumnRenamed(
        "expected_delivery_date",
        "expected_delivery_date_raw"
    )
    .withColumnRenamed(
        "actual_delivery_date",
        "actual_delivery_date_raw"
    )
    .withColumnRenamed(
        "updated_at",
        "updated_at_raw"
    )

    # Standardize mixed date formats
    .withColumn(
        "shipped_date",
        parse_mixed_date("shipped_date_raw")
    )
    .withColumn(
        "expected_delivery_date",
        parse_mixed_date("expected_delivery_date_raw")
    )
    .withColumn(
        "actual_delivery_date",
        parse_mixed_date("actual_delivery_date_raw")
    )

    # Convert source timestamp
    .withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp("updated_at_raw"),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                "updated_at_raw",
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    )

    # Discovered from load_date=... folders
    .withColumn(
        "load_date",
        F.col("load_date").cast("date")
    )

    # Operational metadata
    .withColumn(
        "pipeline_run_id",
        F.lit(RUN_ID)
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
)

shipment_ingested_count = shipments_ingested.count()

print(
    f"Shipment rows ingested: "
    f"{shipment_ingested_count:,}"
)

print("\nShipment ingestion schema:")
shipments_ingested.printSchema()

display(
    shipments_ingested
    .select(
        "shipment_id",
        "order_id",
        "tracking_number",
        "carrier",
        "shipment_status",
        "shipped_date_raw",
        "shipped_date",
        "expected_delivery_date_raw",
        "expected_delivery_date",
        "actual_delivery_date_raw",
        "actual_delivery_date",
        "updated_at",
        "load_date"
    )
    .orderBy("shipment_id", "updated_at")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22B: Check shipment-to-order referential integrity
# ============================================================

orders_staging_for_shipments = (
    spark.read
    .format("delta")
    .load(STAGING_PATHS["orders"])
)

valid_shipment_order_keys = (
    orders_staging_for_shipments
    .select("order_id")
    .filter(F.col("order_id").isNotNull())
    .distinct()
    .withColumn(
        "_order_exists",
        F.lit(True)
    )
)

shipments_with_order_check = (
    shipments_ingested
    .join(
        valid_shipment_order_keys,
        on="order_id",
        how="left"
    )
)

print(
    "Shipment-to-order referential-integrity "
    "check completed."
)

display(
    shipments_with_order_check
    .filter(
        F.col("order_id").isNotNull()
        & F.col("_order_exists").isNull()
    )
    .select(
        "shipment_id",
        "order_id",
        "tracking_number",
        "carrier",
        "shipment_status",
        "load_date"
    )
    .orderBy("shipment_id")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22C: Apply shipment validation rules
# ============================================================

VALID_SHIPMENT_STATUSES = [
    "DELIVERED",
    "IN_TRANSIT",
    "LABEL_CREATED",
    "RETURNED"
]

VALID_CARRIERS = [
    "UPS",
    "FEDEX",
    "USPS",
    "DHL"
]

shipments_validated = (
    shipments_with_order_check

    .withColumn(
        "rejection_reason",
        F.concat_ws(
            " | ",

            # Shipment business key
            F.when(
                F.col("shipment_id").isNull(),
                F.lit("MISSING_SHIPMENT_ID")
            ),

            # Order foreign key
            F.when(
                F.col("order_id").isNull(),
                F.lit("MISSING_ORDER_ID")
            ),

            F.when(
                F.col("order_id").isNotNull()
                & F.col("_order_exists").isNull(),
                F.lit("ORDER_NOT_FOUND")
            ),

            # Tracking number
            F.when(
                F.col("tracking_number").isNull()
                | (F.trim(F.col("tracking_number")) == ""),
                F.lit("MISSING_TRACKING_NUMBER")
            ),

            # Carrier
            F.when(
                F.col("carrier").isNull()
                | (F.trim(F.col("carrier")) == ""),
                F.lit("MISSING_CARRIER")
            ),

            F.when(
                F.col("carrier").isNotNull()
                & ~F.col("carrier").isin(
                    VALID_CARRIERS
                ),
                F.lit("INVALID_CARRIER")
            ),

            # Shipment status
            F.when(
                F.col("shipment_status").isNull()
                | (F.trim(F.col("shipment_status")) == ""),
                F.lit("MISSING_SHIPMENT_STATUS")
            ),

            F.when(
                F.col("shipment_status").isNotNull()
                & ~F.col("shipment_status").isin(
                    VALID_SHIPMENT_STATUSES
                ),
                F.lit("INVALID_SHIPMENT_STATUS")
            ),

            # Shipped date
            F.when(
                F.col("shipped_date_raw").isNull()
                | (
                    F.trim(
                        F.col("shipped_date_raw")
                    ) == ""
                ),
                F.lit("MISSING_SHIPPED_DATE")
            ),

            F.when(
                F.col("shipped_date_raw").isNotNull()
                & F.col("shipped_date").isNull(),
                F.lit("INVALID_SHIPPED_DATE")
            ),

            # Expected delivery date
            F.when(
                F.col("expected_delivery_date_raw").isNull()
                | (
                    F.trim(
                        F.col(
                            "expected_delivery_date_raw"
                        )
                    ) == ""
                ),
                F.lit(
                    "MISSING_EXPECTED_DELIVERY_DATE"
                )
            ),

            F.when(
                F.col(
                    "expected_delivery_date_raw"
                ).isNotNull()
                & F.col(
                    "expected_delivery_date"
                ).isNull(),
                F.lit(
                    "INVALID_EXPECTED_DELIVERY_DATE"
                )
            ),

            # Expected delivery cannot precede shipment
            F.when(
                F.col("shipped_date").isNotNull()
                & F.col(
                    "expected_delivery_date"
                ).isNotNull()
                & (
                    F.col("expected_delivery_date")
                    < F.col("shipped_date")
                ),
                F.lit(
                    "EXPECTED_DELIVERY_BEFORE_SHIPPED_DATE"
                )
            ),

            # Actual delivery date format
            F.when(
                F.col(
                    "actual_delivery_date_raw"
                ).isNotNull()
                & (
                    F.trim(
                        F.col(
                            "actual_delivery_date_raw"
                        )
                    ) != ""
                )
                & F.col(
                    "actual_delivery_date"
                ).isNull(),
                F.lit(
                    "INVALID_ACTUAL_DELIVERY_DATE"
                )
            ),

            # Delivered or returned shipments require an actual date
            F.when(
                F.col("shipment_status").isin(
                    "DELIVERED",
                    "RETURNED"
                )
                & F.col(
                    "actual_delivery_date"
                ).isNull(),
                F.lit(
                    "MISSING_ACTUAL_DELIVERY_DATE"
                )
            ),

            # Actual delivery cannot precede shipment
            F.when(
                F.col("shipped_date").isNotNull()
                & F.col(
                    "actual_delivery_date"
                ).isNotNull()
                & (
                    F.col("actual_delivery_date")
                    < F.col("shipped_date")
                ),
                F.lit(
                    "ACTUAL_DELIVERY_BEFORE_SHIPPED_DATE"
                )
            ),

            # Operational values
            F.when(
                F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            ),

            F.when(
                F.col("load_date").isNull(),
                F.lit("MISSING_LOAD_DATE")
            )
        )
    )

    .withColumn(
        "validation_status",
        F.when(
            F.length(
                F.col("rejection_reason")
            ) > 0,
            F.lit("REJECTED")
        ).otherwise(
            F.lit("VALID")
        )
    )

    .drop("_order_exists")
    .cache()
)

print(
    "Shipment validation rules applied successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22D: Split valid and rejected shipment records
# ============================================================

shipments_rejected = (
    shipments_validated
    .filter(
        F.col("validation_status") == "REJECTED"
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
)

shipments_valid_before_dedupe = (
    shipments_validated
    .filter(
        F.col("validation_status") == "VALID"
    )
    .drop(
        "rejection_reason",
        "validation_status"
    )
)

ingested_shipment_count = (
    shipments_validated.count()
)

rejected_shipment_count = (
    shipments_rejected.count()
)

valid_shipment_count_before_dedupe = (
    shipments_valid_before_dedupe.count()
)

print(
    f"Shipment rows ingested: "
    f"{ingested_shipment_count:,}"
)

print(
    f"Valid shipments before deduplication: "
    f"{valid_shipment_count_before_dedupe:,}"
)

print(
    f"Rejected shipment rows: "
    f"{rejected_shipment_count:,}"
)

print(
    "Reconciliation passed:",
    ingested_shipment_count
    == valid_shipment_count_before_dedupe
    + rejected_shipment_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22E: Deduplicate shipments using latest-update rule
# ============================================================

shipment_dedup_window = (
    Window
    .partitionBy("shipment_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("load_date").desc_nulls_last()
    )
)

shipments_valid_deduplicated = (
    shipments_valid_before_dedupe

    .withColumn(
        "_record_rank",
        F.row_number().over(
            shipment_dedup_window
        )
    )

    .filter(
        F.col("_record_rank") == 1
    )

    .drop("_record_rank")
)

valid_shipment_count_after_dedupe = (
    shipments_valid_deduplicated.count()
)

shipment_duplicates_removed = (
    valid_shipment_count_before_dedupe
    - valid_shipment_count_after_dedupe
)

print(
    f"Valid shipments after deduplication: "
    f"{valid_shipment_count_after_dedupe:,}"
)

print(
    f"Duplicate shipment records removed: "
    f"{shipment_duplicates_removed:,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22F: Calculate shipment delivery delay
# ============================================================

shipments_curated_staging = (
    shipments_valid_deduplicated

    .withColumn(
        "delivery_delay_days",
        F.when(
            F.col("actual_delivery_date").isNotNull()
            & F.col(
                "expected_delivery_date"
            ).isNotNull(),

            F.datediff(
                F.col("actual_delivery_date"),
                F.col("expected_delivery_date")
            )
        )
        .otherwise(
            F.lit(None).cast("int")
        )
    )

    .withColumn(
        "is_late_delivery",
        F.when(
            F.col("delivery_delay_days") > 0,
            F.lit(True)
        )
        .when(
            F.col("delivery_delay_days").isNotNull(),
            F.lit(False)
        )
        .otherwise(
            F.lit(None).cast("boolean")
        )
    )
)

print("Delivery-delay columns created.")

display(
    shipments_curated_staging
    .filter(
        F.col("delivery_delay_days").isNotNull()
    )
    .groupBy(
        "delivery_delay_days",
        "is_late_delivery"
    )
    .count()
    .orderBy("delivery_delay_days")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22G: Write shipment quarantine and staging outputs
# ============================================================

(
    shipments_rejected
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(QUARANTINE_PATHS["shipments"])
)

(
    shipments_curated_staging
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("load_date")
    .save(STAGING_PATHS["shipments"])
)

print(
    "Shipment quarantine output:",
    QUARANTINE_PATHS["shipments"]
)

print(
    "Shipment staging output:",
    STAGING_PATHS["shipments"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 22H: Review shipment validation results
# ============================================================

print("Rejected shipments by reason:")

display(
    shipments_rejected
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

print("Sample rejected shipments:")

display(
    shipments_rejected
    .select(
        "shipment_id",
        "order_id",
        "tracking_number",
        "carrier",
        "shipment_status",
        "shipped_date_raw",
        "expected_delivery_date_raw",
        "actual_delivery_date_raw",
        "rejection_reason",
        "load_date"
    )
    .orderBy("shipment_id")
    .limit(30)
)

print("Shipment statuses after cleaning:")

display(
    shipments_curated_staging
    .groupBy("shipment_status")
    .count()
    .orderBy(F.desc("count"))
)

print("Late-delivery distribution:")

display(
    shipments_curated_staging
    .filter(
        F.col("delivery_delay_days").isNotNull()
    )
    .groupBy(
        "delivery_delay_days"
    )
    .count()
    .orderBy("delivery_delay_days")
)

print("Sample delayed shipments:")

display(
    shipments_curated_staging
    .filter(
        F.col("is_late_delivery") == True
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

shipments_validated.unpersist()

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
