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
# STEP 32A: Data-quality and run-summary setup
# Notebook: 06_quality_run_summary
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timezone

REPORT_RUN_ID = datetime.now(timezone.utc).strftime(
    "REPORT_%Y%m%d_%H%M%S"
)

REPORT_PATHS = {
    "quality": "Files/reports/data_quality_report",
    "run_summary": "Files/reports/pipeline_run_summary"
}

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

BUSINESS_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id"
}

print(f"Report run ID: {REPORT_RUN_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 32B: Build dataset-level data-quality metrics
# ============================================================

quality_rows = []

for dataset_name, business_key in BUSINESS_KEYS.items():

    # Raw source count
    raw_df = (
        spark.read
        .option("header", "true")
        .csv(RAW_PATHS[dataset_name])
    )

    raw_count = raw_df.count()

    # Rejected records
    rejected_df = (
        spark.read
        .format("delta")
        .load(QUARANTINE_PATHS[dataset_name])
    )

    rejected_count = rejected_df.count()

    # Valid, deduplicated staging records
    staging_df = (
        spark.read
        .format("delta")
        .load(STAGING_PATHS[dataset_name])
    )

    staging_count = staging_df.count()

    distinct_key_count = (
        staging_df
        .select(business_key)
        .distinct()
        .count()
    )

    # Raw = rejected + valid before deduplication.
    # Staging contains valid rows after deduplication.
    duplicates_removed = (
        raw_count
        - rejected_count
        - staging_count
    )

    # Count rejected rows by broad data-quality category.
    completeness_failures = (
        rejected_df
        .filter(
            F.col("rejection_reason").contains("MISSING_")
        )
        .count()
    )

    referential_integrity_failures = (
        rejected_df
        .filter(
            F.col("rejection_reason").rlike(
                "CUSTOMER_NOT_FOUND|"
                "ORDER_NOT_FOUND|"
                "PRODUCT_NOT_FOUND"
            )
        )
        .count()
    )

    accepted_value_failures = (
        rejected_df
        .filter(
            F.col("rejection_reason").rlike(
                "INVALID_.*STATUS|"
                "INVALID_REGION|"
                "INVALID_CUSTOMER_SEGMENT|"
                "INVALID_PAYMENT_METHOD|"
                "INVALID_CARRIER|"
                "INVALID_ORDER_CHANNEL|"
                "INVALID_CURRENCY_CODE"
            )
        )
        .count()
    )

    numeric_range_failures = (
        rejected_df
        .filter(
            F.col("rejection_reason").rlike(
                "MUST_BE_POSITIVE|"
                "CANNOT_BE_NEGATIVE|"
                "OUT_OF_RANGE|"
                "EXCEEDS"
            )
        )
        .count()
    )

    date_failures = (
        rejected_df
        .filter(
            F.col("rejection_reason").rlike(
                "INVALID_.*DATE|"
                "DATE_AFTER|"
                "BEFORE_SHIPPED_DATE"
            )
        )
        .count()
    )

    format_failures = (
        rejected_df
        .filter(
            F.col("rejection_reason").contains(
                "_FORMAT"
            )
        )
        .count()
    )

    uniqueness_passed = (
        staging_count == distinct_key_count
    )

    reconciliation_passed = (
        raw_count
        == rejected_count
        + staging_count
        + duplicates_removed
    )

    quality_rows.append(
        (
            REPORT_RUN_ID,
            dataset_name,
            business_key,
            raw_count,
            rejected_count,
            staging_count,
            distinct_key_count,
            duplicates_removed,
            completeness_failures,
            referential_integrity_failures,
            accepted_value_failures,
            numeric_range_failures,
            date_failures,
            format_failures,
            uniqueness_passed,
            reconciliation_passed,
            datetime.now(timezone.utc).replace(
                tzinfo=None
            )
        )
    )

print("Dataset-level quality metrics calculated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 32C: Create data-quality report DataFrame
# ============================================================

quality_report_schema = T.StructType([
    T.StructField("report_run_id", T.StringType(), False),
    T.StructField("dataset_name", T.StringType(), False),
    T.StructField("business_key", T.StringType(), False),
    T.StructField("raw_row_count", T.LongType(), False),
    T.StructField("rejected_row_count", T.LongType(), False),
    T.StructField("staging_row_count", T.LongType(), False),
    T.StructField("distinct_key_count", T.LongType(), False),
    T.StructField("duplicates_removed", T.LongType(), False),
    T.StructField("completeness_failures", T.LongType(), False),
    T.StructField(
        "referential_integrity_failures",
        T.LongType(),
        False
    ),
    T.StructField(
        "accepted_value_failures",
        T.LongType(),
        False
    ),
    T.StructField(
        "numeric_range_failures",
        T.LongType(),
        False
    ),
    T.StructField("date_failures", T.LongType(), False),
    T.StructField("format_failures", T.LongType(), False),
    T.StructField("uniqueness_passed", T.BooleanType(), False),
    T.StructField(
        "reconciliation_passed",
        T.BooleanType(),
        False
    ),
    T.StructField("reported_at", T.TimestampType(), False)
])

data_quality_report = spark.createDataFrame(
    quality_rows,
    quality_report_schema
)

display(
    data_quality_report
    .orderBy("dataset_name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 32D: Create detailed rejection-reason summary
# ============================================================

rejection_reason_dfs = []

for dataset_name in BUSINESS_KEYS.keys():

    dataset_rejections = (
        spark.read
        .format("delta")
        .load(QUARANTINE_PATHS[dataset_name])

        .select(
            F.lit(dataset_name).alias(
                "dataset_name"
            ),
            "rejection_reason"
        )
    )

    rejection_reason_dfs.append(
        dataset_rejections
    )

all_rejections = rejection_reason_dfs[0]

for rejection_df in rejection_reason_dfs[1:]:
    all_rejections = all_rejections.unionByName(
        rejection_df
    )

rejection_reason_summary = (
    all_rejections
    .groupBy(
        "dataset_name",
        "rejection_reason"
    )
    .count()
    .orderBy(
        "dataset_name",
        F.desc("count")
    )
)

display(rejection_reason_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 32E: Write data-quality reports
# ============================================================

# Lakehouse table for SQL access
(
    data_quality_report
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("data_quality_report")
)

# JSON output for GitHub deliverables
(
    data_quality_report
    .coalesce(1)
    .write
    .mode("overwrite")
    .json(REPORT_PATHS["quality"])
)

# Detailed rejection-reason table
(
    rejection_reason_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("rejection_reason_summary")
)

print("Created table: data_quality_report")
print("Created table: rejection_reason_summary")
print(
    "JSON report location:",
    REPORT_PATHS["quality"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 33A: Calculate pipeline-wide metrics
# ============================================================

quality_totals = (
    data_quality_report
    .agg(
        F.sum("raw_row_count").alias(
            "total_raw_rows"
        ),
        F.sum("rejected_row_count").alias(
            "total_rejected_rows"
        ),
        F.sum("staging_row_count").alias(
            "total_staging_rows"
        ),
        F.sum("duplicates_removed").alias(
            "total_duplicates_removed"
        ),
        F.sum(
            "referential_integrity_failures"
        ).alias("total_unmatched_keys"),
        F.min(
            F.col("uniqueness_passed").cast("int")
        ).alias("_all_uniqueness_passed"),
        F.min(
            F.col(
                "reconciliation_passed"
            ).cast("int")
        ).alias("_all_reconciliation_passed")
    )
    .collect()[0]
)

# Collect ingestion timestamps from persisted staging data.
ingestion_timestamp_dfs = []

for dataset_name in BUSINESS_KEYS.keys():

    timestamp_df = (
        spark.read
        .format("delta")
        .load(STAGING_PATHS[dataset_name])
        .select("ingested_at")
    )

    ingestion_timestamp_dfs.append(timestamp_df)

all_ingestion_timestamps = ingestion_timestamp_dfs[0]

for timestamp_df in ingestion_timestamp_dfs[1:]:
    all_ingestion_timestamps = (
        all_ingestion_timestamps.unionByName(
            timestamp_df
        )
    )

pipeline_started_at = (
    all_ingestion_timestamps
    .agg(
        F.min("ingested_at").alias(
            "pipeline_started_at"
        )
    )
    .collect()[0]["pipeline_started_at"]
)

pipeline_completed_at = (
    spark.table("fact_orders")
    .agg(
        F.max("curated_at").alias(
            "pipeline_completed_at"
        )
    )
    .collect()[0]["pipeline_completed_at"]
)

if (
    pipeline_started_at is not None
    and pipeline_completed_at is not None
):
    execution_time_seconds = int(
        (
            pipeline_completed_at
            - pipeline_started_at
        ).total_seconds()
    )
else:
    execution_time_seconds = None

print("Pipeline-wide metrics calculated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 33B: Calculate curated output metrics
# ============================================================

dim_customer_count = spark.table(
    "dim_customers"
).count()

dim_product_count = spark.table(
    "dim_products"
).count()

fact_order_line_count = spark.table(
    "fact_order_lines"
).count()

fact_order_count = spark.table(
    "fact_orders"
).count()

fact_order_distinct_count = (
    spark.table("fact_orders")
    .select("order_id")
    .distinct()
    .count()
)

fact_order_partitions = (
    spark.table("fact_orders")
    .select(
        "order_year",
        "order_month"
    )
    .distinct()
    .orderBy(
        "order_year",
        "order_month"
    )
    .collect()
)

partition_labels = [
    f"{row['order_year']}-{int(row['order_month']):02d}"
    for row in fact_order_partitions
]

output_partition_list = ",".join(
    partition_labels
)

output_partition_count = len(
    partition_labels
)

fact_order_uniqueness_passed = (
    fact_order_count
    == fact_order_distinct_count
)

print(
    f"Fact order output partitions: "
    f"{output_partition_count}"
)

print(output_partition_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 33C: Create pipeline-run summary
# ============================================================

all_quality_checks_passed = (
    quality_totals[
        "_all_uniqueness_passed"
    ] == 1
    and quality_totals[
        "_all_reconciliation_passed"
    ] == 1
    and fact_order_uniqueness_passed
)

pipeline_status = (
    "SUCCESS_WITH_QUARANTINE"
    if (
        all_quality_checks_passed
        and quality_totals[
            "total_rejected_rows"
        ] > 0
    )
    else (
        "SUCCESS"
        if all_quality_checks_passed
        else "QUALITY_CHECK_FAILED"
    )
)

run_summary_rows = [
    (
        REPORT_RUN_ID,
        pipeline_status,
        quality_totals["total_raw_rows"],
        quality_totals[
            "total_rejected_rows"
        ],
        quality_totals[
            "total_staging_rows"
        ],
        quality_totals[
            "total_duplicates_removed"
        ],
        quality_totals[
            "total_unmatched_keys"
        ],
        dim_customer_count,
        dim_product_count,
        fact_order_line_count,
        fact_order_count,
        fact_order_distinct_count,
        fact_order_uniqueness_passed,
        output_partition_count,
        output_partition_list,
        pipeline_started_at,
        pipeline_completed_at,
        execution_time_seconds,
        all_quality_checks_passed,
        datetime.now(timezone.utc).replace(
            tzinfo=None
        )
    )
]

run_summary_schema = T.StructType([
    T.StructField("report_run_id", T.StringType(), False),
    T.StructField("pipeline_status", T.StringType(), False),
    T.StructField("total_raw_rows", T.LongType(), False),
    T.StructField("total_rejected_rows", T.LongType(), False),
    T.StructField("total_staging_rows", T.LongType(), False),
    T.StructField(
        "total_duplicates_removed",
        T.LongType(),
        False
    ),
    T.StructField("total_unmatched_keys", T.LongType(), False),
    T.StructField("dim_customer_rows", T.LongType(), False),
    T.StructField("dim_product_rows", T.LongType(), False),
    T.StructField(
        "fact_order_line_rows",
        T.LongType(),
        False
    ),
    T.StructField("fact_order_rows", T.LongType(), False),
    T.StructField(
        "distinct_fact_order_ids",
        T.LongType(),
        False
    ),
    T.StructField(
        "fact_order_uniqueness_passed",
        T.BooleanType(),
        False
    ),
    T.StructField(
        "output_partition_count",
        T.IntegerType(),
        False
    ),
    T.StructField(
        "output_partitions",
        T.StringType(),
        True
    ),
    T.StructField(
        "pipeline_started_at",
        T.TimestampType(),
        True
    ),
    T.StructField(
        "pipeline_completed_at",
        T.TimestampType(),
        True
    ),
    T.StructField(
        "execution_time_seconds",
        T.LongType(),
        True
    ),
    T.StructField(
        "all_quality_checks_passed",
        T.BooleanType(),
        False
    ),
    T.StructField("reported_at", T.TimestampType(), False)
])

pipeline_run_summary = spark.createDataFrame(
    run_summary_rows,
    run_summary_schema
)

display(pipeline_run_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 33D: Write pipeline-run summary
# ============================================================

(
    pipeline_run_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("pipeline_run_summary")
)

(
    pipeline_run_summary
    .coalesce(1)
    .write
    .mode("overwrite")
    .json(REPORT_PATHS["run_summary"])
)

print("Created table: pipeline_run_summary")
print(
    "JSON run-summary location:",
    REPORT_PATHS["run_summary"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 33E: Final project quality overview
# ============================================================

print("PIPELINE RUN SUMMARY")

display(
    spark.table("pipeline_run_summary")
)

print("DATA QUALITY BY DATASET")

display(
    spark.table("data_quality_report")
    .select(
        "dataset_name",
        "raw_row_count",
        "rejected_row_count",
        "staging_row_count",
        "duplicates_removed",
        "referential_integrity_failures",
        "uniqueness_passed",
        "reconciliation_passed"
    )
    .orderBy("dataset_name")
)

print("TOP REJECTION REASONS")

display(
    spark.table("rejection_reason_summary")
    .orderBy(
        F.desc("count")
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
# STEP 35: FINAL FULL-SCALE PIPELINE VERIFICATION
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

# ------------------------------------------------------------
# 1. Validate required source volumes using the quality report
# ------------------------------------------------------------

minimum_required_rows = [
    ("customers", 1_000_000),
    ("products", 100_000),
    ("orders", 5_000_000),
    ("order_items", 20_000_000),
    ("payments", 5_000_000),
    ("shipments", 4_500_000)
]

minimum_schema = T.StructType([
    T.StructField("dataset_name", T.StringType(), False),
    T.StructField("minimum_required_rows", T.LongType(), False)
])

minimums_df = spark.createDataFrame(
    minimum_required_rows,
    minimum_schema
)

latest_quality_report = (
    spark.table("data_quality_report")
    .select(
        "dataset_name",
        "raw_row_count",
        "rejected_row_count",
        "staging_row_count",
        "duplicates_removed",
        "referential_integrity_failures",
        "uniqueness_passed",
        "reconciliation_passed"
    )
)

volume_verification = (
    minimums_df
    .join(
        latest_quality_report,
        on="dataset_name",
        how="left"
    )
    .withColumn(
        "volume_requirement_passed",
        F.col("raw_row_count")
        >= F.col("minimum_required_rows")
    )
)

print("FULL-SCALE DATASET VERIFICATION")

display(
    volume_verification
    .orderBy("dataset_name")
)

# ------------------------------------------------------------
# 2. Display the final pipeline summary
# ------------------------------------------------------------

print("FINAL PIPELINE RUN SUMMARY")

final_run_summary = (
    spark.table("pipeline_run_summary")
    .orderBy(F.col("reported_at").desc())
    .limit(1)
)

display(final_run_summary)

# ------------------------------------------------------------
# 3. Verify curated fact-table uniqueness
# ------------------------------------------------------------

fact_orders_df = spark.table("fact_orders")
fact_order_lines_df = spark.table("fact_order_lines")

fact_order_rows = fact_orders_df.count()

distinct_order_ids = (
    fact_orders_df
    .select("order_id")
    .distinct()
    .count()
)

fact_order_line_rows = fact_order_lines_df.count()

distinct_order_item_ids = (
    fact_order_lines_df
    .select("order_item_id")
    .distinct()
    .count()
)

print("CURATED TABLE GRAIN CHECKS")

print(
    f"fact_orders rows: {fact_order_rows:,}"
)

print(
    f"Distinct order IDs: {distinct_order_ids:,}"
)

print(
    "fact_orders uniqueness passed:",
    fact_order_rows == distinct_order_ids
)

print(
    f"fact_order_lines rows: "
    f"{fact_order_line_rows:,}"
)

print(
    f"Distinct order-item IDs: "
    f"{distinct_order_item_ids:,}"
)

print(
    "fact_order_lines uniqueness passed:",
    fact_order_line_rows
    == distinct_order_item_ids
)

# ------------------------------------------------------------
# 4. Verify physical output partitions
# ------------------------------------------------------------

fact_partitions = (
    fact_orders_df
    .select(
        "order_year",
        "order_month"
    )
    .distinct()
    .orderBy(
        "order_year",
        "order_month"
    )
)

print("FACT ORDER OUTPUT PARTITIONS")

display(fact_partitions)

# ------------------------------------------------------------
# 5. Produce one overall pass/fail result
# ------------------------------------------------------------

all_volume_checks_passed = (
    volume_verification
    .filter(
        F.col("volume_requirement_passed") == False
    )
    .count()
    == 0
)

summary_row = final_run_summary.collect()[0]

overall_verification_passed = (
    all_volume_checks_passed
    and summary_row["all_quality_checks_passed"]
    and fact_order_rows == distinct_order_ids
    and fact_order_line_rows
        == distinct_order_item_ids
)

print("========================================")
print(
    "ALL VOLUME CHECKS PASSED:",
    all_volume_checks_passed
)
print(
    "ALL QUALITY CHECKS PASSED:",
    summary_row["all_quality_checks_passed"]
)
print(
    "OVERALL FULL-SCALE VERIFICATION PASSED:",
    overall_verification_passed
)
print("========================================")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
