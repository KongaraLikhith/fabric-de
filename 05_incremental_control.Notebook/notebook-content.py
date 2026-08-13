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
# STEP 30A: Incremental-processing control setup
# Notebook: 05_incremental_control
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timezone

CONTROL_RUN_ID = datetime.now(timezone.utc).strftime(
    "CONTROL_%Y%m%d_%H%M%S"
)

MANIFEST_TABLE = "processed_partition_log"

RAW_PATHS = {
    "customers": "Files/raw/customers",
    "products": "Files/raw/products",
    "orders": "Files/raw/orders",
    "order_items": "Files/raw/order_items",
    "payments": "Files/raw/payments",
    "shipments": "Files/raw/shipments"
}

print(f"Control run ID: {CONTROL_RUN_ID}")
print(f"Manifest table: {MANIFEST_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 30B: Create the processed-partition manifest
# ============================================================

manifest_schema = T.StructType([
    T.StructField(
        "dataset_name",
        T.StringType(),
        False
    ),
    T.StructField(
        "partition_value",
        T.StringType(),
        False
    ),
    T.StructField(
        "partition_path",
        T.StringType(),
        False
    ),
    T.StructField(
        "processing_status",
        T.StringType(),
        False
    ),
    T.StructField(
        "source_row_count",
        T.LongType(),
        True
    ),
    T.StructField(
        "pipeline_run_id",
        T.StringType(),
        False
    ),
    T.StructField(
        "processed_at",
        T.TimestampType(),
        False
    ),
    T.StructField(
        "processing_message",
        T.StringType(),
        True
    )
])

if not spark.catalog.tableExists(MANIFEST_TABLE):

    empty_manifest = spark.createDataFrame(
        [],
        manifest_schema
    )

    (
        empty_manifest
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(MANIFEST_TABLE)
    )

    print(
        f"Created manifest table: "
        f"{MANIFEST_TABLE}"
    )

else:
    print(
        f"Manifest table already exists: "
        f"{MANIFEST_TABLE}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 30C: Discover raw load_date partitions
# ============================================================

def discover_load_partitions(
    dataset_name: str,
    root_path: str
):
    """
    Return load_date partition folders found under one
    raw dataset path.
    """

    discovered_rows = []

    for item in notebookutils.fs.ls(root_path):

        item_name = item.name.rstrip("/")

        if (
            item.isDir
            and item_name.startswith("load_date=")
        ):
            partition_value = item_name.split(
                "=",
                1
            )[1]

            discovered_rows.append(
                (
                    dataset_name,
                    partition_value,
                    item.path.rstrip("/")
                )
            )

    return discovered_rows


all_partition_rows = []

for dataset_name, root_path in RAW_PATHS.items():

    dataset_partitions = discover_load_partitions(
        dataset_name,
        root_path
    )

    all_partition_rows.extend(
        dataset_partitions
    )


discovered_partition_schema = T.StructType([
    T.StructField(
        "dataset_name",
        T.StringType(),
        False
    ),
    T.StructField(
        "partition_value",
        T.StringType(),
        False
    ),
    T.StructField(
        "partition_path",
        T.StringType(),
        False
    )
])

discovered_partitions = (
    spark.createDataFrame(
        all_partition_rows,
        discovered_partition_schema
    )
)

print(
    f"Raw partitions discovered: "
    f"{discovered_partitions.count():,}"
)

display(
    discovered_partitions
    .orderBy(
        "dataset_name",
        "partition_value"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 30D: Find unprocessed partitions
# ============================================================

successfully_processed_partitions = (
    spark.table(MANIFEST_TABLE)

    .filter(
        F.col("processing_status") == "SUCCESS"
    )

    .select(
        "dataset_name",
        "partition_path"
    )

    .distinct()
)

pending_partitions = (
    discovered_partitions.alias("discovered")

    .join(
        successfully_processed_partitions.alias(
            "processed"
        ),
        on=[
            "dataset_name",
            "partition_path"
        ],
        how="left_anti"
    )
)

pending_partition_count = (
    pending_partitions.count()
)

print(
    f"Partitions waiting to be processed: "
    f"{pending_partition_count:,}"
)

display(
    pending_partitions
    .orderBy(
        "dataset_name",
        "partition_value"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 30E: One-time manifest bootstrap
#
# These partitions were already processed before the manifest
# table was introduced.
# ============================================================

bootstrap_manifest_rows = (
    pending_partitions

    .withColumn(
        "processing_status",
        F.lit("SUCCESS")
    )

    # We are not recounting every source partition during
    # bootstrap, so this remains null.
    .withColumn(
        "source_row_count",
        F.lit(None).cast("long")
    )

    .withColumn(
        "pipeline_run_id",
        F.lit(CONTROL_RUN_ID)
    )

    .withColumn(
        "processed_at",
        F.current_timestamp()
    )

    .withColumn(
        "processing_message",
        F.lit(
            "BOOTSTRAP: partition processed before "
            "manifest implementation"
        )
    )

    .select(
        "dataset_name",
        "partition_value",
        "partition_path",
        "processing_status",
        "source_row_count",
        "pipeline_run_id",
        "processed_at",
        "processing_message"
    )
)

bootstrap_count = bootstrap_manifest_rows.count()

if bootstrap_count > 0:

    (
        bootstrap_manifest_rows
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(MANIFEST_TABLE)
    )

    print(
        f"Bootstrapped {bootstrap_count:,} "
        f"processed partitions."
    )

else:
    print(
        "No partitions require manifest bootstrap."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 30F: Recheck for pending partitions
# ============================================================

processed_after_bootstrap = (
    spark.table(MANIFEST_TABLE)

    .filter(
        F.col("processing_status") == "SUCCESS"
    )

    .select(
        "dataset_name",
        "partition_path"
    )

    .distinct()
)

pending_after_bootstrap = (
    discovered_partitions

    .join(
        processed_after_bootstrap,
        on=[
            "dataset_name",
            "partition_path"
        ],
        how="left_anti"
    )
)

remaining_pending_count = (
    pending_after_bootstrap.count()
)

print(
    f"Partitions remaining after bootstrap: "
    f"{remaining_pending_count:,}"
)

display(
    spark.table(MANIFEST_TABLE)
    .orderBy(
        "dataset_name",
        "partition_value"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 31A: Create a new incremental customer partition
# ============================================================

from delta.tables import DeltaTable
from pyspark.sql.window import Window

NEW_LOAD_DATE = "2026-01-06"

STAGING_CUSTOMERS_PATH = (
    "Files/curated/staging/customers"
)

QUARANTINE_CUSTOMERS_PATH = (
    "Files/quarantine/customers"
)

NEW_CUSTOMER_PARTITION_PATH = (
    f"{RAW_PATHS['customers']}/"
    f"load_date={NEW_LOAD_DATE}"
)

# Check whether this partition was already successfully processed.
already_processed = (
    spark.table(MANIFEST_TABLE)
    .filter(
        (F.col("dataset_name") == "customers")
        & (
            F.col("partition_value")
            == NEW_LOAD_DATE
        )
        & (
            F.col("processing_status")
            == "SUCCESS"
        )
    )
    .limit(1)
    .count()
    > 0
)

if already_processed:

    print(
        f"Partition {NEW_LOAD_DATE} was already processed. "
        "Raw batch generation skipped."
    )

else:

    # --------------------------------------------------------
    # Read the existing clean customer target.
    # Customer 100 will receive a newer update.
    # --------------------------------------------------------

    existing_customers = (
        spark.read
        .format("delta")
        .load(STAGING_CUSTOMERS_PATH)
    )

    existing_customer_update = (
        existing_customers
        .filter(
            F.col("customer_id") == 100
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "state",
            "region",
            "customer_segment",
            "country",

            # Simulate an updated email address
            F.lit(
                "customer100.updated@example.com"
            ).alias("email"),

            "phone",

            # Convert back to the raw source column name
            F.col("signup_date_raw").alias(
                "signup_date"
            ),

            # Later source-update timestamp
            F.lit(
                f"{NEW_LOAD_DATE} 12:00:00"
            ).alias("updated_at"),

            "customer_status"
        )
    )

    # --------------------------------------------------------
    # Schema for two additional raw customer records
    # --------------------------------------------------------

    incremental_raw_schema = T.StructType([
        T.StructField(
            "customer_id",
            T.LongType(),
            False
        ),
        T.StructField(
            "first_name",
            T.StringType(),
            True
        ),
        T.StructField(
            "last_name",
            T.StringType(),
            True
        ),
        T.StructField(
            "state",
            T.StringType(),
            True
        ),
        T.StructField(
            "region",
            T.StringType(),
            True
        ),
        T.StructField(
            "customer_segment",
            T.StringType(),
            True
        ),
        T.StructField(
            "country",
            T.StringType(),
            True
        ),
        T.StructField(
            "email",
            T.StringType(),
            True
        ),
        T.StructField(
            "phone",
            T.StringType(),
            True
        ),
        T.StructField(
            "signup_date",
            T.StringType(),
            True
        ),
        T.StructField(
            "updated_at",
            T.StringType(),
            True
        ),
        T.StructField(
            "customer_status",
            T.StringType(),
            True
        )
    ])

    new_customer_rows = [
        # New valid customer
        (
            10001,
            "Maya",
            "Lee",
            "CA",
            "West",
            "CONSUMER",
            "US",
            "maya.lee10001@example.com",
            "+1-555-0001",
            "01/05/2026",
            "2026-01-06 12:30:00",
            "ACTIVE"
        ),

        # New invalid customer: missing email
        (
            10002,
            "Daniel",
            "Clark",
            "TX",
            "South",
            "CORPORATE",
            "US",
            None,
            "+1-555-0002",
            "05-Jan-2026",
            "2026-01-06 13:00:00",
            "ACTIVE"
        )
    ]

    new_customers = spark.createDataFrame(
        new_customer_rows,
        incremental_raw_schema
    )

    incremental_customer_raw = (
        existing_customer_update
        .unionByName(new_customers)
    )

    # Write directly into one load_date partition.
    # overwrite affects only this new partition folder.
    (
        incremental_customer_raw
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(NEW_CUSTOMER_PARTITION_PATH)
    )

    print(
        "Incremental customer batch created successfully."
    )

    print(
        f"Partition path: "
        f"{NEW_CUSTOMER_PARTITION_PATH}"
    )

    print(
        f"Raw records created: "
        f"{incremental_customer_raw.count():,}"
    )

    display(incremental_customer_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 31B: Discover and process the new customer partition
# ============================================================

# ------------------------------------------------------------
# 1. Locate the physical partition path
# ------------------------------------------------------------

matching_partition_rows = []

for item in notebookutils.fs.ls(
    RAW_PATHS["customers"]
):

    item_name = item.name.rstrip("/")

    if (
        item.isDir
        and item_name
        == f"load_date={NEW_LOAD_DATE}"
    ):
        matching_partition_rows.append(
            (
                "customers",
                NEW_LOAD_DATE,
                item.path.rstrip("/")
            )
        )

if len(matching_partition_rows) != 1:
    raise RuntimeError(
        "Expected exactly one customer partition for "
        f"{NEW_LOAD_DATE}, but found "
        f"{len(matching_partition_rows)}."
    )

new_partition_schema = T.StructType([
    T.StructField(
        "dataset_name",
        T.StringType(),
        False
    ),
    T.StructField(
        "partition_value",
        T.StringType(),
        False
    ),
    T.StructField(
        "partition_path",
        T.StringType(),
        False
    )
])

new_partition_df = spark.createDataFrame(
    matching_partition_rows,
    new_partition_schema
)

# ------------------------------------------------------------
# 2. Compare the partition against the manifest
# ------------------------------------------------------------

successful_manifest_keys = (
    spark.table(MANIFEST_TABLE)
    .filter(
        F.col("processing_status") == "SUCCESS"
    )
    .select(
        "dataset_name",
        "partition_path"
    )
    .distinct()
)

pending_customer_partition = (
    new_partition_df
    .join(
        successful_manifest_keys,
        on=[
            "dataset_name",
            "partition_path"
        ],
        how="left_anti"
    )
)

pending_count = pending_customer_partition.count()

print(
    f"New customer partitions waiting: "
    f"{pending_count:,}"
)

if pending_count == 0:

    print(
        "The partition was already processed. "
        "No target changes were made."
    )

else:

    # --------------------------------------------------------
    # 3. Read only the new partition
    # --------------------------------------------------------

    incremental_customer_schema = T.StructType([
        T.StructField(
            "customer_id",
            T.LongType(),
            True
        ),
        T.StructField(
            "first_name",
            T.StringType(),
            True
        ),
        T.StructField(
            "last_name",
            T.StringType(),
            True
        ),
        T.StructField(
            "state",
            T.StringType(),
            True
        ),
        T.StructField(
            "region",
            T.StringType(),
            True
        ),
        T.StructField(
            "customer_segment",
            T.StringType(),
            True
        ),
        T.StructField(
            "country",
            T.StringType(),
            True
        ),
        T.StructField(
            "email",
            T.StringType(),
            True
        ),
        T.StructField(
            "phone",
            T.StringType(),
            True
        ),
        T.StructField(
            "signup_date",
            T.StringType(),
            True
        ),
        T.StructField(
            "updated_at",
            T.StringType(),
            True
        ),
        T.StructField(
            "customer_status",
            T.StringType(),
            True
        )
    ])

    new_partition_path = (
        matching_partition_rows[0][2]
    )

    incremental_customers_ingested = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option(
            "basePath",
            RAW_PATHS["customers"]
        )
        .schema(incremental_customer_schema)
        .csv(new_partition_path)

        .withColumnRenamed(
            "signup_date",
            "signup_date_raw"
        )
        .withColumnRenamed(
            "updated_at",
            "updated_at_raw"
        )

        .withColumn(
            "signup_date",
            F.coalesce(
                F.to_date(
                    "signup_date_raw",
                    "yyyy-MM-dd"
                ),
                F.to_date(
                    "signup_date_raw",
                    "MM/dd/yyyy"
                ),
                F.to_date(
                    "signup_date_raw",
                    "dd-MMM-yyyy"
                )
            )
        )

        .withColumn(
            "updated_at",
            F.coalesce(
                F.to_timestamp(
                    "updated_at_raw"
                ),
                F.to_timestamp(
                    "updated_at_raw",
                    "yyyy-MM-dd HH:mm:ss"
                )
            )
        )

        .withColumn(
            "load_date",
            F.to_date(
                F.lit(NEW_LOAD_DATE)
            )
        )

        .withColumn(
            "pipeline_run_id",
            F.lit(CONTROL_RUN_ID)
        )

        .withColumn(
            "ingested_at",
            F.current_timestamp()
        )
    )

    # --------------------------------------------------------
    # 4. Validate the incremental records
    # --------------------------------------------------------

    incremental_customers_validated = (
        incremental_customers_ingested

        .withColumn(
            "rejection_reason",
            F.concat_ws(
                " | ",

                F.when(
                    F.col("customer_id").isNull(),
                    F.lit(
                        "MISSING_CUSTOMER_ID"
                    )
                ),

                F.when(
                    F.col("email").isNull()
                    | (
                        F.trim(
                            F.col("email")
                        ) == ""
                    ),
                    F.lit("MISSING_EMAIL")
                ),

                F.when(
                    F.col("region").isNull()
                    | (
                        F.trim(
                            F.col("region")
                        ) == ""
                    ),
                    F.lit("MISSING_REGION")
                ),

                F.when(
                    ~F.col("customer_status").isin(
                        "ACTIVE",
                        "INACTIVE",
                        "SUSPENDED"
                    ),
                    F.lit(
                        "INVALID_CUSTOMER_STATUS"
                    )
                ),

                F.when(
                    F.col("signup_date").isNull(),
                    F.lit(
                        "INVALID_SIGNUP_DATE"
                    )
                ),

                F.when(
                    F.col("updated_at").isNull(),
                    F.lit(
                        "INVALID_UPDATED_AT"
                    )
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
    )

    incremental_customers_rejected = (
        incremental_customers_validated
        .filter(
            F.col("validation_status")
            == "REJECTED"
        )
        .withColumn(
            "rejected_at",
            F.current_timestamp()
        )
    )

    incremental_customers_valid = (
        incremental_customers_validated
        .filter(
            F.col("validation_status")
            == "VALID"
        )
        .drop(
            "rejection_reason",
            "validation_status"
        )
    )

    # Deduplicate the incoming batch before MERGE.
    incremental_customer_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("updated_at")
            .desc_nulls_last()
        )
    )

    incremental_customers_valid_deduplicated = (
        incremental_customers_valid

        .withColumn(
            "_record_rank",
            F.row_number().over(
                incremental_customer_window
            )
        )

        .filter(
            F.col("_record_rank") == 1
        )

        .drop("_record_rank")
    )

    incremental_ingested_count = (
        incremental_customers_ingested.count()
    )

    incremental_valid_count = (
        incremental_customers_valid_deduplicated
        .count()
    )

    incremental_rejected_count = (
        incremental_customers_rejected.count()
    )

    print(
        f"Incremental rows ingested: "
        f"{incremental_ingested_count:,}"
    )

    print(
        f"Incremental valid rows: "
        f"{incremental_valid_count:,}"
    )

    print(
        f"Incremental rejected rows: "
        f"{incremental_rejected_count:,}"
    )

    print(
        "Incremental reconciliation passed:",
        incremental_ingested_count
        == incremental_valid_count
        + incremental_rejected_count
    )

    # --------------------------------------------------------
    # 5. MERGE valid records into customer staging
    # --------------------------------------------------------

    customer_target_before = (
        spark.read
        .format("delta")
        .load(STAGING_CUSTOMERS_PATH)
    )

    customer_count_before_merge = (
        customer_target_before.count()
    )

    # Ensure source columns match target columns.
    customer_target_columns = (
        customer_target_before.columns
    )

    incremental_customers_for_merge = (
        incremental_customers_valid_deduplicated
        .select(*customer_target_columns)
    )

    customer_target_delta = DeltaTable.forPath(
        spark,
        STAGING_CUSTOMERS_PATH
    )

    (
        customer_target_delta.alias("target")
        .merge(
            incremental_customers_for_merge.alias(
                "source"
            ),
            """
            target.customer_id =
            source.customer_id
            """
        )
        .whenMatchedUpdateAll(
            condition="""
            source.updated_at >=
            target.updated_at
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    customer_count_after_merge = (
        spark.read
        .format("delta")
        .load(STAGING_CUSTOMERS_PATH)
        .count()
    )

    print(
        f"Customer target before MERGE: "
        f"{customer_count_before_merge:,}"
    )

    print(
        f"Customer target after MERGE: "
        f"{customer_count_after_merge:,}"
    )

    # --------------------------------------------------------
    # 6. MERGE rejected records into quarantine
    # --------------------------------------------------------

    if incremental_rejected_count > 0:

        quarantine_target = (
            spark.read
            .format("delta")
            .load(QUARANTINE_CUSTOMERS_PATH)
        )

        quarantine_columns = (
            quarantine_target.columns
        )

        incremental_rejected_for_merge = (
            incremental_customers_rejected
            .select(*quarantine_columns)
        )

        quarantine_delta = DeltaTable.forPath(
            spark,
            QUARANTINE_CUSTOMERS_PATH
        )

        (
            quarantine_delta.alias("target")
            .merge(
                incremental_rejected_for_merge.alias(
                    "source"
                ),
                """
                target.customer_id
                    <=> source.customer_id
                AND target.updated_at_raw
                    <=> source.updated_at_raw
                AND target.load_date
                    <=> source.load_date
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(
            "Rejected incremental customers "
            "written to quarantine."
        )

    # --------------------------------------------------------
    # 7. Mark the partition successful only after writes finish
    # --------------------------------------------------------

    manifest_success_record = (
        pending_customer_partition

        .withColumn(
            "processing_status",
            F.lit("SUCCESS")
        )

        .withColumn(
            "source_row_count",
            F.lit(
                incremental_ingested_count
            ).cast("long")
        )

        .withColumn(
            "pipeline_run_id",
            F.lit(CONTROL_RUN_ID)
        )

        .withColumn(
            "processed_at",
            F.current_timestamp()
        )

        .withColumn(
            "processing_message",
            F.lit(
                "Incremental customer partition "
                "validated and merged successfully"
            )
        )

        .select(
            "dataset_name",
            "partition_value",
            "partition_path",
            "processing_status",
            "source_row_count",
            "pipeline_run_id",
            "processed_at",
            "processing_message"
        )
    )

    (
        manifest_success_record
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(MANIFEST_TABLE)
    )

    print(
        f"Partition {NEW_LOAD_DATE} marked SUCCESS."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 31C: Verify incremental and idempotent behavior
# ============================================================

customers_after_incremental = (
    spark.read
    .format("delta")
    .load(STAGING_CUSTOMERS_PATH)
)

print("Customer 100 and 10001 after MERGE:")

display(
    customers_after_incremental
    .filter(
        F.col("customer_id").isin(
            100,
            10001,
            10002
        )
    )
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "customer_status",
        "updated_at",
        "load_date",
        "pipeline_run_id"
    )
    .orderBy("customer_id")
)

print("Duplicate check:")

display(
    customers_after_incremental
    .filter(
        F.col("customer_id").isin(
            100,
            10001
        )
    )
    .groupBy("customer_id")
    .count()
    .orderBy("customer_id")
)

# Recheck pending partitions after manifest update
processed_customer_partitions = (
    spark.table(MANIFEST_TABLE)
    .filter(
        F.col("processing_status") == "SUCCESS"
    )
    .select(
        "dataset_name",
        "partition_path"
    )
    .distinct()
)

pending_after_processing = (
    new_partition_df
    .join(
        processed_customer_partitions,
        on=[
            "dataset_name",
            "partition_path"
        ],
        how="left_anti"
    )
)

print(
    "Pending partitions after processing:",
    pending_after_processing.count()
)

print("Manifest record for the new partition:")

display(
    spark.table(MANIFEST_TABLE)
    .filter(
        (F.col("dataset_name") == "customers")
        & (
            F.col("partition_value")
            == NEW_LOAD_DATE
        )
    )
    .orderBy(
        F.col("processed_at").desc()
    )
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
