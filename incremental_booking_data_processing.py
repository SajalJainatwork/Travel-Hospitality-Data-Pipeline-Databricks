
# incremental_booking_data_processing.py
# Databricks-ready ETL script (metadata-driven incremental ingestion + SCD2 + fact aggregation)
# Usage: Attach to a running Databricks cluster. No notebook widget required because metadata drives ingestion.
# Make sure required libraries (pydeequ, delta) are installed on the cluster.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum
from delta.tables import DeltaTable
import re
from datetime import datetime, date
import sys

# Optional: PyDeequ imports (installed as library on cluster)
try:
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationSuite, VerificationResult
    PYDEEQU_AVAILABLE = True
except Exception as e:
    PYDEEQU_AVAILABLE = False

# -------------------------------
# Configurable variables
# -------------------------------
BOOKING_RAW_DIR = "dbfs:/DataEngineering/bookings_daily_data/"
CUSTOMER_RAW_DIR = "dbfs:/DataEngineering/customers_daily_data/"
PIPELINE_METADATA_TABLE = "gds_de_bootcamp.default.pipeline_metadata"
FACT_TABLE = "gds_de_bootcamp.default.booking_fact"
SCD_TABLE = "gds_de_bootcamp.default.customer_scd"
PIPELINE_NAME = "booking_customer_pipeline"   # value stored in metadata table for this pipeline

# -------------------------------
# Helper functions
# -------------------------------
def get_last_processed_date(spark):
    """Read metadata table and return last_processed_date as a date object."""
    # If metadata table doesn't exist, create and initialize it
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS gds_de_bootcamp.default")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {PIPELINE_METADATA_TABLE} (
            table_name STRING,
            last_processed_date DATE
        )
    """)
    # Insert initial record if not present
    spark.sql(f"""
        INSERT INTO {PIPELINE_METADATA_TABLE}
        SELECT '{PIPELINE_NAME}', DATE('1900-01-01')
        WHERE NOT EXISTS (SELECT 1 FROM {PIPELINE_METADATA_TABLE} WHERE table_name = '{PIPELINE_NAME}')
    """)
    meta_df = spark.sql(f"""
        SELECT last_processed_date FROM {PIPELINE_METADATA_TABLE}
        WHERE table_name = '{PIPELINE_NAME}'
    """)
    row = meta_df.collect()[0]
    return row["last_processed_date"]

def update_last_processed_date(spark, new_date):
    """Update metadata table after successful processing of a date."""
    spark.sql(f"""
        UPDATE {PIPELINE_METADATA_TABLE}
        SET last_processed_date = DATE('{new_date}')
        WHERE table_name = '{PIPELINE_NAME}'
    """)

def list_raw_dates(spark, raw_dir, pattern):
    """List files in raw_dir, extract dates matching the pattern and return sorted list of date objects."""
    files = dbutils.fs.ls(raw_dir)  # dbutils is available in Databricks
    dates = []
    for f in files:
        m = re.search(pattern, f.name)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                dates.append(dt)
            except Exception:
                continue
    dates = sorted(list(set(dates)))
    return dates

def run_pydeequ_checks(spark, df, checks):
    """Execute pydeequ checks if available, return True if success, False otherwise."""
    if not PYDEEQU_AVAILABLE:
        print("PyDeequ not available on this cluster — skipping DQ checks. Install pydeequ to enable checks.")
        return True
    vs = VerificationSuite(spark).onData(df)
    for c in checks:
        vs = vs.addCheck(c)
    result = vs.run()
    if result.status != "Success":
        print("Data quality checks failed. See details below.")
        try:
            result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
            display(result_df)
        except Exception:
            pass
        return False
    return True

# -------------------------------
# Core processing logic for a single date
# -------------------------------
def process_date(spark, date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    print(f"Processing date: {date_str}")
    booking_path = f"{BOOKING_RAW_DIR}bookings_{date_str}.csv"
    customer_path = f"{CUSTOMER_RAW_DIR}customers_{date_str}.csv"

    # 1) Read raw CSVs
    booking_df = spark.read.format("csv").option("header", True).option("inferSchema", True) \
        .option("multiLine", True).option("quote", "\"").load(booking_path)
    customer_df = spark.read.format("csv").option("header", True).option("inferSchema", True) \
        .option("multiLine", True).option("quote", "\"").load(customer_path)

    print("Raw booking rows:", booking_df.count())
    print("Raw customer rows:", customer_df.count())

    # 2) Run Data Quality checks (PyDeequ) - basic examples
    if PYDEEQU_AVAILABLE:
        check_booking = Check(spark, CheckLevel.Error, "Booking Data Check") \
            .hasSize(lambda x: x > 0) \
            .isUnique("booking_id") \
            .isComplete("customer_id") \
            .isComplete("amount") \
            .isNonNegative("amount") \
            .isNonNegative("quantity") \
            .isNonNegative("discount")

        check_customer = Check(spark, CheckLevel.Error, "Customer Data Check") \
            .hasSize(lambda x: x > 0) \
            .isUnique("customer_id") \
            .isComplete("customer_name") \
            .isComplete("customer_address") \
            .isComplete("phone_number") \
            .isComplete("email")

        if not run_pydeequ_checks(spark, booking_df, [check_booking]):
            raise ValueError("Booking data quality checks failed for date " + date_str)
        if not run_pydeequ_checks(spark, customer_df, [check_customer]):
            raise ValueError("Customer data quality checks failed for date " + date_str)
    else:
        print("Skipping pydeequ checks - ensure data quality before pushing to production.")

    # 3) Transformations: add ingestion_time, calculate total_cost, filter
    booking_df = booking_df.withColumn("ingestion_time", current_timestamp())
    df_joined = booking_df.join(customer_df, "customer_id", how="left")
    df_transformed = df_joined.withColumn("total_cost", col("amount") - col("discount")) \
        .filter(col("quantity") > 0)

    # 4) Aggregate for fact update
    df_agg = df_transformed.groupBy("booking_type", "customer_id").agg(
        _sum("total_cost").alias("total_amount_sum"),
        _sum("quantity").alias("total_quantity_sum")
    )

    # 5) Update booking_fact (Gold) - do incremental merge via full re-aggregation approach
    # If fact exists, union + re-aggregate to avoid duplicates
    catalog = spark._jsparkSession.catalog()
    fact_exists = catalog.tableExists(FACT_TABLE)
    if fact_exists:
        existing_fact = spark.read.format("delta").table(FACT_TABLE)
        combined = existing_fact.unionByName(df_agg, allowMissingColumns=True)
        df_final = combined.groupBy("booking_type", "customer_id").agg(
            _sum("total_amount_sum").alias("total_amount_sum"),
            _sum("total_quantity_sum").alias("total_quantity_sum")
        )
    else:
        df_final = df_agg

    # Write fact table (overwrite with latest aggregation)
    df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FACT_TABLE)
    print(f"Updated fact table: {FACT_TABLE} (rows: {df_final.count()})")

    # 6) SCD Type 2 on customer dimension (Silver)
    scd_exists = catalog.tableExists(SCD_TABLE)
    # Ensure customer_df has valid_from column - if not, add current date as valid_from
    if "valid_from" not in customer_df.columns:
        customer_df = customer_df.withColumn("valid_from", lit(date_str).cast("date"))
    if "valid_to" not in customer_df.columns:
        customer_df = customer_df.withColumn("valid_to", lit("9999-12-31").cast("date"))

    if scd_exists:
        scd_table = DeltaTable.forName(spark, SCD_TABLE)
        # Close existing active records by setting valid_to to new valid_from for matched customers
        scd_table.alias("scd").merge(
            source=customer_df.alias("updates"),
            condition="scd.customer_id = updates.customer_id AND scd.valid_to = '9999-12-31'"
        ).whenMatchedUpdate(set={
            "valid_to": "updates.valid_from"
        }).execute()

        # Append new records (new versions) after closing previous ones
        customer_df.write.format("delta").mode("append").saveAsTable(SCD_TABLE)
    else:
        # Create the SCD table initially
        customer_df.write.format("delta").mode("overwrite").saveAsTable(SCD_TABLE)
    print(f"Updated SCD table: {SCD_TABLE}")

# -------------------------------
# Main pipeline execution
# -------------------------------
if __name__ == "__main__":
    # Get Spark session
    spark = SparkSession.builder.getOrCreate()

    # Use metadata to determine which dates to process
    last_processed = get_last_processed_date(spark)
    print("Last processed date from metadata:", last_processed)

    booking_dates = list_raw_dates(spark, BOOKING_RAW_DIR, r'bookings_(\d{4}-\d{2}-\d{2})\\.csv')
    customer_dates = list_raw_dates(spark, CUSTOMER_RAW_DIR, r'customers_(\d{4}-\d{2}-\d{2})\\.csv')

    # Combine available dates intersecting both booking and customer availability
    pending_dates = sorted([d for d in booking_dates if d in customer_dates and d > last_processed])

    if not pending_dates:
        print("No pending dates to process. Exiting.")
        sys.exit(0)

    for dt in pending_dates:
        try:
            process_date(spark, dt)
            # After successful processing of dt, update metadata
            update_last_processed_date(spark, dt.strftime("%Y-%m-%d"))
            print(f"Successfully processed and updated metadata for {dt}")
        except Exception as e:
            print(f"Processing failed for {dt}: {e}")
            # Do not update metadata on failure; raise or continue based on your retry policy
            raise

