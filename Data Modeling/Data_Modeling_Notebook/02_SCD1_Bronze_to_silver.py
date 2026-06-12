# Databricks notebook source
# DBTITLE 1,Setup: Import libraries and create parameter widget
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

# Create widget for incremental load parameter
dbutils.widgets.text("insert_timestamp", "", "Insert Timestamp (YYYY-MM-DD HH:MM:SS)")

# Get parameter value
insert_timestamp_param = dbutils.widgets.get("insert_timestamp")

print(f"Insert Timestamp Parameter: '{insert_timestamp_param}'")
print(f"Load Type: {'Full Load' if not insert_timestamp_param else 'Incremental Load'}")

# COMMAND ----------

# DBTITLE 1,Read data from bronze table
# Read from bronze table with conditional logic
if not insert_timestamp_param or insert_timestamp_param.strip() == "":
    # Full load
    print("Performing FULL LOAD from bronze table...")
    bronze_df = spark.table("data_modeling_demo.bronze.scd1")
else:
    # Incremental load - only new records
    print(f"Performing INCREMENTAL LOAD for records after: {insert_timestamp_param}")
    bronze_df = spark.table("data_modeling_demo.bronze.scd1") \
        .filter(F.col("insert_timestamp") > insert_timestamp_param)

record_count = bronze_df.count()
print(f"Records read from bronze: {record_count}")

if record_count == 0:
    print("⚠️  No records to process. Exiting...")
    dbutils.notebook.exit("No new records to process")

# COMMAND ----------

# DBTITLE 1,Apply data transformations
# Apply transformations
transformed_df = bronze_df.select(
    F.col("customer_id"),
    
    # Capitalize first letter of first_name and trim spaces
    F.initcap(F.trim(F.col("first_name"))).alias("first_name"),
    
    # Convert email to lowercase
    F.lower(F.col("email")).alias("email"),
    
    F.col("city"),
    
    # Convert registration_date from "DD-Mon-YYYY" format to timestamp
    F.to_timestamp(F.col("registration_date"), "dd-MMM-yyyy").alias("registration_date"),
    
    # Convert last_updated_ts from string to timestamp
    F.to_timestamp(F.col("last_updated_ts")).alias("last_updated_ts"),
    
    # Keep original insert_timestamp
    F.col("insert_timestamp")
)

print("✓ Transformations applied successfully")
print("Sample transformed data:")
display(transformed_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Data quality checks
# Data Quality Checks
print("=" * 60)
print("DATA QUALITY CHECKS")
print("=" * 60)

# Check 1: Null customer_id (primary key)
null_customer_ids = transformed_df.filter(F.col("customer_id").isNull()).count()
print(f"\n1. Null customer_ids: {null_customer_ids}")
if null_customer_ids > 0:
    print("   ⚠️  WARNING: Found null customer_ids!")

# Check 2: Duplicate customer_ids
duplicate_customers = transformed_df.groupBy("customer_id").count().filter(F.col("count") > 1)
duplicate_count = duplicate_customers.count()
print(f"\n2. Duplicate customer_ids: {duplicate_count}")
if duplicate_count > 0:
    print("   ⚠️  WARNING: Found duplicate customer_ids!")
    display(duplicate_customers)

# Check 3: Invalid email format
invalid_emails = transformed_df.filter(
    ~F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
).count()
print(f"\n3. Invalid email formats: {invalid_emails}")
if invalid_emails > 0:
    print("   ⚠️  WARNING: Found invalid email formats!")

# Check 4: Null required fields
null_checks = {
    "first_name": transformed_df.filter(F.col("first_name").isNull()).count(),
    "email": transformed_df.filter(F.col("email").isNull()).count(),
    "registration_date": transformed_df.filter(F.col("registration_date").isNull()).count(),
    "last_updated_ts": transformed_df.filter(F.col("last_updated_ts").isNull()).count()
}

print("\n4. Null values in key fields:")
for field, null_count in null_checks.items():
    status = "⚠️  WARNING" if null_count > 0 else "✓"
    print(f"   {status} {field}: {null_count}")

# Check 5: Future dates
future_dates = transformed_df.filter(
    (F.col("registration_date") > F.current_timestamp()) |
    (F.col("last_updated_ts") > F.current_timestamp())
).count()
print(f"\n5. Future dates: {future_dates}")
if future_dates > 0:
    print("   ⚠️  WARNING: Found future dates!")

# Filter out bad records for production (optional - adjust based on requirements)
quality_df = transformed_df.filter(
    F.col("customer_id").isNotNull() &
    F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$") &
    F.col("first_name").isNotNull()
)

filtered_count = transformed_df.count() - quality_df.count()
print(f"\n{'=' * 60}")
print(f"Records after quality filtering: {quality_df.count()} (Filtered out: {filtered_count})")
print(f"{'=' * 60}")

# COMMAND ----------

# DBTITLE 1,Write to silver table (SCD Type 1)
# SCD Type 1: Upsert to silver table
silver_table_name = "data_modeling_demo.silver.scd1"

# Check if silver table exists
if spark.catalog.tableExists(silver_table_name):
    print(f"Table {silver_table_name} exists. Performing MERGE (SCD Type 1)...")
    
    # Get existing silver table as Delta table
    silver_table = DeltaTable.forName(spark, silver_table_name)
    
    # Perform SCD Type 1 merge (update existing, insert new)
    silver_table.alias("target").merge(
        quality_df.alias("source"),
        "target.customer_id = source.customer_id"
    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    
    print("✓ MERGE completed successfully (SCD Type 1)")
    
else:
    print(f"Table {silver_table_name} does not exist. Creating new table...")
    
    # Create new silver table
    quality_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(silver_table_name)
    
    print(f"✓ Table {silver_table_name} created successfully")

# Show final record count
silver_count = spark.table(silver_table_name).count()
print(f"\nTotal records in silver table: {silver_count}")

# COMMAND ----------

# DBTITLE 1,Verify silver table results
# Verify the final silver table
print("\n" + "=" * 60)
print("SILVER TABLE VERIFICATION")
print("=" * 60)

silver_df = spark.table("data_modeling_demo.silver.scd1")

print(f"\nTotal records: {silver_df.count()}")
print(f"\nSchema:")
silver_df.printSchema()

print("\nSample records from silver table:")
display(silver_df.orderBy(F.col("insert_timestamp").desc()).limit(10))