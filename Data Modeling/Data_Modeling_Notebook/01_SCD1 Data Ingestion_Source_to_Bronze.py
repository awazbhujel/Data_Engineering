# Databricks notebook source
# DBTITLE 1,Auto Loader - Incremental CSV ingestion to Bronze
from pyspark.sql.functions import current_timestamp

# Source and target configuration
source_path = "/Volumes/data_modeling_demo/source/demo_volume/scd1/input_files/"
target_table = "data_modeling_demo.bronze.scd1"

# Read CSV files using Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/data_modeling_demo/source/demo_volume/scd1/_autoloader_schema")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

# Add insert_timestamp column
df_with_timestamp = df.withColumn("insert_timestamp", current_timestamp())

# Write to Bronze table in batch mode (trigger once)
(df_with_timestamp.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/data_modeling_demo/source/demo_volume/scd1/_autoloader_checkpoint")
    .trigger(once=True)
    .toTable(target_table)
)

print(f"✓ Incremental load to {target_table} completed successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT(*) as total_rows from data_modeling_demo.bronze.scd1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_modeling_demo.bronze.scd1 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC