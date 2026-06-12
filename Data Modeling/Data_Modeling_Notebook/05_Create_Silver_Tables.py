# Databricks notebook source
# DBTITLE 1,Create Silver Tables from CSV Files
from pyspark.sql import SparkSession
import os

# Define paths
volume_path = "/Volumes/data_modeling_demo/source/demo_volume/silver_data/"
schema_name = "data_modeling_demo.silver"


# COMMAND ----------

# List all CSV files in the volume
files = dbutils.fs.ls(volume_path)
csv_files = [f for f in files if f.name.endswith('.csv')]

print(f"Found {len(csv_files)} CSV files:")
for f in csv_files:
    print(f"  - {f.name}")

# COMMAND ----------

# Create a table for each CSV file
for file_info in csv_files:
    # Extract table name (remove .csv extension)
    table_name = file_info.name.replace('.csv', '')
    full_table_name = f"{schema_name}.{table_name}"
    file_path = file_info.path
    
    print(f"\nCreating table: {full_table_name}")
    
    # Read CSV file
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)
    
    # Write to Delta table
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(full_table_name)
    
    print(f"✓ Table {full_table_name} created successfully with {df.count()} rows")

print(f"\n✓ All silver tables created successfully!")