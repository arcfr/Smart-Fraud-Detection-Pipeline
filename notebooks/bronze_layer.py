# Databricks notebook source
import json
import os
from pyspark.sql.types import StructType, StructField, StringType

# Resolve the project root directory paths dynamically
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)
config_path = os.path.join(project_root, "config", "pipeline_config.json")

# Load the central JSON configuration file
with open(config_path, "r") as f:
    config = json.load(f)

# Define the baseline structural schema for customer account master data
accounts_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("credit_limit", StringType(), True),
    StructField("branch", StringType(), True)
])

# Define the baseline structural schema for the core transactional ledger data
transactions_schema = StructType([
    StructField("txn_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("txn_date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("merchant", StringType(), True)
])

# Define the baseline structural schema for high-risk watched accounts data
watchlist_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("fraud_type", StringType(), True),
    StructField("flagged_date", StringType(), True)
])

# Define a generic reusable ingestion function to copy raw CSV files into Delta tables
def ingest_csv_to_bronze(relative_source_path, target_table, schema):
    absolute_source_path = os.path.join(project_root, relative_source_path)
    
    # Standardize path string formats for the Databricks Workspace environment
    if not absolute_source_path.startswith("/Workspace"):
        absolute_source_path = os.path.abspath(absolute_source_path)
    
    # Terminate execution early if the source dataset file is missing
    if not os.path.exists(absolute_source_path):
        raise FileNotFoundError(f"File not found at: {absolute_source_path}")
        
    # Load the CSV file dynamically using string fields to map column text headers natively
    raw_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load(absolute_source_path)
    
    # Re-order and match CSV columns dynamically by name to prevent positional shift bugs
    aligned_df = raw_df.select([field.name for field in schema])
    
    # Save the aligned raw dataset as a permanent immutable Bronze Delta Lake table
    aligned_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)
    
    print(f"Successfully ingested {target_table}")

# Execute raw data ingestion pipelines for each of the three foundational data entities
ingest_csv_to_bronze(config["paths"]["raw_accounts"], config["tables"]["bronze_accounts"], accounts_schema)
ingest_csv_to_bronze(config["paths"]["raw_transactions"], config["tables"]["bronze_transactions"], transactions_schema)
ingest_csv_to_bronze(config["paths"]["raw_watchlist"], config["tables"]["bronze_watchlist"], watchlist_schema)