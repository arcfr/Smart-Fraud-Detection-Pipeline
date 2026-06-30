# Databricks notebook source
import json
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Load Configurations
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)  # Steps out of 'notebooks/' into 'smart-fraud-detection-pipeline/'
config_path = os.path.join(project_root, "config", "pipeline_config.json")

with open(config_path, "r") as f:
    config = json.load(f)

# 2. Define Explicit Schemas (Schema Enforcement)
accounts_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("credit_limit", StringType(), True), # Ingested as string to clean in Silver
    StructField("branch", StringType(), True)
])

transactions_schema = StructType([
    StructField("txn_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("txn_date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("merchant", StringType(), True)
])

watchlist_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("fraud_type", StringType(), True),
    StructField("flagged_date", StringType(), True)
])

# 3. Read Raw CSV Files and Save as Delta Tables (Idempotent Overwrites)
def ingest_csv_to_bronze(source_path, target_table, schema):
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(source_path)
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)
    print(f"Successfully ingested {source_path} into Bronze Delta table: {target_table}")

# Run Ingestions
ingest_csv_to_bronze(config["paths"]["raw_accounts"], config["tables"]["bronze_accounts"], accounts_schema)
ingest_csv_to_bronze(config["paths"]["raw_transactions"], config["tables"]["bronze_transactions"], transactions_schema)
ingest_csv_to_bronze(config["paths"]["raw_watchlist"], config["tables"]["bronze_watchlist"], watchlist_schema)