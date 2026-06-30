# Databricks notebook source
import json
import os
from pyspark.sql.functions import col, to_date, coalesce, lit

# Resolve the project root directory paths dynamically
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)
config_path = os.path.join(project_root, "config", "pipeline_config.json")

# Load the central JSON configuration file
with open(config_path, "r") as f:
    config = json.load(f)

# Load the raw ingestion tables from the Bronze database layer
bronze_accounts_df = spark.read.table(config["tables"]["bronze_accounts"])
bronze_transactions_df = spark.read.table(config["tables"]["bronze_transactions"])

# Standardize account metrics, fill blank names, and filter out null account keys
cleaned_accounts = bronze_accounts_df.select(
    col("account_id"),
    coalesce(col("customer_name"), lit("Unknown")).alias("customer_name"),
    col("account_type"),
    col("credit_limit").cast("decimal(15,2)").alias("credit_limit"),
    col("branch")
).filter(col("account_id").isNotNull())

# Standardize transaction amounts, cast string dates, and filter out null transaction keys
cleaned_transactions = bronze_transactions_df.select(
    col("txn_id"),
    col("account_id"),
    to_date(col("txn_date"), "yyyy-MM-dd").alias("txn_date"),
    col("amount").cast("decimal(15,2)").alias("amount"),
    col("merchant")
).filter(col("txn_id").isNotNull() & col("account_id").isNotNull())

# Combine transaction records with customer profile details using an inner join
silver_enriched_df = cleaned_transactions.join(
    cleaned_accounts,
    on="account_id",
    how="inner"
)

# Save the unified, cleaned dataset into an intermediate Silver Delta Lake table
silver_enriched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(config["tables"]["silver_enriched_transactions"])

# Co-locate data physically by account ID to maximize downstream performance
spark.sql(f"OPTIMIZE {config['tables']['silver_enriched_transactions']} ZORDER BY (account_id)")
print("Silver Layer Enriched and Optimized successfully.")