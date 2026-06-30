# Databricks notebook source
import json
from pyspark.sql.functions import col, to_date, coalesce, lit

# 1. Load Configurations
config_path = "/Workspace/smart-fraud-detection-pipeline/config/pipeline_config.json"
with open(config_path, "r") as f:
    config = json.load(f)

# 2. Extract Data from Bronze Layer
bronze_accounts_df = spark.read.table(config["tables"]["bronze_accounts"])
bronze_transactions_df = spark.read.table(config["tables"]["bronze_transactions"])

# 3. Transform and Cast Data Types
cleaned_accounts = bronze_accounts_df.select(
    col("account_id"),
    coalesce(col("customer_name"), lit("Unknown")).alias("customer_name"),
    col("account_type"),
    col("credit_limit").cast("decimal(15,2)").alias("credit_limit"),
    col("branch")
).filter(col("account_id").isNotNull())

cleaned_transactions = bronze_transactions_df.select(
    col("txn_id"),
    col("account_id"),
    to_date(col("txn_date"), "yyyy-MM-dd").alias("txn_date"),
    col("amount").cast("decimal(15,2)").alias("amount"),
    col("merchant")
).filter(col("txn_id").isNotNull() & col("account_id").isNotNull())

# 4. Join Datasets to Enrich Transactions
silver_enriched_df = cleaned_transactions.join(
    cleaned_accounts,
    on="account_id",
    how="inner"
)

# 5. Write to Silver Delta Table with Optimization Spark Configurations
silver_enriched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(config["tables"]["silver_enriched_transactions"])

# Execute Z-Ordering down to the join partition key to speed up future merges/queries
spark.sql(f"OPTIMIZE {config['tables']['silver_enriched_transactions']} ZORDER BY (account_id)")
print("Silver Layer Enriched and Optimized successfully.")