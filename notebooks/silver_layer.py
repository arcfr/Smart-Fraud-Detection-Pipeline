# Databricks notebook source
import json
import os
from pyspark.sql.functions import col, to_date, coalesce, lit

notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)
config_path = os.path.join(project_root, "config", "pipeline_config.json")

with open(config_path, "r") as f:
    config = json.load(f)

bronze_accounts_df = spark.read.table(config["tables"]["bronze_accounts"])
bronze_transactions_df = spark.read.table(config["tables"]["bronze_transactions"])

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

silver_enriched_df = cleaned_transactions.join(
    cleaned_accounts,
    on="account_id",
    how="inner"
)

silver_enriched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(config["tables"]["silver_enriched_transactions"])

spark.sql(f"OPTIMIZE {config['tables']['silver_enriched_transactions']} ZORDER BY (account_id)")
print("Silver Layer Enriched and Optimized successfully.")