# Databricks notebook source
import json
import os
from pyspark.sql.functions import col, to_date, when

notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)
config_path = os.path.join(project_root, "config", "pipeline_config.json")

with open(config_path, "r") as f:
    config = json.load(f)

silver_df = spark.read.table(config["tables"]["silver_enriched_transactions"])
watchlist_df = spark.read.table(config["tables"]["bronze_watchlist"]) \
    .withColumn("flagged_date", to_date(col("flagged_date"), "yyyy-MM-dd"))

gold_flagged_df = silver_df.join(
    watchlist_df,
    on="account_id",
    how="left"
).withColumn(
    "fraud_flag",
    when(
        col("flagged_date").isNotNull() & (col("txn_date") >= col("flagged_date")), 
        "fraud"
    ).otherwise("normal")
).select(
    col("txn_id"),
    col("account_id"),
    col("customer_name"),
    col("txn_date"),
    col("amount"),
    col("merchant"),
    col("account_type"),
    col("fraud_type"),
    col("fraud_flag")
)

gold_flagged_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(config["tables"]["gold_flagged_transactions"])

print("Gold Fraud Flagged table updated successfully. Generating Business Insights Dashboard below...\n")

kpi_query = f"""
SELECT 
    (SELECT COUNT(*) FROM {config['tables']['gold_flagged_transactions']}) AS `Total Transactions`,
    (SELECT COUNT(*) FROM {config['tables']['gold_flagged_transactions']} WHERE fraud_flag = 'fraud') AS `Total Fraud Transactions`,
    (SELECT COUNT(*) FROM {config['tables']['gold_flagged_transactions']} WHERE fraud_flag = 'normal') AS `Total Normal Transactions`
"""
spark.sql(kpi_query).show()

distribution_query = f"""
SELECT 
    account_id,
    customer_name,
    COUNT(txn_id) AS total_fraud_txn_count,
    SUM(amount) AS total_fraudulent_volume_lost
FROM 
    {config['tables']['gold_flagged_transactions']}
WHERE 
    fraud_flag = 'fraud'
GROUP BY 
    account_id, customer_name
ORDER BY 
    total_fraudulent_volume_lost DESC
"""
spark.sql(distribution_query).show()