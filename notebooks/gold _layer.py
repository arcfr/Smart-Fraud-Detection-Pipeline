# Databricks notebook source
import json
from pyspark.sql.functions import col, to_date, when, count

# 1. Load Configurations
config_path = "/Workspace/smart-fraud-detection-pipeline/config/pipeline_config.json"
with open(config_path, "r") as f:
    config = json.load(f)

# 2. Load Datasets
silver_df = spark.read.table(config["tables"]["silver_enriched_transactions"])
watchlist_df = spark.read.table(config["tables"]["bronze_watchlist"]) \
    .withColumn("flagged_date", to_date(col("flagged_date"), "yyyy-MM-dd"))

# 3. Apply Time-Based Fraud Flag Logic
# A transaction is marked as fraud if the account is on the watchlist and the transaction occurred on or after the flagged date.
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

# 4. Save Final Output to Gold Table
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

kpi_df = spark.sql(kpi_query)
kpi_df.show()

# COMMAND ----------
# DBTITLE 1,Fraud Distribution per Account (Executed via Python)
# Detailed view tracking metrics for compromised accounts
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