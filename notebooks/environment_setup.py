# Databricks notebook source
import json
import os

# 1. Load Configurations
config_path = "/Workspace/smart-fraud-detection-pipeline/config/pipeline_config.json"
with open(config_path, "r") as f:
    config = json.load(f)

db_name = config["database_name"]

# 2. Create the Database Environment
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
print(f"Database '{db_name}' initialized successfully.")

# 3. Setup Directories and Generate Mock Data Files
os.makedirs("/Workspace/smart-fraud-detection-pipeline/data/raw", exist_ok=True)
os.makedirs("/Workspace/smart-fraud-detection-pipeline/data/checkpoint", exist_ok=True)

# Generate mock accounts CSV
accounts_data = """account_id,customer_name,account_type,credit_limit,branch
ACC001,Archit Sahay,Savings,500000.0,Delhi
ACC002,John Doe,Current,100000.0,Mumbai
ACC003,Jane Smith,Savings,250000.0,Noida
"""

# Generate mock transactions CSV
transactions_data = """txn_id,account_id,txn_date,amount,merchant
TXN101,ACC001,2026-06-01,150.0,Amazon
TXN102,ACC002,2026-06-02,12000.0,CryptoEx
TXN103,ACC002,2026-06-15,45000.0,UnknownMerchant
TXN104,ACC003,2026-06-10,500.0,Starbucks
"""

# Generate mock watchlist CSV
fraud_watchlist_data = """account_id,fraud_type,flagged_date
ACC002,Identity Theft,2026-06-10
"""

with open(config["paths"]["raw_accounts"], "w") as f: f.write(accounts_data.strip())
with open(config["paths"]["raw_transactions"], "w") as f: f.write(transactions_data.strip())
with open(config["paths"]["raw_watchlist"], "w") as f: f.write(fraud_watchlist_data.strip())

print("Environment setup complete. Mock raw data files populated successfully.")