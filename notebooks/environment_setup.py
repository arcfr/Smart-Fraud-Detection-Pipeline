# Databricks notebook source
import json
import os

notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)
config_path = os.path.join(project_root, "config", "pipeline_config.json")

with open(config_path, "r") as f:
    config = json.load(f)

db_name = config["database_name"]
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

accounts_dest = os.path.join(project_root, config["paths"]["raw_accounts"])
transactions_dest = os.path.join(project_root, config["paths"]["raw_transactions"])
watchlist_dest = os.path.join(project_root, config["paths"]["raw_watchlist"])
checkpoint_dest = os.path.join(project_root, config["paths"]["checkpoint_dir"])

os.makedirs(os.path.dirname(accounts_dest), exist_ok=True)
os.makedirs(checkpoint_dest, exist_ok=True)

accounts_data = """account_id,customer_name,account_type,credit_limit,branch
ACC001,Archit Sahay,Savings,500000.0,Delhi
ACC002,John Doe,Current,100000.0,Mumbai
ACC003,Jane Smith,Savings,250000.0,Noida
"""

transactions_data = """txn_id,account_id,txn_date,amount,merchant
TXN101,ACC001,2026-06-01,150.0,Amazon
TXN102,ACC002,2026-06-02,12000.0,CryptoEx
TXN103,ACC002,2026-06-15,45000.0,UnknownMerchant
TXN104,ACC003,2026-06-10,500.0,Starbucks
"""

fraud_watchlist_data = """account_id,fraud_type,flagged_date
ACC002,Identity Theft,2026-06-10
"""

with open(accounts_dest, "w") as f: 
    f.write(accounts_data.strip())

with open(transactions_dest, "w") as f: 
    f.write(transactions_data.strip())

with open(watchlist_dest, "w") as f: 
    f.write(fraud_watchlist_data.strip())

print("Environment setup complete.")