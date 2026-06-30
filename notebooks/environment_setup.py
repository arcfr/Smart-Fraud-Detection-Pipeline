# Databricks notebook source
import json
import os

# Resolve the project root directory paths dynamically
notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)
config_path = os.path.join(project_root, "config", "pipeline_config.json")

# Load the central JSON configuration file
with open(config_path, "r") as f:
    config = json.load(f)

# Initialize the target Spark SQL database
db_name = config["database_name"]
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# Construct absolute paths for the raw data and checkpoint directories
accounts_dest = os.path.join(project_root, config["paths"]["raw_accounts"])
checkpoint_dest = os.path.join(project_root, config["paths"]["checkpoint_dir"])

# Create the physical landing folders inside the workspace if they do not exist
os.makedirs(os.path.dirname(accounts_dest), exist_ok=True)
os.makedirs(checkpoint_dest, exist_ok=True)

print("Environment directories and database verified")