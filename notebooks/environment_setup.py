# Databricks notebook source
import json
import os

notebook_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
project_root = os.path.dirname(notebook_dir)
config_path = os.path.join(project_root, "config", "pipeline_config.json")

with open(config_path, "r") as f:
    config = json.load(f)

# 1. Only create the database schema
db_name = config["database_name"]
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# 2. Only make the empty directories if they don't exist
accounts_dest = os.path.join(project_root, config["paths"]["raw_accounts"])
checkpoint_dest = os.path.join(project_root, config["paths"]["checkpoint_dir"])

os.makedirs(os.path.dirname(accounts_dest), exist_ok=True)
os.makedirs(checkpoint_dest, exist_ok=True)

print("Environment directories and database verified. Ready for real data.")