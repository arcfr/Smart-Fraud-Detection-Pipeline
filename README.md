# Smart-Fraud-Detection-Pipeline

**Author:** Archit Sahay

## Overview

Smart-Fraud-Detection-Pipeline is an end-to-end data engineering pipeline that processes financial transaction data using a Medallion Architecture (Bronze, Silver, and Gold layers) built with Apache Spark and Delta Lake.

The pipeline ingests transactional logs, customer account information, and fraud watchlists, performs data cleaning and enrichment, and identifies fraudulent transactions using a chronological business rule. It is designed to produce reliable analytical datasets that can be consumed by dashboards and reporting systems.

---

## Fraud Detection Logic

Fraud detection is based on the following business rule:

Transaction date > watchlist flagged date

If an account appears in the fraud watchlist, every transaction occurring **on or after** the flagged date is classified as **Fraud**, while transactions occurring **before** the flagged date remain classified as **Normal**.

---

## Pipeline Architecture

The project follows the Medallion Architecture.

### Bronze Layer – Raw Data Ingestion

- Reads raw CSV datasets.
- Dynamically maps columns using header names.
- Completely independent of column ordering.
- Performs schema validation.
- Stores immutable raw data as Delta tables.

### Silver Layer – Data Cleaning & Enrichment

- Removes records with missing identifiers.
- Standardizes date columns (`yyyy-MM-dd`).
- Converts currency values into `decimal(15,2)` format.
- Cleans inconsistent values.
- Joins transactions with account information.
- Optimizes Delta tables using **ZORDER** on `account_id`.

### Gold Layer – Fraud Detection & Analytics

- Joins enriched transaction data with the fraud watchlist.
- Applies chronological fraud detection logic.
- Labels each transaction as **Fraud** or **Normal**.
- Generates business metrics for reporting and dashboards.

---

## Technologies Used

- Apache Spark (PySpark)
- Delta Lake
- Python
- SQL
- JSON Configuration
- CSV
- Databricks Notebook Environment

---

## Repository Structure

```text
smart-fraud-detection-pipeline/
│
├── config/
│   └── pipeline_config.json
│
├── data/
│   ├── checkpoint/
│   └── raw/
│       ├── accounts.csv
│       ├── transactions.csv
│       └── known_fraud_accounts.csv
│
├── notebooks/
│   ├── environment_setup.py
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   └── gold_layer.py
│
├── outputs.docx
└── README.md

