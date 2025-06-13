# ETL Pipeline with Apache Airflow and SQL Server

This project implements a full **Extract, Transform, Load (ETL)** pipeline using **Apache Airflow**, Python, and **Microsoft SQL Server**. The DAG reads from CSV and JSON sources, transforms the data, and performs incremental loading into a target SQL Server database.

## Project Structure

- **Extract:** Reads raw CSV/JSON data from the local file system and SQL Server source tables, and loads it into a staging database.
- **Transform:** Cleans data, handles duplicates and null values, maps hashed IDs to integers, normalizes data formats, and ensures data quality.
- **Load:** Performs incremental updates by comparing the staging and final tables. It handles insertions, updates (via delete & insert), and maintains referential integrity.

## Technologies Used

- **Apache Airflow** – Task orchestration
- **Pandas** – Data manipulation
- **SQLAlchemy** – Database connection and interaction
- **Microsoft SQL Server** – Data storage (Staging and Final)
- **ODBC Driver 17** – SQL Server connector for Python

## Data Sources

- `CUSTOMERS.csv`
- `GEO_LOCATION.csv`
- `ORDER_ITEMS.csv`
- `ORDER_PAYMENTS.csv`
- `ORDER_REVIEW_RATINGS.csv`
- `ORDERS.csv`
- `PRODUCTS.json`
- `sellers` table from SQL Server

## DAG Info

- **DAG Name:** `Etl_Pipline_dag`
- **Schedule:** Daily (`@daily`)
- **Tasks:**
  - `extract_task`
  - `transform_task`
  - `load_task`

## Features

- Handles null values and data formatting for categorical and datetime fields
- Replaces hashed IDs with integer keys for consistency
- Cleans and deduplicates data
- Supports incremental loading to avoid redundant inserts
- Uses SQLAlchemy's `to_sql` for database writes with `fast_executemany=True`

## 🛠️ Setup Instructions

1. Install Airflow and required Python packages:
   ```bash
   pip install apache-airflow pandas sqlalchemy pyodbc
