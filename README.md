# ✈️ Airline Delay Cause Analysis

## 📘 Description
This project analyzes airline delay data using **Apache Spark** for distributed processing.  
An end-to-end **ETL pipeline** is implemented to clean, transform, and load data efficiently, automated through **Apache Airflow**, and visualized in **Power BI**.

## ⚙️ Tech Stack
- **Apache Spark using Java** – Data processing  
- **Apache Airflow** – Workflow orchestration  
- **MySQL** – Data storage  
- **Power BI** – Visualization  

## 💻 Implementation
### 1️⃣ Data Ingestion (`DataIngestion.java`)
Reads raw CSV files, validates schema, and stores data in the **Bronze layer** as Parquet and MySQL.

### 2️⃣ Data Cleaning (`DataCleaning.java`)
Removes duplicates, handles nulls, cleans categorical fields, and stores the cleaned dataset in the **Silver layer**.

### 3️⃣ Data Transformation (`DataTransformation.java`)
Performs aggregations, derives new columns, and creates **Fact** and **Dimension** tables stored in the **Gold layer**.

### 4️⃣ Airflow
Automates the ETL pipeline with Slack and Email alerts on task failure.
