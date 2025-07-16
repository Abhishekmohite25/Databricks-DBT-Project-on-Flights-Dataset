# ✈️ **Flight Booking Data Pipeline with Databricks & DBT**  
This project demonstrates an end-to-end Data Engineering pipeline for processing flight booking data using Databricks, Delta Lake, Delta Live Tables, and DBT. It employs a multi-layer architecture (Bronze, Silver, Gold) following best practices for modern data lakehouses.  

![Architecture](https://github.com/Abhishekmohite25/Databricks-DBT-Project-on-Flights-Dataset/blob/2ba322a4fb18720fb2d2078df250658f16b9f67c/Screenshots/Architecture.png)

## 📊 Data Flow Overview  

### 🟤 **Bronze Layer — Raw Ingestion**  

Tooling: Databricks Autoloader + Spark Structured Streaming

Source Format: Incrementally loaded CSV files

**Key Features:**

- Autoloader automatically detects new files
- Streaming ingestion using Spark for real-time updates
- Parsed and transformed based on defined parameters and control flow in Databricks Jobs

Storage: Raw data stored in Delta Lake format (Unity Catalog managed tables)

![Bronze Layer](https://github.com/Abhishekmohite25/Databricks-DBT-Project-on-Flights-Dataset/blob/2ba322a4fb18720fb2d2078df250658f16b9f67c/Screenshots/bronze.png)

### ⚪️ **Silver Layer — Cleaned & Enriched Data**

Tooling: Delta Live Tables (DLT)

**Key Features:**

- Declarative data pipeline using LakeFlow
- Built-in idempotency for safe reruns
- Handles deduplication and schema evolution

Output: Refined Delta tables stored in Unity Catalog

![Silver Layer](https://github.com/Abhishekmohite25/Databricks-DBT-Project-on-Flights-Dataset/blob/2ba322a4fb18720fb2d2078df250658f16b9f67c/Screenshots/silver.png)

### 🟡 **Gold Layer — Dimensional Model**

Tooling: dbt integrated with Databricks

**Key Features:**

-Star Schema modeling: automated creation of fact and dimension tables
- Built-in support for Slowly Changing Dimensions (SCD Type 1)
- DBT models deployed and version-controlled

Use Case: BI-ready datasets consumed by downstream analytics (e.g., Synapse, Power BI)

![Gold Dimension](https://github.com/Abhishekmohite25/Databricks-DBT-Project-on-Flights-Dataset/blob/2ba322a4fb18720fb2d2078df250658f16b9f67c/Screenshots/gold_dimension.png)


![Gold Fact](https://github.com/Abhishekmohite25/Databricks-DBT-Project-on-Flights-Dataset/blob/2ba322a4fb18720fb2d2078df250658f16b9f67c/Screenshots/gold_fact.png)


![DBT 1](https://github.com/Abhishekmohite25/Databricks-DBT-Project-on-Flights-Dataset/blob/2ba322a4fb18720fb2d2078df250658f16b9f67c/Screenshots/dbt1.png)


![DBT 2](https://github.com/Abhishekmohite25/Databricks-DBT-Project-on-Flights-Dataset/blob/2ba322a4fb18720fb2d2078df250658f16b9f67c/Screenshots/dbt2.jpg)

## 📦 **Technology Stack**

| Component |	Purpose|
| --- | --- |
|Databricks |	Unified data platform for big data & ML |
|Delta Lake |	Open-source storage layer with ACID |
|Spark |	Distributed data processing |
|Autoloader | Real-time data ingestion from cloud files |
|Delta Live Tables |	Declarative ETL with orchestration |
|DBT |	Data modeling and transformations |

## 🧭 **Key Features**

- End-to-end ETL pipeline with layered architecture

- Real-time, incremental ingestion using Autoloader + Spark Streaming

- Scalable, declarative transformation logic using Delta Live Tables

- SCD Type 1 management with automated dimension/fact table builders

- Seamless integration with dbt Cloud or dbt Core

- Warehouse-ready outputs for analytics and reporting
