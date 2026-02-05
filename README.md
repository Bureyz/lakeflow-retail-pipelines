# Lakeflow Retail Pipelines Demo

**Author:** Krzysztof Burejza  
**Technology:** Databricks Lakeflow Pipelines, Unity Catalog, Spark Structured Streaming.

---

## Project Overview
This project demonstrates a modern **Data Warehouse** implementation using **Databricks Lakeflow Pipelines**. It builds a complete **Star Schema** (Fact & Dimensions) with support for Slowly Changing Dimensions (SCD Type 2) and real-time ingestion.

### Architecture
The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold):

1.  **Bronze (Raw Ingestion)**:
    *   **Auto Loader (CloudFiles)**: Ingests CSV & JSON files from Unity Catalog Volumes.
    *   **Schema Evolution**: Handles unexpected column changes automatically.
    *   **Logic**: Streaming Tables (`STREAM read_files` / `readStream`).

2.  **Silver (Clean & Conformed)**:
    *   **SCD Type 2 (History)**: Tracking Customer address changes over time (Full History).
    *   **SCD Type 1 (Current)**: Products & Loyalty segments (Latest State).
    *   **Data Quality**: Enforcing expectations (e.g., valid order amounts) with `EXPECT` constraints.

3.  **Gold (Business Layer - Star Schema)**:
    *   **Fact Table**: `Fact_Sales` (Transactional grain).
    *   **Dimensions**: `Dim_Customer`, `Dim_Product`, `Dim_Loyalty`, `Dim_Date`.
    *   **Logic**: Materialized Views (Batch) for high-performance BI queries.

---

## How to Run

### 1. Prerequisites
*   Databricks Workspace with Unity Catalog enabled.
*   **Catalog**: `lakeflow_demo` (or adjust paths).
*   **Schema**: `default`.
*   **Volume**: `dataset` (created inside the schema).

### 2. Data Setup
Run the setup script **once** to generate historical and streaming data:
```bash
# Run this notebook in Databricks
generate_data/01_Data_Setup.py
```
*This script will populate `/Volumes/lakeflow_demo/default/dataset/landing/` with initial history and simulated updates.*

### 3. Deploy Pipeline
You can run the pipeline in two flavors: **SQL** or **Python**.

#### Option A: SQL Version
1.  Create a new Lakeflow Pipeline.
2.  Point "Source Code" to the **`Lakeflow_demo/SQL/`** folder.
3.  Set Target Schema to `lakeflow_demo`.
4.  Click **Start**.

#### Option B: Python Version
1.  Create a new Lakeflow Pipeline.
2.  Point "Source Code" to the **`Lakeflow_demo/Python/`** folder.
3.  Set Target Schema to `lakeflow_demo`.
4.  Click **Start**.

---

## Project Structure
```text
Lakeflow Pipelines/
├── common/
│   └── 01_Data_Setup.py           # Data Generator (SCD2 Simulation)
└── Lakeflow_demo/
    ├── SQL/                       # Pure SQL Implementation
    │   ├── bronze/                # Ingestion (read_files)
    │   ├── silver/                # SCD Logic (APPLY CHANGES)
    │   └── gold/                  # Star Schema (Materialized Views)
    └── Python/                    # PySpark (Declarative Pipelines) Implementation
        ├── bronze/                # Ingestion (cloudFiles)
        ├── silver/                # SCD Logic (dp.apply_changes)
        └── gold/                  # Star Schema (@dp.table)
```
