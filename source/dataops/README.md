# DataOps

This directory contains the **data engineering and orchestration layer** of the NYC Taxi project. It is responsible for extracting raw data, validating inputs and outputs, transforming data into analysis-ready tables, loading curated datasets into the warehouse, and orchestrating the full pipeline through Airflow.

The DataOps layer ensures that monthly taxi and weather data move through a reproducible ETL workflow before they are used downstream for reporting, drift monitoring, and model training.

---

## Responsibilities

The `dataops` module handles the following core tasks:

- **Data extraction**  
  Downloads or reads raw taxi trip data and weather data for a given year-month period.

- **Validation checks**  
  Confirms that required raw files exist before processing and that processed outputs are complete, non-empty, and schema-compliant after transformation.

- **Transformation**  
  Cleans and standardises raw inputs into curated datasets such as trip facts, weather dimensions, and zone lookup tables.

- **Warehouse loading**  
  Loads processed outputs into PostgreSQL tables used by the backend, monitoring workflows, and model pipelines.

- **Pipeline orchestration**  
  Provides Airflow DAG logic and task wrappers so the ETL process can run automatically on a schedule or be backfilled for historical months.

- **Monitoring support**  
  Exposes lightweight checks and wrappers for tracking task health and validating pipeline execution.

---

## Directory Structure

The structure of this folder is as follows:

```text
dataops/
├── dag.py
├── Dockerfile
├── extract.py
├── load.py
├── README.md
├── report.py
├── requirements.txt
├── transform.py
├── validate.py
└── verify_watermark.py