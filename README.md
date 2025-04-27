# My Data Warehouse Project

## Overview
This project demonstrates a configuration-driven PySpark ETL pipeline orchestrated by Apache Airflow.

- **I/O config**: `conf/tables/<table>.yaml`
- **I/O abstraction**: `io_helper.py`
- **Spark jobs**: `spark_jobs/ingest_raw.py`, `spark_jobs/transform_curated.py`
- **Airflow DAG**: `dags/etl_pipeline.py`
- **Tests**: `tests/test_io_helper.py`

## Requirements

- Conda
- Java 8+
- Python 3.9

## Setup

1. Create the Conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate my_dw_env
   ```
2. Initialize Airflow:
   ```bash
   export AIRFLOW_HOME=$(pwd)/airflow_home
   airflow db init
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

3. Configure Spark connection in Airflow:
   ```bash
   airflow connections add spark_default \
     --conn-type spark \
     --conn-host localhost \
     --conn-port 7077 \
     --conn-extra '{"master":"local[*]","deploy_mode":"client"}'
   ```

## Running Locally

1. Start Airflow services:
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```
2. Trigger the DAG:
   ```bash
   airflow dags trigger daily_transactions_etl
   ```

## Testing

Run tests with:
```bash
pytest tests
```
