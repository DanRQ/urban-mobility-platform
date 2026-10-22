# Urban Mobility Platform

An end-to-end batch data pipeline that ingests historical Citi Bike trip data, loads it into PostgreSQL, transforms it with dbt, and orchestrates the workflow with Apache Airflow.

## Architecture

```text
Citi Bike Trip Data
        |
        v
Python ingestion scripts
(download -> extract -> load)
        |
        v
PostgreSQL
(raw layer)
        |
        v
dbt
(transformations + tests)
        |
        v
Analytics mart
(public_marts.fct_daily_trip_metrics)
```

## Tech Stack

- Python
- Pandas
- PostgreSQL
- Apache Airflow
- dbt
- Docker Compose
- pytest
- WSL / Ubuntu

## Pipeline

The Airflow DAG runs the following tasks in sequence:

```text
download_trips
    >>
extract_trips
    >>
load_trips
    >>
dbt_run
    >>
dbt_test
```

### Ingestion

The pipeline uses three Python scripts:

| Script | Responsibility |
|---|---|
| `download_trips.py` | Builds the Citi Bike URL and downloads the monthly ZIP archive. |
| `extract_trips.py` | Extracts CSV files from the archive. |
| `load_trips.py` | Validates CSV schemas and loads records into PostgreSQL. |

The raw table stores ingestion metadata for traceability:

```text
source_month
source_file
loaded_at
```

## Data Modeling

Raw trip data is stored in the `raw` schema.

dbt transforms the raw data into analytical models in the `public_marts` schema.

Main model:

```text
public_marts.fct_daily_trip_metrics
```

This model provides daily trip metrics for analytics and reporting.

## Data Quality

The project includes data-quality checks at multiple stages:

- URL construction tests for the downloader.
- Source month extraction tests from file paths.
- Required-column validation before database loading.
- dbt tests for analytical models.

Run unit tests:

```bash
python -m pytest -q
```

Run dbt models and tests:

```bash
cd dbt
dbt build
```

## Airflow Backfill

The DAG runs on a monthly schedule and supports historical processing through Airflow's native backfill feature.

Each DAG run derives the processing month from:

```python
month = "{{ data_interval_start.strftime('%Y%m') }}"
```

For example, a backfill from `2024-12-01` to `2026-12-01` creates monthly runs from `202412` through `202612`.

## Local Setup

Create a `.env` file:

```env
WAREHOUSE_USER=warehouse_user
WAREHOUSE_PASSWORD=warehouse_password
WAREHOUSE_DB=warehouse_db
WAREHOUSE_HOST=warehouse-db
WAREHOUSE_PORT=5432
```

Start the local infrastructure:

```bash
docker compose up -d
```

Access the Airflow UI:

```text
http://localhost:8080
```

## Local Execution

```bash
python src/download_trips.py --month 202401 --output-dir data/raw
```

```bash
python src/extract_trips.py \
  --zip-path data/raw/202401-citibike-tripdata.zip \
  --output-dir data/extracted/202401
```

```bash
python src/load_trips.py \
  --input-dir data/extracted/202401 \
  --month 202401
```

## Data Source

[Citi Bike System Data](https://citibikenyc.com/system-data)