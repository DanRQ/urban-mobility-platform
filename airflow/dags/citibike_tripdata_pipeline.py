from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


RAW_DIR = "/opt/airflow/data/raw"
EXTRACTED_BASE_DIR = "/opt/airflow/data/extracted"
DBT_PROJECT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="citibike_tripdata_pipeline",
    description="Monthly Citi Bike trip data pipeline with backfill support.",
    schedule="@monthly",
    catchup=False,
    default_args = {
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["citibike", "batch", "postgres", "dbt", "backfill"],
)
def citibike_tripdata_pipeline():
    month = "{{ data_interval_start.strftime('%Y%m') }}"

    download_trips = BashOperator(
        task_id="download_trips",
        retries=3,
        retry_delay=timedelta(minutes=3),
        bash_command=(
            "set -euo pipefail; "
            "python /opt/airflow/src/download_trips.py "
            f"--month {month} "
            f"--output-dir {RAW_DIR}"
        ),
    )

    extract_trips = BashOperator(
        task_id="extract_trips",
        bash_command=(
            "set -euo pipefail; "
            "python /opt/airflow/src/extract_trips.py "
            f"--zip-path {RAW_DIR}/{month}-citibike-tripdata.zip "
            f"--output-dir {EXTRACTED_BASE_DIR}/{month}"
        ),
    )

    load_trips = BashOperator(
            task_id="load_trips",
            bash_command=(
                "set -euo pipefail; "
                "python /opt/airflow/src/load_trips.py "
                f"--input-dir {EXTRACTED_BASE_DIR}/{month} "
                f"--month {month}"
            ),
        )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "set -euo pipefail; "
            f"cd {DBT_PROJECT_DIR}; "
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "set -euo pipefail; "
            f"cd {DBT_PROJECT_DIR}; "
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        retries=0
    )

    download_trips >> extract_trips >> load_trips >> dbt_run >> dbt_test


citibike_tripdata_pipeline()