from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="urban_mobility_project_check",
    description="Checks that Urban Mobility project files are available in Airflow.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["urban-mobility", "validation"],
)
def urban_mobility_project_check():
    check_project_files = BashOperator(
        task_id="check_project_files",
        bash_command="""
            set -e
            echo "Source files:"
            ls -la /opt/airflow/src
            echo ""
            echo "dbt project:"
            ls -la /opt/airflow/dbt
            echo ""
            echo "Extracted data:"
            ls -la /opt/airflow/data/extracted
        """,
    )

    check_project_files


urban_mobility_project_check()