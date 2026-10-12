from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="urban_mobility_dependencies_check",
    description="Checks Python dependencies required by the Urban Mobility pipeline.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["urban-mobility", "validation"],
)
def urban_mobility_dependencies_check():
    check_dependencies = BashOperator(
        task_id="check_python_dependencies",
        bash_command="""
            set -e

            python --version

            python -c "import pandas; print('pandas:', pandas.__version__)"
            python -c "import sqlalchemy; print('sqlalchemy:', sqlalchemy.__version__)"
            python -c "import psycopg; print('psycopg: available')"
            python -c "import dotenv; print('python-dotenv: available')"
        """,
    )

    check_dependencies


urban_mobility_dependencies_check()