from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="urban_mobility_warehouse_check",
    description="Checks connectivity from Airflow to the Urban Mobility warehouse.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["urban-mobility", "validation"],
)
def urban_mobility_warehouse_check():
    check_connection = BashOperator(
        task_id="check_warehouse_connection",
        bash_command="""
            set -e

            python - <<'PYTHON'
import os

from sqlalchemy import create_engine, text

connection_url = (
    "postgresql+psycopg://"
    f"{os.environ['WAREHOUSE_USER']}:"
    f"{os.environ['WAREHOUSE_PASSWORD']}@"
    f"{os.environ['WAREHOUSE_HOST']}:"
    f"{os.environ['WAREHOUSE_PORT']}/"
    f"{os.environ['WAREHOUSE_DB']}"
)

engine = create_engine(connection_url)

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1")).scalar()

print(f"Warehouse connection result: {result}")
PYTHON
        """,
        env={
            "WAREHOUSE_HOST": "warehouse-db",
            "WAREHOUSE_PORT": "5432",
            "WAREHOUSE_DB": "warehouse_db",
            "WAREHOUSE_USER": "warehouse_user",
            "WAREHOUSE_PASSWORD": "warehouse_password",
        },
        append_env=True,
    )

    check_connection


urban_mobility_warehouse_check()