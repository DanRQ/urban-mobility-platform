from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="urban_mobility_healthcheck",
    description="Validates that the Urban Mobility Airflow environment is working.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["urban-mobility", "healthcheck"],
)
def urban_mobility_healthcheck():
    @task
    def validate_environment() -> None:
        print("Urban Mobility Airflow environment is ready.")

    validate_environment()


urban_mobility_healthcheck()