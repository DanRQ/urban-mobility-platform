from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="create_raw_layer",
    description="Creates the raw schema and tables in the warehouse.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["urban-mobility", "warehouse", "raw"],
)
def create_raw_layer():
    create_raw_schema = SQLExecuteQueryOperator(
        task_id="create_raw_schema",
        conn_id="urban_mobility_warehouse",
        sql="sql/create_raw_schema.sql",
    )

    create_raw_events_table = SQLExecuteQueryOperator(
        task_id="create_raw_events_table",
        conn_id="urban_mobility_warehouse",
        sql="sql/create_raw_events_table.sql",
    )

    insert_raw_test_event = SQLExecuteQueryOperator(
        task_id="insert_raw_test_event",
        conn_id="urban_mobility_warehouse",
        sql="sql/insert_raw_test_event.sql",
    )

    create_raw_schema >> create_raw_events_table >> insert_raw_test_event


create_raw_layer()