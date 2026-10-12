from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Date, DateTime, Numeric, Text, text
from sqlalchemy.engine import Engine


load_dotenv()

TABLE_NAME = "citibike_trips"
SCHEMA_NAME = "raw"
READ_CHUNK_SIZE = 10_00
WRITE_CHUNK_SIZE = 10_00


CSV_DTYPES = {
    "ride_id": "string",
    "rideable_type": "string",
    "start_station_name": "string",
    "start_station_id": "string",
    "end_station_name": "string",
    "end_station_id": "string",
    "start_lat": "float64",
    "start_lng": "float64",
    "end_lat": "float64",
    "end_lng": "float64",
    "member_casual": "string",
}

DATE_COLUMNS = ["started_at", "ended_at"]


def get_engine() -> Engine:
    """
    Create a SQLAlchemy engine using the database URL from the environment variable.
    """
    user = os.environ["WAREHOUSE_USER"]
    password = os.environ["WAREHOUSE_PASSWORD"]
    host = os.environ["WAREHOUSE_HOST"]
    port = os.environ["WAREHOUSE_PORT"]
    database = os.environ["WAREHOUSE_DB"]

    connection_url = (f"postgresql://{user}:{password}@{host}:{port}/{database}")

    return create_engine(connection_url)

def ensure_raw_schema(engine: Engine) -> None:
    """
    Create the raw schema when it does not exist.
    """

    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))


def normalize_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to lowercase snake_case.
    """

    dataframe.columns = (
        dataframe.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    return dataframe


def load_csv_file(
    csv_path: str | Path,
    engine: Engine,
    if_exists: str,
) -> int:
    """
    Load one CSV file into raw.citibike_trips in batches.

    Returns the number of rows loaded.
    """

    csv_file = Path(csv_path)
    total_rows = 0

    for chunk_number, dataframe in enumerate(
        pd.read_csv(
            csv_file,
            chunksize=READ_CHUNK_SIZE,
            dtype= CSV_DTYPES,
            parse_dates= DATE_COLUMNS,
            date_format="%Y-%m-%d %H:%M:%S.%f",
            na_values=["", "null", "NULL", "NaN"],
            keep_default_na=True,
        ),

        start=1,
    ):
        dataframe.to_sql(
            name=TABLE_NAME,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists=if_exists,
            index=False,
            chunksize=WRITE_CHUNK_SIZE,
            method="multi",
        )

        loaded_rows = len(dataframe)
        total_rows += loaded_rows

        print(
            f"{csv_file.name} | "
            f"chunk {chunk_number} | "
            f"{loaded_rows:,} rows loaded"
        )

        if_exists = "append"

    return total_rows

def load_extracted_files(input_dir: str | Path = "data/extracted") -> None:
    """
    Load an explicit list of extracted Citi Bike CSV files into PostgreSQL.
    """

    if not csv_files:
        raise ValueError("No CSV files were provided for loading.")

    engine = get_engine()
    ensure_raw_schema(engine)

    total_rows = 0

    for index, csv_file in enumerate(csv_files):
        mode = "replace" if index == 0 else "append"

        print(f"Loading file: {csv_file.name}")

        total_rows += load_csv_file(
            csv_path=csv_file,
            engine=engine,
            if_exists=mode,
        )

    print(f"Load completed: {total_rows:,} rows in {SCHEMA_NAME}.{TABLE_NAME}")


if __name__ == "__main__":
    extracted_files = sorted(Path("data/extracted").glob("*.csv"))
    load_csv_files(extracted_files)