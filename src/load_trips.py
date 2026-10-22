from __future__ import annotations

import argparse
import os
import re
import pandas as pd

from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


load_dotenv()

TABLE_NAME = "citibike_trips"
SCHEMA_NAME = "raw"
READ_CHUNK_SIZE = 3_000
WRITE_CHUNK_SIZE = 3_000

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

REQUIRED_COLUMNS = {
    "ride_id",
    "rideable_type",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
    "started_at",
    "ended_at",
}

def get_engine() -> Engine:
    user = os.environ["WAREHOUSE_USER"]
    password = os.environ["WAREHOUSE_PASSWORD"]
    host = os.environ["WAREHOUSE_HOST"]
    port = os.environ["WAREHOUSE_PORT"]
    database = os.environ["WAREHOUSE_DB"]

    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_url)


def ensure_raw_schema(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))

def ensure_raw_table(engine: Engine) -> None:
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.citibike_trips (
            ride_id TEXT,
            rideable_type TEXT,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            start_station_name TEXT,
            start_station_id TEXT,
            end_station_name TEXT,
            end_station_id TEXT,
            start_lat DOUBLE PRECISION,
            start_lng DOUBLE PRECISION,
            end_lat DOUBLE PRECISION,
            end_lng DOUBLE PRECISION,
            member_casual TEXT,
            source_month TEXT NOT NULL,
            source_file TEXT NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """
    create_source_month_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_citibike_trips_source_month
        ON raw.citibike_trips (source_month);
    """
    with engine.begin() as connection:
        connection.execute(text(create_table_sql))
        connection.execute(text(create_source_month_index_sql))


def normalize_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = (
        dataframe.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return dataframe

def validate_required_columns(df: pd.DataFrame, source_file: str) -> None:
    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        missing_columns_text = ", ".join(sorted(missing_columns))
        available_columns_text = ", ".join(df.columns)

        raise ValueError(
            f"Invalid File: {source_file}. "
            f"Missing mandatory columns: {missing_columns_text}. "
            f"Columns found: {available_columns_text}."
        )

def delete_month_data(engine: Engine, month: str) -> None:
    source_month = month

    delete_sql = text(
        f"""
        DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE source_month = :source_month
        """
    )

    with engine.begin() as connection:
        connection.execute(
            delete_sql,
            {
                "source_month": source_month,
            },
        )

    print(
        f"Existing rows removed for month {month}: "
    )


def load_csv_file(
    csv_path: str | Path,
    engine: Engine,
) -> int:
    csv_file = Path(csv_path)
    total_rows = 0
    for chunk_number, dataframe in enumerate(
        pd.read_csv(
            csv_file,
            chunksize=READ_CHUNK_SIZE,
            dtype=CSV_DTYPES,
            parse_dates=DATE_COLUMNS,
            date_format="%Y-%m-%d %H:%M:%S.%f",
            na_values=["", "null", "NULL", "NaN"],
            keep_default_na=True,
        ),
        start=1,
    ):
        validate_required_columns(dataframe, csv_path.name)
        dataframe = normalize_columns(dataframe)

        dataframe["source_file"] = csv_path.name
        dataframe["source_month"] = get_source_month(csv_path)
        dataframe = dataframe.drop(columns=["loaded_at"], errors="ignore")

        dataframe.to_sql(
            name=TABLE_NAME,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists="append",
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

    return total_rows


def load_extracted_files(input_dir: str | Path, month: str) -> None:
    input_path = Path(input_dir)
    started_at = datetime.now()

    print(f"Load started at: {started_at:%Y-%m-%d %H:%M:%S}")

    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_path}")

    csv_files = sorted(input_path.rglob("*.csv"))

    if not csv_files:
        raise ValueError(f"No CSV files found in: {input_path}")

    engine = get_engine()
    ensure_raw_schema(engine)
    ensure_raw_table(engine)
    delete_month_data(engine, month)

    total_rows = 0

    for csv_file in csv_files:
        print(f"Loading file: {csv_file}")

        total_rows += load_csv_file(
            csv_path=csv_file,
            engine=engine,
        )

    finished_at = datetime.now()
   
    print(f"Load Finished at: {finished_at:%Y-%m-%d %H:%M:%S}")
    print(
        f"Load completed for {month}: "
        f"{total_rows:,} rows in {SCHEMA_NAME}.{TABLE_NAME}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load extracted Citi Bike CSV files into PostgreSQL."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing extracted CSV files.",
    )
    parser.add_argument(
        "--month",
        required=True,
        help="Reference month in YYYYMM format.",
    )
    return parser.parse_args()

def get_source_month(csv_path: Path) -> str:
    match = re.search(r"(\d{4})[-_]?(\d{2})", csv_path.name)

    if not match:
        raise ValueError(
            f"Could not identify year and month in filename: {csv_path.name}"
        )

    year, month = match.groups()

    return f"{year}{month}"

if __name__ == "__main__":
    args = parse_args()
    load_extracted_files(
        input_dir=args.input_dir,
        month=args.month,
    )