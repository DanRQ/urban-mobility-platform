from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


load_dotenv()

TABLE_NAME = "citibike_trips"
SCHEMA_NAME = "raw"
READ_CHUNK_SIZE = 10_000
WRITE_CHUNK_SIZE = 10_000

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


def normalize_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = (
        dataframe.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return dataframe


def delete_month_data(engine: Engine, month: str) -> None:
    month_start = pd.to_datetime(month, format="%Y%m")
    next_month_start = month_start + pd.offsets.MonthBegin(1)

    delete_sql = text(
        f"""
        DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE started_at >= :month_start
          AND started_at < :next_month_start
        """
    )

    with engine.begin() as connection:
        connection.execute(
            delete_sql,
            {
                "month_start": month_start.to_pydatetime(),
                "next_month_start": next_month_start.to_pydatetime(),
            },
        )

    print(
        f"Existing rows removed for month {month}: "
        f"{month_start.date()} to {next_month_start.date()}"
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
        dataframe = normalize_columns(dataframe)

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

    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_path}")

    csv_files = sorted(input_path.rglob("*.csv"))

    if not csv_files:
        raise ValueError(f"No CSV files found in: {input_path}")

    engine = get_engine()
    ensure_raw_schema(engine)
    delete_month_data(engine, month)

    total_rows = 0

    for csv_file in csv_files:
        print(f"Loading file: {csv_file}")

        total_rows += load_csv_file(
            csv_path=csv_file,
            engine=engine,
        )

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


if __name__ == "__main__":
    args = parse_args()
    load_extracted_files(
        input_dir=args.input_dir,
        month=args.month,
    )