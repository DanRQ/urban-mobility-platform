import pandas as pd
import pytest
from pathlib import Path
from datetime import datetime

from src.load_trips import (
    REQUIRED_COLUMNS,
    get_source_month,
    validate_required_columns,
)


@pytest.mark.parametrize(
    ("csv_path", "expected_month"),
    [
        (
            Path("data/extracted/202401/202401-citibike-tripdata_1.csv"),
            datetime(year=int(2024),month=int(1),day=1,).date(),
        ),
        (
            Path("data/extracted/202401/202401-citibike-tripdata_2.csv"),
            datetime(year=int(2024),month=int(1),day=1,).date(),
        ),
        (
            Path("data/extracted/202402/202402-citibike-tripdata_1.csv"),
            datetime(year=int(2024),month=int(2),day=1,).date(),
        ),
        (
            Path("data/extracted/202402/202402-citibike-tripdata_2.csv"),
            datetime(year=int(2024),month=int(2),day=1,).date(),
        ),
        (
            Path("data/extracted/202402/202402-citibike-tripdata_3.csv"),
            datetime(year=int(2024),month=int(2),day=1,).date(),
        ),
    ],
)
def test_get_source_month_extracts_month_from_path(
    csv_path,
    expected_month,
):
    assert get_source_month(csv_path) == expected_month


def test_validate_required_columns_accepts_complete_schema():
    dataframe = pd.DataFrame(columns=sorted(REQUIRED_COLUMNS))

    result = validate_required_columns(dataframe, "data/extracted/202401/202401-citibike-tripdata_1.csv")

    assert result is None


def test_validate_required_columns_rejects_missing_columns():
    dataframe = pd.DataFrame(
        columns=[
            "tripduration",
            "starttime",
            "stoptime",
        ]
    )

    with pytest.raises(ValueError) as error:
        validate_required_columns(dataframe, "invalid_file.csv")

    error_message = str(error.value)

    assert "invalid_file.csv" in error_message
    assert "rideable_type" in error_message
    assert "start_station_name" in error_message
    assert "ride_id" in error_message
    assert "start_station_id" in error_message
    assert "end_station_name" in error_message
    assert "end_station_id" in error_message
    assert "start_lat" in error_message
    assert "start_lng" in error_message
    assert "end_lat" in error_message
    assert "end_lng" in error_message
    assert "member_casual" in error_message
    assert "started_at" in error_message
    assert "ended_at" in error_message
    
    
   
    
    
    