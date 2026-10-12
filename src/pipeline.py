from __future__ import annotations

from src.download_trips import download_tripdata
from src.extract_trips import extract_tripdata
from src.load_trips import load_extracted_files


def run_pipeline(month: str) -> None:
    """
    Download, extract and load Citi Bike trip data for one month.
    """

    zip_path = download_tripdata(month=month)

    extracted_files = extract_tripdata(zip_path=zip_path)

    load_csv_files(extracted_files)


if __name__ == "__main__":
    run_pipeline(month="202401")