from __future__ import annotations

import argparse
import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://s3.amazonaws.com/tripdata"


def build_tripdata_url(month: str) -> str:
    return f"{BASE_URL}/{month}-citibike-tripdata.zip"


def download_tripdata(
    month: str,
    output_dir: str = "/opt/airflow/data/raw",
) -> Path:
    url = build_tripdata_url(month)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    destination = output_path / f"{month}-citibike-tripdata.zip"

    logger.info(
        "Downloading trip data for %s from %s to %s",
        month,
        url,
        destination,
    )

    with requests.get(url, stream=True, timeout=240) as response:
        response.raise_for_status()

        with open(destination, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    logger.info(
        "File saved to: %s",
        destination,
    )

    return destination


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Citi Bike trip data ZIP file."
    )
    parser.add_argument(
        "--month",
        required=True,
        help="Reference month in YYYYMM format, for example 201402.",
    )
    parser.add_argument(
        "--output-dir",
        default="/opt/airflow/data/raw",
        help="Directory where the downloaded ZIP file will be stored.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    downloaded_file = download_tripdata(
        month=args.month,
        output_dir=args.output_dir,
    )

    logger.info(
        "Downloaded file: %s",
        downloaded_file,
    )
