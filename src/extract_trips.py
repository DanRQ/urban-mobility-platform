from __future__ import annotations

import argparse
import logging
from pathlib import Path
from zipfile import ZipFile

logger = logging.getLogger(__name__)


def extract_tripdata(
    zip_path: str | Path,
    output_dir: str | Path = "/opt/airflow/data/extracted",
) -> list[Path]:
    """
    Extract Citi Bike CSV files from a ZIP archive.

    Returns a list with the extracted CSV paths.
    """
    source = Path(zip_path)
    destination = Path(output_dir)

    if not source.exists():
        raise FileNotFoundError(f"ZIP file not found: {source}")

    destination.mkdir(parents=True, exist_ok=True)

    with ZipFile(source, "r") as archive:
        csv_members = [
            member for member in archive.namelist() if member.lower().endswith(".csv")
        ]

        if not csv_members:
            raise ValueError(f"No CSV files found inside: {source.name}")

        archive.extractall(path=destination, members=csv_members)

    extracted_files = [destination / member for member in csv_members]

    logger.info(
        "Extracted %s CSV file(s):",
        len(extracted_files),
    )
    for file_path in extracted_files:
        logger.info(
            "- %s",
            file_path,
        )

    return extracted_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Citi Bike CSV files from a ZIP archive."
    )
    parser.add_argument(
        "--zip-path",
        required=True,
        help="Path to the Citi Bike ZIP file.",
    )
    parser.add_argument(
        "--output-dir",
        default="/opt/airflow/data/extracted",
        help="Directory where the CSV files will be extracted.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    extracted_files = extract_tripdata(
        zip_path=args.zip_path,
        output_dir=args.output_dir,
    )

    logger.info("Extraction Completed.")
    for file_path in extracted_files:
        logger.info(
            "Extracted file: %s",
            file_path,
        )
