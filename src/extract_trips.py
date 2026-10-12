from __future__ import annotations

import argparse
from pathlib import Path
from zipfile import ZipFile


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
            member
            for member in archive.namelist()
            if member.lower().endswith(".csv")
        ]

        if not csv_members:
            raise ValueError(f"No CSV files found inside: {source.name}")

        archive.extractall(path=destination, members=csv_members)

    extracted_files = [destination / member for member in csv_members]

    print(f"Extracted {len(extracted_files)} CSV file(s):")
    for file_path in extracted_files:
        print(f"- {file_path}")

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

    print("Extraction completed.")
    for file_path in extracted_files:
        print(f"Extracted file: {file_path}")