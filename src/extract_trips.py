from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

DEFAULT_MONTH = "202401"

def extract_tripdata(
    zip_path: str | Path,
    output_dir: str | Path = "data/extracted",
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


if __name__ == "__main__":
    extract_tripdata(
        zip_path=f"data/raw/{DEFAULT_MONTH}-citibike-tripdata.zip",
    )