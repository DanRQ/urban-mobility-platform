from __future__ import annotations

from pathlib import Path

import requests

BASE_URL = "https://s3.amazonaws.com/tripdata"
DEFAULT_MONTH="202401"

def build_tripdata_url(month: str) -> str:
    return f"{BASE_URL}/{month}-citibike-tripdata.zip"

def download_tripdata(month: str = DEFAULT_MONTH, output_dir: str="data/raw",) -> Path:
    url = build_tripdata_url(month)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    destination = output_path / f"{month}-citibike-tripdata.zip"

    print(f"Downloading trip data for {month} from {url} to {destination}")
    
    with requests.get(url, stream=True, timeout=240) as response:
        response.raise_for_status()

        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1 MB chunks
                f.write(chunk)

    print(f"File saved to: {destination}")

    return destination

if __name__ == "__main__":
    download_tripdata(month=DEFAULT_MONTH)
