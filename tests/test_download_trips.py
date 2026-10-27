import pytest

from src.download_trips import build_tripdata_url


@pytest.mark.parametrize(
    ("month", "expected_url"),
    [
        (
            "202401",
            "https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip",
        ),
    ],
)
def test_build_tripdata_url_returns_expected_url(month, expected_url):
    assert build_tripdata_url(month) == expected_url
