"""
Download data from FDIC BankFind Suite API.

Endpoints:
- /banks/institutions - Bank structure/demographic data
- /banks/failures - Historical bank failures (1934-present)

API Documentation: https://api.fdic.gov/banks/docs/
"""

import requests
import json
from datetime import datetime
from pathlib import Path

# API Configuration
BASE_URL = "https://api.fdic.gov/banks"
DOCS_URL = "https://api.fdic.gov/banks/docs"
MAX_LIMIT = 10000  # API maximum per request

# Output directories
PROJECT_ROOT = Path(__file__).parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# YAML definition files
YAML_FILES = {
    "failures": "failure_properties.yaml",
    "institutions": "institution_properties.yaml",
}


def fetch_endpoint(endpoint: str, params: dict = None) -> dict:
    """Fetch data from FDIC API endpoint."""
    url = f"{BASE_URL}/{endpoint}"
    default_params = {
        "format": "json",
        "limit": MAX_LIMIT,
        "offset": 0,
    }
    if params:
        default_params.update(params)

    response = requests.get(url, params=default_params)
    response.raise_for_status()
    return response.json()


def fetch_all_records(endpoint: str, params: dict = None) -> list:
    """Fetch all records from an endpoint, handling pagination."""
    all_records = []
    offset = 0
    params = params or {}

    while True:
        params["offset"] = offset
        params["limit"] = MAX_LIMIT

        print(f"  Fetching {endpoint} offset={offset}...")
        data = fetch_endpoint(endpoint, params)

        records = data.get("data", [])
        if not records:
            break

        all_records.extend(records)

        # Check if we've fetched all records
        total = data.get("meta", {}).get("total", 0)
        offset += len(records)

        if offset >= total:
            break

    print(f"  Total records fetched: {len(all_records)}")
    return all_records


def save_json(data: list, filepath: Path) -> None:
    """Save data as JSON file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"  Saved: {filepath}")


def download_yaml_definitions() -> None:
    """Download YAML variable definition files."""
    print("\nDownloading variable definition files...")

    for endpoint, filename in YAML_FILES.items():
        url = f"{DOCS_URL}/{filename}"
        filepath = RAW_DATA_DIR / filename

        print(f"  Fetching {filename}...")
        response = requests.get(url)
        response.raise_for_status()

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"  Saved: {filepath}")


def download_failures() -> None:
    """Download bank failures data."""
    print("\nDownloading bank failures data...")

    records = fetch_all_records("failures")

    timestamp = datetime.now().strftime("%Y%m%d")
    save_json(records, RAW_DATA_DIR / f"failures_{timestamp}.json")


def download_institutions() -> None:
    """Download bank institutions data."""
    print("\nDownloading bank institutions data...")

    records = fetch_all_records("institutions")

    timestamp = datetime.now().strftime("%Y%m%d")
    save_json(records, RAW_DATA_DIR / f"institutions_{timestamp}.json")


def main():
    """Main entry point."""
    print("FDIC Data Download Script")
    print("=" * 40)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    download_yaml_definitions()
    download_failures()
    download_institutions()

    print("\nDownload complete!")


if __name__ == "__main__":
    main()
