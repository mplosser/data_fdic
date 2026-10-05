"""
Download data from FDIC BankFind Suite API.

Endpoints:
- /banks/institutions - Bank structure/demographic data (current record only)
- /banks/failures - Historical bank failures (1934-present)
- /banks/financials - structure fields AS OF each quarter (1984Q1-present): name, class,
  charter agent, regulator, top holder, trust powers, ... One file per quarter.
- /banks/history - dated structure-change events for institutions (EFFDATE, before/after
  class, name, regulator, ...)

API Documentation: https://api.fdic.gov/banks/docs/
Register for API key at: https://api.fdic.gov/banks/docs/
"""

import argparse
import os
import time
import requests
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

# API Configuration
BASE_URL = "https://api.fdic.gov/banks"
DOCS_URL = "https://api.fdic.gov/banks/docs"
MAX_LIMIT = 10000  # API maximum per request
DEFAULT_DELAY = 0.5  # Seconds between requests

# Output directories
PROJECT_ROOT = Path(__file__).parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

STRUCTURE_DIR = RAW_DATA_DIR / "structure"

# YAML definition files
YAML_FILES = {
    "failures": "failure_properties.yaml",
    "institutions": "institution_properties.yaml",
    "structure": "risview_properties.yaml",
    "history": "history_properties.yaml",
}

# Structure fields taken from /financials, each reported AS OF the quarter (REPDTE), so a
# bank's name, class, regulator and top holder are the ones in force that quarter. ESTYMD,
# ENDEFYMD and INSDATE are institution-level dates (the same in every quarter); EFFDATE is
# the date of the latest structure change in force at REPDTE.
STRUCTURE_FIELDS = [
    "CERT", "RSSDID", "REPDTE", "NAME", "CITY", "STALP", "ZIP",
    "BKCLASS", "CLCODE", "CHRTAGNT", "REGAGNT", "FED", "FDICSUPV", "INSAGNT1", "INSFDIC",
    "TRUST", "SPECGRP", "MUTUAL", "SUBCHAPS", "SASSER", "CB", "DENOVO", "INSTCRCD",
    "RSSDHCR", "NAMEHCR", "CITYHCR", "STALPHCR", "HCTMULT",
    "IBA", "OI", "FORCHRTR", "OFFFOR", "OFFDOM",
    "INSTTYPE", "FORMCFR", "FORM31", "UNINUM",
    "EFFDATE", "ESTYMD", "ENDEFYMD", "INSDATE",
]
STRUCTURE_START = "1984-03-31"   # first quarter /financials covers
REFRESH_QUARTERS = 4             # re-fetch the latest quarters every run (late amendments)
HISTORY_FILTER = "ORG_ROLE_CDE:FI"   # institution-level events (branch events excluded)
HISTORY_FIRST_YEAR = 1934


def fetch_endpoint(endpoint: str, params: dict = None, api_key: str = None) -> dict:
    """Fetch data from FDIC API endpoint."""
    url = f"{BASE_URL}/{endpoint}"
    default_params = {
        "format": "json",
        "limit": MAX_LIMIT,
        "offset": 0,
    }
    if params:
        default_params.update(params)
    if api_key:
        default_params["api_key"] = api_key

    response = requests.get(url, params=default_params)
    response.raise_for_status()
    return response.json()


def fetch_all_records(endpoint: str, params: dict = None, api_key: str = None) -> list:
    """Fetch all records from an endpoint, handling pagination."""
    all_records = []
    offset = 0
    params = params or {}

    while True:
        params["offset"] = offset
        params["limit"] = MAX_LIMIT

        print(f"  Fetching {endpoint} offset={offset}...")
        data = fetch_endpoint(endpoint, params, api_key=api_key)

        records = data.get("data", [])
        if not records:
            break

        all_records.extend(records)

        # Check if we've fetched all records
        total = data.get("meta", {}).get("total", 0)
        offset += len(records)

        if offset >= total:
            break
        time.sleep(DEFAULT_DELAY)

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


def download_failures(api_key: str = None) -> None:
    """Download bank failures data."""
    print("\nDownloading bank failures data...")

    records = fetch_all_records("failures", api_key=api_key)

    timestamp = datetime.now().strftime("%Y%m%d")
    save_json(records, RAW_DATA_DIR / f"failures_{timestamp}.json")


def download_institutions(api_key: str = None) -> None:
    """Download bank institutions data."""
    print("\nDownloading bank institutions data...")

    records = fetch_all_records("institutions", api_key=api_key)

    timestamp = datetime.now().strftime("%Y%m%d")
    save_json(records, RAW_DATA_DIR / f"institutions_{timestamp}.json")


def _flat(records: list) -> pd.DataFrame:
    return pd.DataFrame([r.get("data", r) for r in records])


def latest_repdte(api_key: str = None) -> pd.Timestamp:
    """Latest quarter /financials has published."""
    data = fetch_endpoint("financials", {"fields": "REPDTE", "sort_by": "REPDTE",
                                         "sort_order": "DESC", "limit": 1}, api_key=api_key)
    return pd.Timestamp(data["data"][0]["data"]["REPDTE"])


def download_structure(api_key: str = None, force: bool = False) -> None:
    """Quarterly structure panel from /financials: one gzipped CSV per quarter.

    Quarters already on disk are skipped, except the latest REFRESH_QUARTERS (re-fetched
    every run so late amendments land) or everything with --force.
    """
    print("\nDownloading quarterly structure (financials endpoint)...")
    STRUCTURE_DIR.mkdir(parents=True, exist_ok=True)
    latest = latest_repdte(api_key)
    quarters = pd.date_range(STRUCTURE_START, latest, freq="QE")
    refresh_from = quarters[max(len(quarters) - REFRESH_QUARTERS, 0)]
    print(f"  {quarters[0]:%Y-%m-%d} to {latest:%Y-%m-%d} ({len(quarters)} quarters)")
    for q in quarters:
        out = STRUCTURE_DIR / f"structure_{q:%Y%m%d}.csv.gz"
        if out.exists() and not force and q < refresh_from:
            continue
        params = {"filters": f"REPDTE:{q:%Y%m%d}", "fields": ",".join(STRUCTURE_FIELDS),
                  "sort_by": "CERT", "sort_order": "ASC"}
        records = fetch_all_records("financials", params, api_key=api_key)
        # convert_dtypes keeps integer codes integer where the JSON page had blanks
        frame = _flat(records).reindex(columns=STRUCTURE_FIELDS).convert_dtypes()
        if frame["CERT"].duplicated().any():
            raise RuntimeError(f"{q:%Y-%m-%d}: duplicate CERT rows in the download")
        frame.to_csv(out, index=False)
        print(f"  Saved: {out.name} ({len(frame):,} institutions)")
        time.sleep(DEFAULT_DELAY)


def download_history(api_key: str = None) -> None:
    """Institution-level structure-change events from /history, fetched year by year."""
    print("\nDownloading structure-change history (history endpoint)...")
    chunks = [f"EFFDATE:[* TO {HISTORY_FIRST_YEAR - 1}-12-31]"]
    chunks += [f"EFFDATE:[{y}-01-01 TO {y}-12-31]"
               for y in range(HISTORY_FIRST_YEAR, datetime.now().year + 1)]
    frames = []
    for chunk in chunks:
        params = {"filters": f"{HISTORY_FILTER} AND {chunk}", "sort_by": "EFFDATE",
                  "sort_order": "ASC"}
        records = fetch_all_records("history", params, api_key=api_key)
        if records:
            frames.append(_flat(records))
    history = pd.concat(frames, ignore_index=True)
    total = fetch_endpoint("history", {"filters": HISTORY_FILTER, "limit": 1},
                           api_key=api_key)["meta"]["total"]
    if history["ID"].nunique() != len(history) or len(history) != total:
        raise RuntimeError(f"history download incomplete: {len(history):,} rows, "
                           f"{history['ID'].nunique():,} unique, API total {total:,}")
    timestamp = datetime.now().strftime("%Y%m%d")
    out = RAW_DATA_DIR / f"history_{timestamp}.csv.gz"
    history.to_csv(out, index=False)
    print(f"  Saved: {out} ({len(history):,} events)")


DATASETS = ("failures", "institutions", "structure", "history")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Download data from FDIC BankFind Suite API"
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="FDIC API key (or set FDIC_API_KEY environment variable)",
    )
    parser.add_argument(
        "--only", nargs="+", choices=DATASETS, default=list(DATASETS),
        help="Datasets to download (default: all)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Structure: re-download every quarter, not just new and recent ones",
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Get API key from args or environment
    api_key = args.api_key or os.environ.get("FDIC_API_KEY")

    print("FDIC Data Download Script")
    print("=" * 40)

    if api_key:
        print("Using API key: ****" + api_key[-4:])
    else:
        print("Warning: No API key provided. Requests may be rate-limited.")
        print("  Set FDIC_API_KEY environment variable or use --api-key")

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    download_yaml_definitions()
    if "failures" in args.only:
        download_failures(api_key=api_key)
    if "institutions" in args.only:
        download_institutions(api_key=api_key)
    if "structure" in args.only:
        download_structure(api_key=api_key, force=args.force)
    if "history" in args.only:
        download_history(api_key=api_key)

    print("\nDownload complete!")


if __name__ == "__main__":
    main()
