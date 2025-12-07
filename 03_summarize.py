"""
Summarize FDIC processed data.

Provides quick overview of processed parquet files including:
- Record counts
- Date ranges
- Variable counts
- Field metadata coverage

Usage:
    python 03_summarize.py
"""

import argparse
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime

# Directories
PROJECT_ROOT = Path(__file__).parent
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"


def get_latest_file(pattern: str) -> Path | None:
    """Get the most recently modified file matching pattern."""
    files = sorted(PROCESSED_DATA_DIR.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def summarize_parquet(filepath: Path) -> dict:
    """Generate summary statistics for a parquet file."""
    table = pq.read_table(filepath)
    schema = table.schema

    # Count fields with metadata
    fields_with_title = sum(1 for f in schema if f.metadata and b"title" in f.metadata)
    fields_with_desc = sum(1 for f in schema if f.metadata and b"description" in f.metadata)

    summary = {
        "file": filepath.name,
        "records": table.num_rows,
        "fields": len(schema),
        "fields_with_title": fields_with_title,
        "fields_with_description": fields_with_desc,
        "file_size_mb": filepath.stat().st_size / (1024 * 1024),
    }

    return summary, table, schema


def summarize_failures() -> None:
    """Summarize bank failures data."""
    print("\n" + "=" * 60)
    print("BANK FAILURES SUMMARY")
    print("=" * 60)

    filepath = get_latest_file("failures_*.parquet")
    if not filepath:
        print("  No failures data found in data/processed/")
        return

    summary, table, schema = summarize_parquet(filepath)

    print(f"\nFile: {summary['file']}")
    print(f"Size: {summary['file_size_mb']:.2f} MB")
    print(f"\nRecords: {summary['records']:,}")
    print(f"Fields: {summary['fields']}")
    print(f"  - with title: {summary['fields_with_title']}")
    print(f"  - with description: {summary['fields_with_description']}")

    # Date range analysis
    df = table.to_pandas()
    if "FAILDATE" in df.columns:
        dates = df["FAILDATE"].dropna()
        if len(dates) > 0:
            min_date = dates.min()
            max_date = dates.max()
            print(f"\nDate Range:")
            print(f"  Earliest failure: {min_date}")
            print(f"  Latest failure: {max_date}")

    if "FAILYR" in df.columns:
        years = df["FAILYR"].dropna()
        if len(years) > 0:
            print(f"\nYear Range: {years.min()} - {years.max()}")

    # Top states by failures
    if "PSTALP" in df.columns:
        state_counts = df["PSTALP"].value_counts().head(5)
        print(f"\nTop 5 States by Failures:")
        for state, count in state_counts.items():
            print(f"  {state}: {count:,}")


def summarize_institutions() -> None:
    """Summarize bank institutions data."""
    print("\n" + "=" * 60)
    print("BANK INSTITUTIONS SUMMARY")
    print("=" * 60)

    filepath = get_latest_file("institutions_*.parquet")
    if not filepath:
        print("  No institutions data found in data/processed/")
        return

    summary, table, schema = summarize_parquet(filepath)

    print(f"\nFile: {summary['file']}")
    print(f"Size: {summary['file_size_mb']:.2f} MB")
    print(f"\nRecords: {summary['records']:,}")
    print(f"Fields: {summary['fields']}")
    print(f"  - with title: {summary['fields_with_title']}")
    print(f"  - with description: {summary['fields_with_description']}")

    # Additional analysis
    df = table.to_pandas()

    if "ACTIVE" in df.columns:
        active_counts = df["ACTIVE"].value_counts()
        print(f"\nInstitution Status:")
        for status, count in active_counts.items():
            label = "Active" if str(status) == "1" else "Inactive"
            print(f"  {label}: {count:,}")

    if "STNAME" in df.columns:
        state_counts = df["STNAME"].value_counts().head(5)
        print(f"\nTop 5 States by Institution Count:")
        for state, count in state_counts.items():
            print(f"  {state}: {count:,}")

    if "BKCLASS" in df.columns:
        class_counts = df["BKCLASS"].value_counts()
        print(f"\nInstitution Classes:")
        for bkclass, count in class_counts.items():
            print(f"  {bkclass}: {count:,}")


def list_fields(dataset: str) -> None:
    """List all fields with their metadata."""
    pattern = f"{dataset}_*.parquet"
    filepath = get_latest_file(pattern)

    if not filepath:
        print(f"  No {dataset} data found")
        return

    table = pq.read_table(filepath)
    schema = table.schema

    print(f"\n{'Field':<20} {'Type':<10} {'Title'}")
    print("-" * 70)

    for field in schema:
        title = ""
        if field.metadata and b"title" in field.metadata:
            title = field.metadata[b"title"].decode("utf-8")
        print(f"{field.name:<20} {str(field.type):<10} {title}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Summarize FDIC processed data files."
    )
    parser.add_argument(
        "--fields",
        choices=["failures", "institutions"],
        help="List all fields for a dataset"
    )
    args = parser.parse_args()

    print("FDIC Data Summary")
    print("=" * 60)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.fields:
        list_fields(args.fields)
    else:
        summarize_failures()
        summarize_institutions()

    print("\n" + "=" * 60)
    print("Summary complete!")


if __name__ == "__main__":
    main()
