"""
Parse and process FDIC data files.

Reads raw data from data/raw/ and outputs parquet files to data/processed/
with variable descriptions incorporated from YAML definition files.

Usage:
    python 02_parse.py          # Skip if output exists
    python 02_parse.py --force  # Overwrite existing output
"""

import argparse
import csv
import json
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime

# Fields that contain dates in M/D/YYYY format
DATE_FIELDS = {"FAILDATE", "RESDATE", "BRDATE", "PTRDATE"}

# Directories
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# YAML definition files
YAML_FILES = {
    "failures": "failure_properties.yaml",
    "institutions": "institution_properties.yaml",
}


def get_latest_file(pattern: str) -> Path | None:
    """Get the most recently modified file matching pattern."""
    files = sorted(RAW_DATA_DIR.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def output_exists(pattern: str) -> bool:
    """Check if output file matching pattern exists."""
    files = list(PROCESSED_DATA_DIR.glob(pattern))
    return len(files) > 0


def load_json(filepath: Path) -> list:
    """Load JSON data file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_variable_definitions(yaml_path: Path) -> dict:
    """
    Load variable definitions from YAML file.

    Returns dict mapping field names to their metadata (title, description, type).
    """
    if not yaml_path.exists():
        print(f"  Warning: {yaml_path} not found")
        return {}

    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data:
        return {}

    # Extract properties from nested structure
    properties = data.get("properties", {}).get("data", {}).get("properties", {})
    return properties


def flatten_records(records: list) -> list:
    """Flatten nested 'data' structure if present."""
    flat_data = []
    for record in records:
        if "data" in record:
            flat_data.append(record["data"])
        else:
            flat_data.append(record)
    return flat_data


def build_schema_with_metadata(records: list, var_defs: dict) -> pa.Schema:
    """Build PyArrow schema with field metadata from YAML definitions."""
    if not records:
        return pa.schema([])

    # Get all unique keys
    all_keys = set()
    for record in records:
        all_keys.update(record.keys())

    fields = []
    for key in sorted(all_keys):
        # Check if this is a known date field
        if key in DATE_FIELDS:
            pa_type = pa.date32()
        else:
            # Determine field type from data
            sample_value = None
            for record in records:
                if key in record and record[key] is not None:
                    sample_value = record[key]
                    break

            if isinstance(sample_value, bool):
                pa_type = pa.bool_()
            elif isinstance(sample_value, int):
                pa_type = pa.int64()
            elif isinstance(sample_value, float):
                pa_type = pa.float64()
            else:
                pa_type = pa.string()

        # Build field metadata from YAML definitions
        metadata = {}
        if key in var_defs:
            var_info = var_defs[key]
            if "title" in var_info:
                metadata[b"title"] = var_info["title"].encode("utf-8")
            if "description" in var_info:
                metadata[b"description"] = var_info["description"].encode("utf-8")
            if "enum" in var_info:
                metadata[b"enum"] = json.dumps(var_info["enum"]).encode("utf-8")
            if "x-number-unit" in var_info:
                metadata[b"unit"] = var_info["x-number-unit"].encode("utf-8")

        field = pa.field(key, pa_type, metadata=metadata if metadata else None)
        fields.append(field)

    return pa.schema(fields)


def parse_date(value):
    """Parse date string in M/D/YYYY format to date object."""
    if value is None or value == "":
        return None
    try:
        # Handle M/D/YYYY format (variable width month/day)
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        try:
            # Try alternative formats
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None


def coerce_value(value, pa_type):
    """Coerce a value to match the expected PyArrow type."""
    if value is None or value == "":
        return None

    if pa.types.is_date32(pa_type):
        return parse_date(value)
    elif pa.types.is_string(pa_type):
        return str(value)
    elif pa.types.is_int64(pa_type):
        if isinstance(value, (int, float)):
            return int(value)
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    elif pa.types.is_float64(pa_type):
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    elif pa.types.is_boolean(pa_type):
        return bool(value)

    return value


def save_parquet(records: list, filepath: Path, var_defs: dict) -> None:
    """Save data as parquet file with metadata."""
    if not records:
        print(f"  No data to save for {filepath}")
        return

    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Build schema with metadata
    schema = build_schema_with_metadata(records, var_defs)

    # Convert records to columnar format with type coercion
    columns = {field.name: [] for field in schema}
    for record in records:
        for field in schema:
            raw_value = record.get(field.name)
            coerced_value = coerce_value(raw_value, field.type)
            columns[field.name].append(coerced_value)

    # Create table and write parquet
    table = pa.table(columns, schema=schema)
    pq.write_table(table, filepath)

    print(f"  Saved: {filepath}")
    print(f"  Records: {len(records)}, Fields: {len(schema)}")


def parse_failures(force: bool = False) -> None:
    """Parse bank failures data."""
    print("\nParsing bank failures data...")

    # Check for existing output
    if not force and output_exists("failures_*.parquet"):
        print("  Output already exists. Use --force to overwrite.")
        return

    latest_file = get_latest_file("failures_*.json")
    if not latest_file:
        print("  No failures data found in data/raw/")
        return

    print(f"  Reading: {latest_file}")
    records = load_json(latest_file)
    flat_data = flatten_records(records)

    # Load variable definitions
    yaml_path = RAW_DATA_DIR / YAML_FILES["failures"]
    var_defs = load_variable_definitions(yaml_path)
    print(f"  Variable definitions loaded: {len(var_defs)} fields")

    timestamp = datetime.now().strftime("%Y%m%d")
    save_parquet(flat_data, PROCESSED_DATA_DIR / f"failures_{timestamp}.parquet", var_defs)


def parse_institutions(force: bool = False) -> None:
    """Parse bank institutions data."""
    print("\nParsing bank institutions data...")

    # Check for existing output
    if not force and output_exists("institutions_*.parquet"):
        print("  Output already exists. Use --force to overwrite.")
        return

    latest_file = get_latest_file("institutions_*.json")
    if not latest_file:
        print("  No institutions data found in data/raw/")
        return

    print(f"  Reading: {latest_file}")
    records = load_json(latest_file)
    flat_data = flatten_records(records)

    # Load variable definitions
    yaml_path = RAW_DATA_DIR / YAML_FILES["institutions"]
    var_defs = load_variable_definitions(yaml_path)
    print(f"  Variable definitions loaded: {len(var_defs)} fields")

    timestamp = datetime.now().strftime("%Y%m%d")
    save_parquet(flat_data, PROCESSED_DATA_DIR / f"institutions_{timestamp}.parquet", var_defs)


def create_data_dictionary() -> None:
    """Create data_dictionary.csv from YAML definition files."""
    print("\nCreating data dictionary...")

    rows = []

    for dataset, yaml_file in YAML_FILES.items():
        yaml_path = RAW_DATA_DIR / yaml_file
        var_defs = load_variable_definitions(yaml_path)

        for field_name, field_info in sorted(var_defs.items()):
            row = {
                "dataset": dataset,
                "field": field_name,
                "type": field_info.get("type", ""),
                "title": field_info.get("title", ""),
                "description": field_info.get("description", "").replace("\n", " ").strip(),
                "enum": "|".join(field_info.get("enum", [])) if "enum" in field_info else "",
                "unit": field_info.get("x-number-unit", ""),
            }
            rows.append(row)

    if not rows:
        print("  No variable definitions found")
        return

    # Write CSV
    filepath = DATA_DIR / "data_dictionary.csv"
    fieldnames = ["dataset", "field", "type", "title", "description", "enum", "unit"]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Saved: {filepath}")
    print(f"  Variables: {len(rows)}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse FDIC data and output parquet files with metadata."
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Overwrite existing output files"
    )
    args = parser.parse_args()

    print("FDIC Data Parse Script")
    print("=" * 40)

    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    parse_failures(force=args.force)
    parse_institutions(force=args.force)
    create_data_dictionary()

    print("\nParsing complete!")


if __name__ == "__main__":
    main()
