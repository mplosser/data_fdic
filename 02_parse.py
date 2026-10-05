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
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date, datetime

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
    "structure": "risview_properties.yaml",
    "history": "history_properties.yaml",
}

STRUCTURE_DIR = RAW_DATA_DIR / "structure"
STRUCTURE_OUT = "structure_quarterly.parquet"
HISTORY_OUT = "history_events.parquet"

# Quarterly structure: YYYYMMDD dates, integer codes/flags; everything else is text.
STRUCTURE_DATES = ["REPDTE", "EFFDATE", "ESTYMD", "ENDEFYMD", "INSDATE"]
STRUCTURE_INTS = [
    "CERT", "RSSDID", "CLCODE", "FED", "FDICSUPV", "INSFDIC", "SPECGRP", "MUTUAL",
    "SUBCHAPS", "SASSER", "CB", "DENOVO", "INSTCRCD", "RSSDHCR", "HCTMULT",
    "IBA", "OI", "FORCHRTR", "OFFFOR", "OFFDOM", "FORMCFR", "FORM31", "UNINUM",
]

# Fields a dataset's dictionary rows are limited to (the full financials YAML has ~2,400).
DICTIONARY_FIELDS = {}


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


def _ymd_dates(values: pd.Series) -> pd.Series:
    """YYYYMMDD (or ISO yyyy-mm-dd...) text -> datetime.date; 9999-12-31 ("open") is kept."""
    text = values.astype("string").str.replace("-", "", regex=False).str[:8]
    bad = text.notna() & ~text.str.fullmatch(r"\d{8}").fillna(False)
    if bad.any():
        raise ValueError(f"{values.name}: unparseable dates, e.g. {values[bad].iloc[0]!r}")
    return text.map(lambda v: None if pd.isna(v) else date(int(v[:4]), int(v[4:6]), int(v[6:])),
                    na_action="ignore")


def _strict_ints(values: pd.Series) -> pd.Series:
    """Integer codes; fails on any non-numeric text rather than blanking it."""
    numbers = pd.to_numeric(values, errors="raise")
    if (numbers.dropna() % 1 != 0).any():
        raise ValueError(f"{values.name}: non-integer values")
    return numbers.astype("Int64")


def write_with_metadata(frame: pd.DataFrame, filepath: Path, var_defs: dict) -> None:
    """Write a DataFrame to parquet, attaching each field's YAML title/description."""
    table = pa.Table.from_pandas(frame, preserve_index=False)
    fields = []
    for field in table.schema:
        info = var_defs.get(field.name, {})
        meta = {k.encode(): str(info[k]).encode("utf-8") for k in ("title", "description")
                if info.get(k)}
        fields.append(field.with_metadata(meta or None))
    pq.write_table(table.cast(pa.schema(fields, metadata=table.schema.metadata)), filepath)
    print(f"  Saved: {filepath}")
    print(f"  Records: {len(frame):,}, Fields: {len(fields)}")


def parse_structure(force: bool = False) -> None:
    """Quarterly structure panel (one row per institution per quarter, as of that quarter)."""
    print("\nParsing quarterly structure data...")
    out = PROCESSED_DATA_DIR / STRUCTURE_OUT
    if not force and out.exists():
        print("  Output already exists. Use --force to overwrite.")
        return
    files = sorted(STRUCTURE_DIR.glob("structure_*.csv.gz"))
    if not files:
        print("  No structure data found in data/raw/structure/")
        return
    print(f"  Reading {len(files)} quarter files")
    frame = pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
                       for f in files], ignore_index=True)
    for col in STRUCTURE_DATES:
        frame[col] = _ymd_dates(frame[col])
    for col in STRUCTURE_INTS:
        frame[col] = _strict_ints(frame[col])
    # ZIP arrives as a JSON number: "4401" or "4401.0" -> "04401"
    frame["ZIP"] = frame["ZIP"].str.replace(r"\.0$", "", regex=True).str.zfill(5)
    frame = frame.sort_values(["REPDTE", "CERT"]).reset_index(drop=True)

    if frame.duplicated(["CERT", "REPDTE"]).any():
        raise ValueError("structure: duplicate (CERT, REPDTE) rows")
    keyed = frame[frame["RSSDID"].fillna(0) > 0]
    dup_rssd = keyed.duplicated(["RSSDID", "REPDTE"], keep=False)
    print(f"  Quarters: {frame['REPDTE'].min()} to {frame['REPDTE'].max()} "
          f"({frame['REPDTE'].nunique()})")
    print(f"  Rows without an RSSD ID: {len(frame) - len(keyed):,}; "
          f"(RSSDID, REPDTE) shared by more than one cert: {int(dup_rssd.sum()):,}")
    DICTIONARY_FIELDS["structure"] = set(frame.columns)
    write_with_metadata(frame, out, load_variable_definitions(RAW_DATA_DIR / YAML_FILES["structure"]))


def parse_history(force: bool = False) -> None:
    """Institution-level structure-change events (EFFDATE, before/after attributes)."""
    print("\nParsing structure-change history...")
    out = PROCESSED_DATA_DIR / HISTORY_OUT
    if not force and out.exists():
        print("  Output already exists. Use --force to overwrite.")
        return
    latest = get_latest_file("history_*.csv.gz")
    if not latest:
        print("  No history data found in data/raw/")
        return
    print(f"  Reading: {latest}")
    frame = pd.read_csv(latest, dtype=str, keep_default_na=False, na_values=[""])
    var_defs = load_variable_definitions(RAW_DATA_DIR / YAML_FILES["history"])
    for col in frame.columns:
        kind = var_defs.get(col, {}).get("type")
        if col.endswith("DATE") or col == "FI_EFFDATE":
            frame[col] = _ymd_dates(frame[col])
        elif kind in ("number", "integer"):
            numbers = pd.to_numeric(frame[col], errors="raise")
            frame[col] = numbers.astype("Int64") if (numbers.dropna() % 1 == 0).all() else numbers
    frame = frame.sort_values(["CERT", "EFFDATE", "ID"], key=None).reset_index(drop=True)
    if frame["ID"].duplicated().any():
        raise ValueError("history: duplicate event IDs")
    print(f"  Events: {len(frame):,}; institutions (CERT): {frame['CERT'].nunique():,}")
    DICTIONARY_FIELDS["history"] = set(frame.columns)
    write_with_metadata(frame, out, var_defs)


def create_data_dictionary() -> None:
    """Create data_dictionary.csv from YAML definition files."""
    print("\nCreating data dictionary...")

    rows = []

    for dataset, yaml_file in YAML_FILES.items():
        yaml_path = RAW_DATA_DIR / yaml_file
        var_defs = load_variable_definitions(yaml_path)

        keep = DICTIONARY_FIELDS.get(dataset)
        for field_name, field_info in sorted(var_defs.items()):
            if keep is not None and field_name not in keep:
                continue
            row = {
                "dataset": dataset,
                "field": field_name,
                "type": field_info.get("type", ""),
                "title": field_info.get("title") or "",
                "description": (field_info.get("description") or "").replace("\n", " ").strip(),
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
    parse_structure(force=args.force)
    parse_history(force=args.force)
    for dataset, name in (("structure", STRUCTURE_OUT), ("history", HISTORY_OUT)):
        if dataset not in DICTIONARY_FIELDS and (PROCESSED_DATA_DIR / name).exists():
            DICTIONARY_FIELDS[dataset] = set(pq.read_schema(PROCESSED_DATA_DIR / name).names)
    create_data_dictionary()

    print("\nParsing complete!")


if __name__ == "__main__":
    main()
