# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FDIC Data Pipeline - automates downloading, processing, and summarizing data from the FDIC API (https://api.fdic.gov/banks/docs/#/).

**Target API Endpoints:**
- `/banks/institutions` - Bank structure data
- `/banks/failures` - Bank failure data

Each endpoint has an associated .yaml file with variable definitions that should be incorporated into processed data (similar to STATA variable labels).

## Project Structure

```
data/
  raw/        # Downloaded JSON + YAML definition files
  processed/  # Parquet files with embedded field metadata
01_download.py   # Downloads data + YAML variable definitions
02_parse.py      # Outputs parquet with metadata from YAML
03_summarize.py  # Summary statistics for processed data
04_cleanup.py    # Remove raw/processed data files
```

## Commands

```bash
# Setup
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt

# Pipeline
python 01_download.py              # Download data + YAML
python 02_parse.py --force         # Parse to parquet
python 03_summarize.py             # Verify results
python 04_cleanup.py --raw         # Remove raw files (optional)
```

## Reference Implementation

The `data_sod` repository uses the same FDIC API and can serve as a reference for:
- Pipeline organization
- Download scripts
- Processing workflow

Note: `data_sod` does not incorporate .yaml variable descriptions - that enhancement is planned for both projects.

## Development Goals

1. ~~Download data from FDIC API endpoints~~ (done)
2. ~~Parse and incorporate .yaml variable descriptions into processed datasets~~ (done - parquet metadata)
3. ~~Generate summary statistics and reports~~ (done)