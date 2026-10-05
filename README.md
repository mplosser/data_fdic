# FDIC Data Pipeline

Automated pipeline for downloading, processing, and summarizing bank data from the [FDIC BankFind Suite API](https://api.fdic.gov/banks/docs/).

## Data Sources

| Endpoint | Description | Data Range |
|----------|-------------|------------|
| `/banks/institutions` | Bank structure and demographic data | Current record only |
| `/banks/failures` | Historical bank failure records | 1934-present |
| `/banks/financials` (structure fields) | Structure **as of each quarter**: name, class, regulator, top holder, ... | 1984Q1-present |
| `/banks/history` | Dated structure-change events (institution level) | 1800s-present |

## Quick Start

### Installation

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (macOS/Linux)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### API Key Setup (Recommended)

Register for a free API key at https://api.fdic.gov/banks/docs/ to avoid rate limiting.

```bash
# Set environment variable (recommended)
set FDIC_API_KEY=your_api_key_here      # Windows CMD
$env:FDIC_API_KEY="your_api_key_here"   # Windows PowerShell
export FDIC_API_KEY=your_api_key_here   # macOS/Linux

# Or pass directly to script
python 01_download.py --api-key your_api_key_here
```

### Download Data

```bash
python 01_download.py
```

Downloads:
- Bank failures and institutions data as JSON
- Quarterly structure (one gzipped CSV per quarter in `data/raw/structure/`) and
  structure-change history (gzipped CSV)
- YAML variable definition files for each endpoint

`--only failures institutions structure history` picks datasets; `--force` re-fetches
every structure quarter.

### Parse Data

```bash
python 02_parse.py           # Skip if output exists
python 02_parse.py --force   # Overwrite existing output
```

Outputs:
- Parquet files to `data/processed/` with embedded field metadata
- `data/data_dictionary.csv` with all variable definitions

### Summarize Data

```bash
python 03_summarize.py                    # Show summary of all datasets
python 03_summarize.py --fields failures  # List all fields in failures dataset
```

Displays summary statistics: record counts, date ranges, field coverage, top states, etc.

### Cleanup

```bash
python 04_cleanup.py --raw        # Remove raw data files
python 04_cleanup.py --processed  # Remove processed data files
python 04_cleanup.py --all        # Remove all data files
python 04_cleanup.py --dry-run    # Preview without deleting
```

## Structure as of each quarter

The `institutions` file holds each bank's **current** record only: a bank renamed or
re-chartered after 2015 shows its new name and class throughout its history. For history,
use the two dated datasets:

- **`structure_quarterly.parquet`** — one row per institution per quarter, 1984Q1 to the
  latest published quarter, taken from the `/financials` endpoint (Statistics on Depository
  Institutions). Every field is the value **in force that quarter**: name, city/state,
  institution class (`BKCLASS`, `CLCODE`), charter agent, primary regulator, Fed district,
  insurer, trust powers, mutual/stock, Subchapter S, asset-concentration group (`SPECGRP`),
  regulatory top holder (`RSSDHCR`, `NAMEHCR`), foreign-bank flags (`IBA`, `OI`,
  `FORCHRTR`), office counts, and the date of the latest structure change in force
  (`EFFDATE`). `ESTYMD`, `ENDEFYMD` and `INSDATE` are institution-level dates, the same in
  every quarter. Keys: `CERT` + `REPDTE`; `RSSDID` is the Federal Reserve ID the Call
  Report files under. It covers every Call Report filer, including uninsured trust
  companies (`BKCLASS` = NC). Example, cert 24045:

  | REPDTE | NAME | BKCLASS | REGAGNT | NAMEHCR |
  |---|---|---|---|---|
  | 1999-06-30 | FIRST PROFESSIONAL BANK NA | N | OCC | PROFESSIONAL BCORP INC |
  | 2021-12-31 | PACIFIC WESTERN BANK | NM | FDIC | PACWEST BCORP |
  | 2024-12-31 | BANC OF CALIFORNIA | SM | FED | BANC OF CALIFORNIA INC |

- **`history_events.parquet`** — institution-level structure-change events from the
  `/history` endpoint (`ORG_ROLE_CDE` = FI; branch events excluded): effective date
  (`EFFDATE`), event (`CHANGECODE`, `CHANGECODE_DESC`), and the attributes before
  (`FRM_*`) and after (class, name, charter agent, regulator, trust powers, ...). Use it to
  date a change exactly; the quarterly panel shows the state at each quarter end.

Downloads are incremental: each quarter is saved once under `data/raw/structure/`, and the
latest four quarters are re-fetched every run so late amendments land
(`--force` re-fetches everything). History is fetched year by year and checked against the
API's event count.

```bash
python 01_download.py --only structure history   # just these two
python 02_parse.py --force
python 03_summarize.py --fields structure
```

## Project Structure

```
data_fdic/
├── data/
│   ├── raw/                  # Downloaded JSON, CSV and YAML files
│   │   └── structure/        # One CSV per quarter (structure as of that quarter)
│   ├── processed/            # Parquet files with embedded metadata
│   └── data_dictionary.csv   # Variable definitions from YAML
├── 01_download.py
├── 02_parse.py
├── 03_summarize.py
├── 04_cleanup.py
├── requirements.txt
└── README.md
```

## Variable Definitions

The FDIC provides YAML files with variable definitions for each endpoint. The parse script embeds these as parquet field metadata:
- `title` - Display name for the field
- `description` - Detailed field description
- `enum` - Valid values (if applicable)
- `unit` - Measurement unit (e.g., "Thousands of US Dollars")

## API Reference

Full documentation: https://api.fdic.gov/banks/docs/
