# FDIC Data Pipeline

Automated pipeline for downloading, processing, and summarizing bank data from the [FDIC BankFind Suite API](https://api.fdic.gov/banks/docs/).

## Data Sources

| Endpoint | Description | Data Range |
|----------|-------------|------------|
| `/banks/institutions` | Bank structure and demographic data | Current |
| `/banks/failures` | Historical bank failure records | 1934-present |

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
- YAML variable definition files (`failure_properties.yaml`, `institution_properties.yaml`)

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

## Project Structure

```
data_fdic/
├── data/
│   ├── raw/                  # Downloaded JSON and YAML files
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
