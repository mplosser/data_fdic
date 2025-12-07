"""
Cleanup FDIC data files.

Options for removing raw and/or processed data files.

Usage:
    python 04_cleanup.py --raw          # Remove raw data files
    python 04_cleanup.py --processed    # Remove processed data files
    python 04_cleanup.py --all          # Remove all data files
    python 04_cleanup.py --dry-run      # Show what would be deleted
"""

import argparse
from pathlib import Path

# Directories
PROJECT_ROOT = Path(__file__).parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

# File patterns to clean
RAW_PATTERNS = ["*.json", "*.yaml"]
PROCESSED_PATTERNS = ["*.parquet", "*.json", "*.csv"]


def get_files(directory: Path, patterns: list) -> list:
    """Get all files matching patterns in directory."""
    files = []
    for pattern in patterns:
        files.extend(directory.glob(pattern))
    return sorted(files)


def format_size(size_bytes: int) -> str:
    """Format file size in human readable form."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


def cleanup_files(files: list, dry_run: bool = False) -> tuple:
    """Delete files and return count and total size."""
    total_size = 0
    deleted_count = 0

    for filepath in files:
        size = filepath.stat().st_size
        total_size += size

        if dry_run:
            print(f"  Would delete: {filepath.name} ({format_size(size)})")
        else:
            filepath.unlink()
            print(f"  Deleted: {filepath.name} ({format_size(size)})")
            deleted_count += 1

    return deleted_count if not dry_run else len(files), total_size


def cleanup_raw(dry_run: bool = False) -> None:
    """Clean up raw data files."""
    print("\nRaw data files:")

    files = get_files(RAW_DATA_DIR, RAW_PATTERNS)

    if not files:
        print("  No files to clean")
        return

    count, size = cleanup_files(files, dry_run)
    action = "Would delete" if dry_run else "Deleted"
    print(f"\n  {action} {count} files ({format_size(size)})")


def cleanup_processed(dry_run: bool = False) -> None:
    """Clean up processed data files."""
    print("\nProcessed data files:")

    files = get_files(PROCESSED_DATA_DIR, PROCESSED_PATTERNS)

    if not files:
        print("  No files to clean")
        return

    count, size = cleanup_files(files, dry_run)
    action = "Would delete" if dry_run else "Deleted"
    print(f"\n  {action} {count} files ({format_size(size)})")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Clean up FDIC data files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 04_cleanup.py --raw          Remove raw data files only
  python 04_cleanup.py --processed    Remove processed data files only
  python 04_cleanup.py --all          Remove all data files
  python 04_cleanup.py --all --dry-run  Show what would be deleted
        """
    )
    parser.add_argument(
        "--raw", "-r",
        action="store_true",
        help="Remove raw data files (JSON, YAML)"
    )
    parser.add_argument(
        "--processed", "-p",
        action="store_true",
        help="Remove processed data files (parquet)"
    )
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Remove all data files"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show what would be deleted without actually deleting"
    )
    args = parser.parse_args()

    # Default to showing help if no options provided
    if not (args.raw or args.processed or args.all):
        parser.print_help()
        return

    print("FDIC Data Cleanup")
    print("=" * 40)

    if args.dry_run:
        print("DRY RUN - No files will be deleted")

    if args.all or args.raw:
        cleanup_raw(dry_run=args.dry_run)

    if args.all or args.processed:
        cleanup_processed(dry_run=args.dry_run)

    print("\nCleanup complete!")


if __name__ == "__main__":
    main()
