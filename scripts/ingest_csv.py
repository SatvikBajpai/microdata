#!/usr/bin/env python3
"""Ingest ASI CSV files into a DuckDB database.

Usage:
    python scripts/ingest_csv.py <csv_dir> <output_db>

Example:
    python scripts/ingest_csv.py extracted_csv/ASI_DATA_2023_24_CSV data/asi_2023_24.duckdb
"""

import sys
from pathlib import Path

import duckdb

# Map CSV filename patterns to DuckDB table names
TABLE_MAP = {
    "blkA": "block_a",
    "blkB": "block_b",
    "blkC": "block_c",
    "blkD": "block_d",
    "blkE": "block_e",
    "blkF": "block_f",
    "blkG": "block_g",
    "blkH": "block_h",
    "blkI": "block_i",
    "blkJ": "block_j",
}


def ingest(csv_dir: str, db_path: str):
    csv_dir = Path(csv_dir)
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    for csv_file in sorted(csv_dir.glob("*.csv")):
        table_name = None
        for prefix, name in TABLE_MAP.items():
            if csv_file.name.startswith(prefix):
                table_name = name
                break
        if not table_name:
            print(f"Skipping unrecognized file: {csv_file.name}")
            continue

        print(f"Loading {csv_file.name} → {table_name}...")
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}')")
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  → {count:,} rows")

    con.close()
    print(f"\nDone. Database written to {db_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    ingest(sys.argv[1], sys.argv[2])
