"""Ingest microdata from CSV zip files into DuckDB databases."""

import tempfile
import zipfile
from pathlib import Path

import duckdb

# Map CSV filename prefixes to DuckDB table names
ASI_TABLE_MAP = {
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


def ingest_csv_dir(csv_dir: Path, db_path: Path, table_map: dict[str, str], verbose: bool = True) -> Path:
    """Load CSV files from a directory into a DuckDB database.

    Args:
        csv_dir: Directory containing CSV files.
        db_path: Output DuckDB file path.
        table_map: Mapping of CSV filename prefix → DuckDB table name.
        verbose: Print progress.

    Returns:
        Path to the created DuckDB file.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        for csv_file in sorted(csv_dir.glob("*.csv")):
            table_name = None
            for prefix, name in table_map.items():
                if csv_file.name.startswith(prefix):
                    table_name = name
                    break
            if not table_name:
                if verbose:
                    print(f"  Skipping unrecognized file: {csv_file.name}")
                continue
            if verbose:
                print(f"  Loading {csv_file.name} → {table_name}...")
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}')")
            if verbose:
                count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"    {count:,} rows")
    finally:
        con.close()
    return db_path


def ingest_zip(zip_path: str | Path, db_path: Path, table_map: dict[str, str], verbose: bool = True) -> Path:
    """Extract a CSV zip and ingest into DuckDB.

    Args:
        zip_path: Path to the CSV zip file.
        db_path: Output DuckDB file path.
        table_map: Mapping of CSV filename prefix → DuckDB table name.
        verbose: Print progress.

    Returns:
        Path to the created DuckDB file.
    """
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a valid zip file: {zip_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        if verbose:
            print(f"Extracting {zip_path.name}...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmpdir)

        # Find the directory containing CSVs (may be nested one level)
        tmpdir_path = Path(tmpdir)
        csv_files = list(tmpdir_path.rglob("*.csv"))
        if not csv_files:
            raise ValueError(f"No CSV files found in {zip_path.name}")

        # Use the parent of the first CSV as the csv_dir
        csv_dir = csv_files[0].parent

        if verbose:
            print(f"Found {len(csv_files)} CSV files in {csv_dir.name}")
            print(f"Ingesting into {db_path}...")

        return ingest_csv_dir(csv_dir, db_path, table_map, verbose=verbose)
