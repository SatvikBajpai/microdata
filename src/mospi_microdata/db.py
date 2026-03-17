"""DuckDB query engine with SQL guardrails."""

import re

import duckdb
import pandas as pd

from mospi_microdata.errors import QueryError, InvalidTableError

BLOCKED_KEYWORDS = [
    "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "TRUNCATE", "ATTACH", "COPY", "EXPORT", "IMPORT", "LOAD",
]
MAX_RESULT_ROWS = 500

# Simple identifier pattern for table names
_VALID_TABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SQL_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b",
    re.IGNORECASE,
)


def validate_sql(sql: str) -> str | None:
    """Return error message if SQL is not allowed, else None."""
    upper = sql.upper().strip()
    if not upper.startswith("SELECT") and not upper.startswith("WITH"):
        return "Only SELECT queries (including WITH/CTE) are allowed."
    for kw in BLOCKED_KEYWORDS:
        if f" {kw} " in f" {upper} ":
            return f"Blocked keyword: {kw}. Only read-only queries allowed."
    return None


def _validate_table(table: str, allowed_tables: set[str] | None):
    """Validate a table name is safe and allowed."""
    if not _VALID_TABLE_RE.match(table):
        raise InvalidTableError(f"Invalid table name: {table!r}")
    if allowed_tables is not None and table not in allowed_tables:
        raise InvalidTableError(
            f"Table {table!r} not in this survey. Available: {sorted(allowed_tables)}"
        )


def _validate_sql_tables(sql: str, allowed_tables: set[str] | None):
    """Validate table references in a SQL query against the survey table set."""
    if allowed_tables is None:
        return
    for table in _SQL_TABLE_RE.findall(sql):
        _validate_table(table, allowed_tables)


class QueryEngine:
    """Read-only DuckDB query engine."""

    def __init__(self, db_path: str, allowed_tables: set[str] | None = None):
        self.db_path = db_path
        self.allowed_tables = allowed_tables

    def execute(self, sql: str, limit: int | None = None) -> pd.DataFrame:
        """Execute a validated SQL query and return a DataFrame.

        Args:
            sql: SQL query string.
            limit: Max rows to fetch. Uses fetchmany() to avoid materializing
                   the entire result set in memory. None = no limit (use .df()).
        """
        error = validate_sql(sql)
        if error:
            raise QueryError(error)
        _validate_sql_tables(sql, self.allowed_tables)
        con = duckdb.connect(self.db_path, read_only=True)
        try:
            result = con.execute(sql)
            if limit is not None:
                rows = result.fetchmany(limit)
                columns = [desc[0] for desc in result.description]
                return pd.DataFrame(rows, columns=columns)
            return result.df()
        finally:
            con.close()

    def tables(self) -> list[str]:
        """List all tables in the database."""
        con = duckdb.connect(self.db_path, read_only=True)
        try:
            result = con.execute("SHOW TABLES")
            return [row[0] for row in result.fetchall()]
        finally:
            con.close()

    def sample(self, table: str, n: int = 5) -> pd.DataFrame:
        """Return a sample of rows from a table."""
        _validate_table(table, self.allowed_tables)
        con = duckdb.connect(self.db_path, read_only=True)
        try:
            result = con.execute(f"SELECT * FROM {table} USING SAMPLE {n}")
            return result.df()
        finally:
            con.close()

    def load_table(self, table: str, columns: list[str] | None = None) -> pd.DataFrame:
        """Load an entire table (or selected columns) as a DataFrame."""
        _validate_table(table, self.allowed_tables)
        if columns:
            cols = ", ".join(f'"{c}"' for c in columns)
        else:
            cols = "*"
        con = duckdb.connect(self.db_path, read_only=True)
        try:
            return con.execute(f"SELECT {cols} FROM {table}").df()
        finally:
            con.close()
