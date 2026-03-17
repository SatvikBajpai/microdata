"""User-facing Survey class wrapping an adapter and query engine."""

import pandas as pd

from mospi_microdata.adapters.base import SurveyAdapter
from mospi_microdata.db import QueryEngine


class Survey:
    """A loaded survey-round. Provides metadata access and data querying."""

    def __init__(self, adapter: SurveyAdapter, engine: QueryEngine):
        self._adapter = adapter
        self._engine = engine

    def metadata(self) -> dict:
        """Study-level metadata from microdata.gov.in."""
        return self._adapter.metadata()

    def files(self) -> list[dict]:
        """List data files with case counts and variable counts."""
        return self._adapter.files()

    def variables(self, file: str) -> list[dict]:
        """Variables in a given file with labels."""
        return self._adapter.variables(file)

    def variable(self, file: str, var: str) -> dict:
        """Detailed info for a single variable: labels, value codes, summary stats."""
        return self._adapter.variable(file, var)

    def join_map(self) -> dict[str, str]:
        """Table name -> join key column mapping."""
        return self._adapter.join_map()

    def load_file(self, name: str, columns: list[str] | None = None) -> pd.DataFrame:
        """Load an entire data file (DuckDB table) as a DataFrame."""
        return self._engine.load_table(name, columns)

    def query(self, sql: str, limit: int | None = None) -> pd.DataFrame:
        """Execute a read-only SQL query and return a DataFrame.

        Args:
            sql: SQL query string.
            limit: Max rows to fetch. When set, uses fetchmany() to avoid
                   materializing the full result in memory.
        """
        return self._engine.execute(sql, limit=limit)

    def sample(self, file: str, n: int = 5) -> pd.DataFrame:
        """Return a random sample from a data file."""
        return self._engine.sample(file, n)
