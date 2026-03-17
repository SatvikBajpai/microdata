"""Abstract base class for survey adapters."""

from abc import ABC, abstractmethod


class SurveyAdapter(ABC):
    """Base adapter that all survey-specific adapters must implement."""

    @abstractmethod
    def metadata(self) -> dict:
        """Study-level metadata from NADA: title, abstract, keywords, producers."""

    @abstractmethod
    def files(self) -> list[dict]:
        """List of data files with descriptions, case counts, variable counts."""

    @abstractmethod
    def variables(self, file: str) -> list[dict]:
        """Variables in a given file with labels and types."""

    @abstractmethod
    def variable(self, file: str, var: str) -> dict:
        """Single variable detail with value labels, summary stats."""

    @classmethod
    @abstractmethod
    def db_filename(cls, round_id: str) -> str:
        """Return the DuckDB filename for a round. Must work without network."""

    @abstractmethod
    def tables(self) -> list[str]:
        """DuckDB table names for this survey-round."""

    @abstractmethod
    def join_map(self) -> dict[str, str]:
        """Mapping of table name -> join key column."""
