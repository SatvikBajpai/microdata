"""Typed exceptions for mospi_microdata."""


class MospiError(Exception):
    """Base exception for mospi_microdata."""


class NetworkError(MospiError):
    """Raised when a NADA API call fails due to network issues."""


class StudyNotFoundError(MospiError):
    """Raised when a study/variable is not found in the NADA catalog."""


class DataNotFoundError(MospiError):
    """Raised when a local DuckDB data file is missing."""


class InvalidTableError(MospiError):
    """Raised when a table name is not in this survey's table set."""


class QueryError(MospiError):
    """Raised when a SQL query is invalid or blocked by guardrails."""
