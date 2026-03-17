"""MCP server — thin wrapper over the mospi_microdata library.

All metadata comes from microdata.gov.in NADA API. Nothing is hardcoded.
"""

import os

from fastmcp import FastMCP

from mospi_microdata import Catalog
from mospi_microdata.db import MAX_RESULT_ROWS

DATA_DIR = os.environ.get("MOSPI_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))

mcp = FastMCP("MoSPI Microdata Server")
catalog = Catalog(data_dir=DATA_DIR)


@mcp.tool()
def search_catalog(keyword: str | None = None) -> list[dict]:
    """Search the microdata.gov.in catalog for surveys.

    Args:
        keyword: Keyword to search (e.g., "industries", "labour", "consumption")
    """
    return catalog.search(keyword=keyword)


@mcp.tool()
def get_metadata(study_id: str, round_id: str) -> dict:
    """Get study-level metadata from microdata.gov.in: title, abstract, keywords, producers.

    Args:
        study_id: Survey identifier (e.g., "ASI")
        round_id: Round identifier (e.g., "2023-24")
    """
    survey = catalog.load(study_id, round_id)
    return survey.metadata()


@mcp.tool()
def get_files(study_id: str, round_id: str) -> list[dict]:
    """List data files/blocks with case counts and variable counts.

    Args:
        study_id: Survey identifier (e.g., "ASI")
        round_id: Round identifier (e.g., "2023-24")
    """
    survey = catalog.load(study_id, round_id)
    return survey.files()


@mcp.tool()
def get_variables(study_id: str, round_id: str, file: str) -> list[dict]:
    """List variables in a data file with their official labels.

    Args:
        study_id: Survey identifier (e.g., "ASI")
        round_id: Round identifier (e.g., "2023-24")
        file: Table name (e.g., "block_e")
    """
    survey = catalog.load(study_id, round_id)
    return survey.variables(file)


@mcp.tool()
def get_variable_detail(study_id: str, round_id: str, file: str, variable: str) -> dict:
    """Get detailed info for a variable: label, value codes, summary stats.

    All information comes from the official microdata.gov.in metadata.

    Args:
        study_id: Survey identifier (e.g., "ASI")
        round_id: Round identifier (e.g., "2023-24")
        file: Table name (e.g., "block_e")
        variable: Variable name (e.g., "EI6")
    """
    survey = catalog.load(study_id, round_id)
    return survey.variable(file, variable)


@mcp.tool()
def get_join_map(study_id: str, round_id: str) -> dict:
    """Get the join key column for each table (detected from metadata).

    Args:
        study_id: Survey identifier (e.g., "ASI")
        round_id: Round identifier (e.g., "2023-24")
    """
    survey = catalog.load(study_id, round_id)
    return survey.join_map()


@mcp.tool()
def query_microdata(study_id: str, round_id: str, sql: str) -> dict:
    """Run a read-only SQL query against survey microdata in DuckDB.

    Results capped at 500 rows. Only SELECT/WITH queries allowed.

    Args:
        study_id: Survey identifier (e.g., "ASI")
        round_id: Round identifier (e.g., "2023-24")
        sql: DuckDB-compatible SQL SELECT query
    """
    survey = catalog.load(study_id, round_id)
    try:
        # Fetch one extra row to detect truncation, but never materialize more
        df = survey.query(sql, limit=MAX_RESULT_ROWS + 1)
        truncated = len(df) > MAX_RESULT_ROWS
        if truncated:
            df = df.head(MAX_RESULT_ROWS)
        records = df.to_dict(orient="records")
        result = {
            "columns": list(df.columns),
            "data": records,
            "row_count": len(records),
            "truncated": truncated,
        }
        if truncated:
            result["note"] = f"Showing first {MAX_RESULT_ROWS} rows. Add LIMIT or GROUP BY."
        return result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def sample_microdata(study_id: str, round_id: str, file: str, n: int = 5) -> dict:
    """Get a random sample of rows from a data file.

    Args:
        study_id: Survey identifier (e.g., "ASI")
        round_id: Round identifier (e.g., "2023-24")
        file: Table name (e.g., "block_a")
        n: Number of rows (max 20)
    """
    n = min(n, 20)
    survey = catalog.load(study_id, round_id)
    try:
        df = survey.sample(file, n)
        return {
            "file": file,
            "columns": list(df.columns),
            "data": df.to_dict(orient="records"),
            "row_count": len(df),
        }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    mcp.run()
