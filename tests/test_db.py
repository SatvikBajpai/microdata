import pytest

from mospi_microdata.db import QueryEngine
from mospi_microdata.errors import InvalidTableError


DB_PATH = "data/asi_2023_24.duckdb"
ALLOWED_TABLES = {
    "block_a",
    "block_b",
    "block_c",
    "block_d",
    "block_e",
    "block_f",
    "block_g",
    "block_h",
    "block_i",
    "block_j",
}


def test_execute_rejects_non_survey_tables():
    engine = QueryEngine(DB_PATH, allowed_tables=ALLOWED_TABLES)

    with pytest.raises(InvalidTableError):
        engine.execute("select * from duckdb_tables()", limit=5)


def test_execute_allows_survey_tables():
    engine = QueryEngine(DB_PATH, allowed_tables=ALLOWED_TABLES)

    df = engine.execute("select count(*) as n from block_a", limit=1)

    assert df.to_dict(orient="records") == [{"n": 68641}]
