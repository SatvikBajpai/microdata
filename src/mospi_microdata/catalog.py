"""CatalogManager: search microdata.gov.in, load survey-round instances."""

from pathlib import Path

from mospi_microdata.nada import NADACache, catalog_search
from mospi_microdata.adapters import ADAPTER_REGISTRY
from mospi_microdata.db import QueryEngine
from mospi_microdata.survey import Survey
from mospi_microdata.errors import DataNotFoundError, NetworkError


class Catalog:
    """Discover and load Indian government microdata surveys from microdata.gov.in."""

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self._cache = NADACache(self.data_dir)

    def search(self, keyword: str | None = None) -> list[dict]:
        """Search the NADA catalog at microdata.gov.in.

        Returns list of studies. Raises NetworkError if microdata.gov.in is unreachable.
        """
        rows = catalog_search(keyword=keyword)
        return [
            {
                "id": r["id"],
                "idno": r["idno"],
                "title": r["title"],
                "authoring_entity": r.get("authoring_entity", ""),
                "year_start": r.get("year_start"),
                "year_end": r.get("year_end"),
                "varcount": r.get("varcount"),
                "url": r.get("url"),
            }
            for r in rows
        ]

    def load(self, study_id: str, round_id: str) -> Survey:
        """Load a specific survey-round, returning a Survey instance.

        Checks for local data file BEFORE fetching metadata from the network,
        so offline use with cached metadata works reliably.

        Args:
            study_id: Survey identifier (e.g., "ASI")
            round_id: Round identifier (e.g., "2023-24")
        """
        study_id = study_id.upper()
        if study_id not in ADAPTER_REGISTRY:
            raise KeyError(f"No adapter for study: {study_id}. Available: {list(ADAPTER_REGISTRY.keys())}")

        adapter_cls = ADAPTER_REGISTRY[study_id]

        # Check DB existence BEFORE instantiating the adapter (which may need network)
        db_file = adapter_cls.db_filename(round_id)
        db_path = self.data_dir / db_file
        if not db_path.exists():
            raise DataNotFoundError(
                f"Data file not found: {db_path}. "
                f"Download the DuckDB file or run scripts/ingest_csv.py to create it."
            )

        # Now instantiate adapter (may fetch metadata from cache or network)
        adapter = adapter_cls(round_id, cache=self._cache)

        engine = QueryEngine(str(db_path), allowed_tables=set(adapter.tables()))
        return Survey(adapter, engine)
