"""CatalogManager: search microdata.gov.in, load survey-round instances."""

from pathlib import Path

from mospi_microdata.nada import NADACache, catalog_search
from mospi_microdata.adapters import ADAPTER_REGISTRY
from mospi_microdata.db import QueryEngine
from mospi_microdata.survey import Survey
from mospi_microdata.errors import DataNotFoundError


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

    def setup(self, study_id: str, round_id: str, csv_zip: str) -> Path:
        """Set up a survey-round from a downloaded CSV zip file.

        Downloads metadata from microdata.gov.in, extracts the zip,
        and ingests the CSVs into a DuckDB database in data_dir.

        Args:
            study_id: Survey identifier (e.g., "ASI")
            round_id: Round identifier (e.g., "2023-24")
            csv_zip: Path to the CSV zip file downloaded from microdata.gov.in

        Returns:
            Path to the created DuckDB file.

        Example:
            catalog = Catalog(data_dir="./data")
            catalog.setup("ASI", "2023-24", "~/Downloads/ASI_DATA_2023_24_CSV.zip")
            asi = catalog.load("ASI", "2023-24")
        """
        from mospi_microdata.ingest import ingest_zip, ASI_TABLE_MAP

        study_id = study_id.upper()
        if study_id not in ADAPTER_REGISTRY:
            raise KeyError(f"No adapter for study: {study_id}. Available: {list(ADAPTER_REGISTRY.keys())}")

        adapter_cls = ADAPTER_REGISTRY[study_id]
        db_file = adapter_cls.db_filename(round_id)
        db_path = self.data_dir / db_file

        # Pick the right table map for this study
        table_maps = {
            "ASI": ASI_TABLE_MAP,
        }
        table_map = table_maps.get(study_id)
        if not table_map:
            raise KeyError(f"No ingest table map for study: {study_id}")

        zip_path = Path(csv_zip).expanduser().resolve()
        print(f"Setting up {study_id} {round_id}...")
        result = ingest_zip(zip_path, db_path, table_map, verbose=True)

        # Pre-cache metadata
        try:
            adapter = adapter_cls(round_id, cache=self._cache)
            print(f"\nMetadata cached for {adapter.idno}")
        except Exception as e:
            print(f"\nWarning: could not cache metadata (will retry on load): {e}")

        print(f"\nReady! Use: catalog.load(\"{study_id}\", \"{round_id}\")")
        return result

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
                f"Run catalog.setup(\"{study_id}\", \"{round_id}\", \"<path_to_csv_zip>\") to ingest data."
            )

        # Now instantiate adapter (may fetch metadata from cache or network)
        adapter = adapter_cls(round_id, cache=self._cache)

        engine = QueryEngine(str(db_path), allowed_tables=set(adapter.tables()))
        return Survey(adapter, engine)
