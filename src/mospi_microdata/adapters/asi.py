"""ASI (Annual Survey of Industries) adapter — reads all metadata from NADA API."""

from mospi_microdata.adapters.base import SurveyAdapter
from mospi_microdata.nada import NADACache

# NADA idno pattern for ASI rounds
IDNO_TEMPLATE = "DDI-IND-NSO-ASI-{round_id}"

# Map NADA file_id (F1..F10) to DuckDB table names
FILE_ID_TO_TABLE = {
    "F1": "block_a", "F2": "block_b", "F3": "block_c", "F4": "block_d",
    "F5": "block_e", "F6": "block_f", "F7": "block_g", "F8": "block_h",
    "F9": "block_i", "F10": "block_j",
}
TABLE_TO_FILE_ID = {v: k for k, v in FILE_ID_TO_TABLE.items()}

# The first variable in each block (after yr, blk) is the factory ID / join key.
# This is discoverable from the NADA variables list (the column that matches
# block_a's primary key). We detect it: the first variable whose name is NOT yr/blk.
SKIP_VARS = {"yr", "blk"}


def _db_filename(round_id: str) -> str:
    """Convert round_id like '2023-24' to db filename 'asi_2023_24.duckdb'."""
    return f"asi_{round_id.replace('-', '_')}.duckdb"


class ASIAdapter(SurveyAdapter):
    """Adapter for ASI microdata. All metadata comes from microdata.gov.in NADA API."""

    def __init__(self, round_id: str, cache: NADACache):
        self.round_id = round_id
        self.idno = IDNO_TEMPLATE.format(round_id=round_id)
        self._cache = cache
        self._vars_indexed = False
        self._vars_by_fid: dict[str, list[dict]] = {}
        self._var_by_name: dict[str, dict] = {}  # keyed by "fid:name"

    def _ensure_vars(self):
        """Lazy-load and index variables on first access."""
        if self._vars_indexed:
            return
        all_vars = self._cache.variables(self.idno)
        for v in all_vars:
            fid = v["fid"]
            self._vars_by_fid.setdefault(fid, []).append(v)
            self._var_by_name[f"{fid}:{v['name']}"] = v
        self._vars_indexed = True

    @classmethod
    def db_filename(cls, round_id: str) -> str:
        """Get DB filename without needing an instance (no network required)."""
        return _db_filename(round_id)

    @property
    def db_file(self) -> str:
        return _db_filename(self.round_id)

    def metadata(self) -> dict:
        study = self._cache.study(self.idno)
        meta = study.get("metadata", {})
        study_desc = meta.get("study_desc", {})
        study_info = study_desc.get("study_info", {})
        doc_desc = meta.get("doc_desc", {})
        return {
            "study_id": "ASI",
            "idno": study.get("idno", self.idno),
            "title": study.get("title", ""),
            "round_id": self.round_id,
            "abstract": study_info.get("abstract", ""),
            "keywords": [k["keyword"] for k in study_info.get("keywords", [])],
            "nation": [n["name"] for n in study_info.get("nation", [])],
            "producers": doc_desc.get("producers", []),
            "total_views": study.get("total_views"),
            "total_downloads": study.get("total_downloads"),
            "varcount": study.get("varcount"),
        }

    def files(self) -> list[dict]:
        data_files = self._cache.data_files(self.idno)
        result = []
        for fid, info in data_files.items():
            table = FILE_ID_TO_TABLE.get(fid)
            if not table:
                continue
            result.append({
                "file_id": fid,
                "table": table,
                "file_name": info.get("file_name", ""),
                "case_count": int(info.get("case_count", 0)),
                "var_count": int(info.get("var_count", 0)),
            })
        return result

    def variables(self, file: str) -> list[dict]:
        self._ensure_vars()
        fid = TABLE_TO_FILE_ID.get(file)
        if not fid:
            raise KeyError(f"Unknown file: {file}. Available: {list(FILE_ID_TO_TABLE.values())}")
        vars_list = self._vars_by_fid.get(fid, [])
        return [
            {"name": v["name"], "label": v["labl"], "vid": v["vid"]}
            for v in vars_list
        ]

    def variable(self, file: str, var: str) -> dict:
        self._ensure_vars()
        fid = TABLE_TO_FILE_ID.get(file)
        if not fid:
            raise KeyError(f"Unknown file: {file}")
        key = f"{fid}:{var}"
        basic = self._var_by_name.get(key)
        if not basic:
            available = [v["name"] for v in self._vars_by_fid.get(fid, [])]
            raise KeyError(f"Unknown variable: {var} in {file}. Available: {available}")

        # Fetch detailed info (with value labels, summary stats)
        detail = self._cache.variable_detail(self.idno, basic["vid"])
        meta = detail.get("metadata", {})

        result = {
            "name": var,
            "file": file,
            "label": detail.get("labl", basic["labl"]),
            "type": meta.get("var_format", {}).get("type", "unknown"),
            "interval": meta.get("var_intrvl", ""),
        }

        # Value labels (for discrete/categorical variables)
        categories = meta.get("var_catgry", [])
        if categories:
            result["value_labels"] = {
                c["value"]: c.get("labl", "") for c in categories
            }
            result["frequencies"] = {
                c["value"]: c.get("stats", [{}])[0].get("value")
                for c in categories if c.get("stats")
            }

        # Summary statistics
        sumstats = meta.get("var_sumstat", [])
        if sumstats:
            result["summary_stats"] = {s["type"]: s["value"] for s in sumstats}

        # Value range
        valrng = meta.get("var_valrng", {}).get("range", {})
        if valrng:
            result["range"] = {
                "min": valrng.get("min"),
                "max": valrng.get("max"),
                "count": valrng.get("count"),
            }

        return result

    def tables(self) -> list[str]:
        data_files = self._cache.data_files(self.idno)
        return [
            FILE_ID_TO_TABLE[fid]
            for fid in data_files
            if fid in FILE_ID_TO_TABLE
        ]

    def join_map(self) -> dict[str, str]:
        """Detect join keys: first non-yr/blk variable in each file."""
        self._ensure_vars()
        result = {}
        for fid, vars_list in self._vars_by_fid.items():
            table = FILE_ID_TO_TABLE.get(fid)
            if not table:
                continue
            for v in vars_list:
                if v["name"] not in SKIP_VARS:
                    result[table] = v["name"]
                    break
        return result
