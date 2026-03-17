"""Client for the MoSPI NADA (National Data Archive) API at microdata.gov.in."""

import json
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen, Request

from mospi_microdata.errors import NetworkError, StudyNotFoundError

BASE_URL = "https://microdata.gov.in/NADA/index.php/api"


def _get(path: str, params: dict | None = None) -> dict:
    """Make a GET request to the NADA API."""
    url = f"{BASE_URL}/{path}"
    if params:
        qs = urlencode({k: v for k, v in params.items() if v is not None})
        if qs:
            url = f"{url}?{qs}"
    req = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except URLError as e:
        raise NetworkError(f"Failed to reach microdata.gov.in: {e}") from e


def catalog_search(keyword: str | None = None, ps: int = 200) -> list[dict]:
    """Search the NADA catalog. Returns list of study entries."""
    params = {"ps": str(ps)}
    if keyword:
        params["study_keywords"] = keyword
    data = _get("catalog", params)
    return data.get("result", {}).get("rows", [])


def get_study(idno: str) -> dict:
    """Get full study metadata by NADA idno (e.g. 'DDI-IND-NSO-ASI-2023-24')."""
    data = _get(f"catalog/{idno}")
    if data.get("status") == "failed":
        raise StudyNotFoundError(f"Study not found: {idno}. {data.get('message', '')}")
    return data.get("dataset", {})


def get_data_files(idno: str) -> dict[str, dict]:
    """Get data files for a study. Returns dict keyed by file_id."""
    data = _get(f"catalog/{idno}/data_files")
    return data.get("datafiles", {})


def get_variables(idno: str) -> list[dict]:
    """Get all variables for a study, paginating until exhaustion."""
    page_size = 500
    offset = 0
    all_vars = []
    while True:
        data = _get(f"catalog/{idno}/variables", {"ps": str(page_size), "offset": str(offset)})
        batch = data.get("variables", [])
        if not batch:
            break
        all_vars.extend(batch)
        total = int(data.get("total", 0))
        if len(all_vars) >= total or len(batch) < page_size:
            break
        offset += page_size
    return all_vars


def get_variable_detail(idno: str, vid: str) -> dict:
    """Get detailed info for a single variable by vid (e.g. 'V64')."""
    data = _get(f"catalog/{idno}/variable/{vid}")
    if data.get("status") == "failed":
        raise StudyNotFoundError(f"Variable not found: {vid}. {data.get('message', '')}")
    return data.get("variable", {})


class NADACache:
    """Caches NADA API responses to disk to avoid repeated network calls.

    Cache is stored in data_dir/.nada_cache/<idno>/.
    Call refresh() to re-fetch from the API.
    """

    def __init__(self, data_dir: str | Path):
        self.cache_dir = Path(data_dir) / ".nada_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _study_cache_dir(self, idno: str) -> Path:
        d = self.cache_dir / idno
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _read_cache(self, idno: str, name: str) -> dict | list | None:
        path = self._study_cache_dir(idno) / f"{name}.json"
        if path.exists():
            with open(path) as f:
                return json.load(f)
        return None

    def _write_cache(self, idno: str, name: str, data):
        path = self._study_cache_dir(idno) / f"{name}.json"
        with open(path, "w") as f:
            json.dump(data, f)

    def study(self, idno: str) -> dict:
        cached = self._read_cache(idno, "study")
        if cached:
            return cached
        data = get_study(idno)
        self._write_cache(idno, "study", data)
        return data

    def data_files(self, idno: str) -> dict[str, dict]:
        cached = self._read_cache(idno, "data_files")
        if cached:
            return cached
        data = get_data_files(idno)
        self._write_cache(idno, "data_files", data)
        return data

    def variables(self, idno: str) -> list[dict]:
        cached = self._read_cache(idno, "variables")
        if cached:
            return cached
        data = get_variables(idno)
        self._write_cache(idno, "variables", data)
        return data

    def variable_detail(self, idno: str, vid: str) -> dict:
        cached = self._read_cache(idno, f"var_{vid}")
        if cached:
            return cached
        data = get_variable_detail(idno, vid)
        self._write_cache(idno, f"var_{vid}", data)
        return data

    def refresh(self, idno: str):
        """Clear cache for a study and re-fetch."""
        import shutil
        d = self._study_cache_dir(idno)
        if d.exists():
            shutil.rmtree(d)
        self.study(idno)
        self.data_files(idno)
        self.variables(idno)
