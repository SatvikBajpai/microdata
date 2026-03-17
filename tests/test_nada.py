from urllib.parse import parse_qs, urlparse

import mospi_microdata.nada as nada


def test_get_urlencodes_query_params(monkeypatch):
    captured = {}

    class DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        return DummyResponse()

    monkeypatch.setattr(nada, "urlopen", fake_urlopen)

    nada._get("catalog", {"study_keywords": "health & family", "ps": "1"})

    parsed = urlparse(captured["url"])
    assert parsed.path.endswith("/catalog")
    assert parse_qs(parsed.query) == {
        "study_keywords": ["health & family"],
        "ps": ["1"],
    }
