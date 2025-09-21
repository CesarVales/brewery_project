import types
import os
from stages.bronze.brewery_api_ingestion import api_data_fetch


class Resp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self._raise = False

    def raise_for_status(self):
        if self._raise:
            raise Exception("err")

    def json(self):
        return self._payload


def test_api_data_fetch_success(monkeypatch):
    calls = {"n": 0}

    def fake_get(url):
        calls["n"] += 1
        r = Resp(200, {"ok": True})
        r._raise = False
        return r

    monkeypatch.setenv("API_URL", "http://x")
    monkeypatch.setitem(api_data_fetch.__globals__, "requests", types.SimpleNamespace(get=fake_get, RequestException=Exception))
    monkeypatch.setitem(api_data_fetch.__globals__, "get_run_logger", lambda: types.SimpleNamespace(error=lambda *a, **k: None, info=lambda *a, **k: None))
    assert api_data_fetch() == {"ok": True}
    assert calls["n"] == 1


def test_api_data_fetch_retries_and_returns_none(monkeypatch):
    def fake_get(url):
        r = Resp(500, None)
        r._raise = True
        return r

    monkeypatch.setenv("API_URL", "http://x")
    monkeypatch.setitem(api_data_fetch.__globals__, "requests", types.SimpleNamespace(get=fake_get, RequestException=Exception))
    monkeypatch.setitem(api_data_fetch.__globals__, "get_run_logger", lambda: types.SimpleNamespace(error=lambda *a, **k: None, info=lambda *a, **k: None))
    assert api_data_fetch() is None
