"""
Tests for mandi_pipeline/api.py: each test recreates a failure we met against the real
data.gov.in API and checks the client handles it. No network is used.

Run from the project folder:   python -m pytest -v
"""
import json

import pytest
import requests

from mandi_pipeline import api
from mandi_pipeline.api import ApiFailed, ApiKeyRefused, DataGovClient, redact

KEY = "579b464db66ec23bdd0000019999999999999999999999999999999"


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, chunks=None):
        self.status_code = status
        self.headers = headers or {}
        self._chunks = chunks if chunks is not None else [json.dumps(body or {}).encode()]

    def iter_content(self, chunk_size):
        yield from self._chunks

    def close(self):
        pass

    def json(self):
        return json.loads(self._content)

    @property
    def text(self):
        return self._content.decode()


def client(**kw):
    waits = []
    c = DataGovClient(KEY, sleep=waits.append, **kw)
    return c, waits


def test_sends_browser_style_user_agent(monkeypatch):
    """Incident 22 Oct: requests labelled python-requests got no answer."""
    seen = {}

    def fake_get(url, params, headers, timeout, stream):
        seen.update(headers)
        return FakeResponse(body={"status": "ok", "total": 1, "records": [{"a": 1}]})

    monkeypatch.setattr(requests, "get", fake_get)
    c, _ = client()
    c.get({"limit": 1}, "t")
    assert seen["User-Agent"].startswith("Mozilla/5.0")
    assert "python-requests" not in seen["User-Agent"]


def test_refused_key_stops_at_once(monkeypatch):
    """Incident 23 Oct: a wrong key must stop the run, not be retried for every day."""
    calls = []

    def fake_get(*a, **k):
        calls.append(1)
        return FakeResponse(status=403, body={"error": "Key not authorised"})

    monkeypatch.setattr(requests, "get", fake_get)
    c, waits = client()
    with pytest.raises(ApiKeyRefused):
        c.get({"limit": 1}, "t")
    assert len(calls) == 1 and waits == []


def test_rate_limit_waits_as_asked(monkeypatch):
    """HTTP 429: wait as long as Retry-After says, then succeed."""
    answers = iter([FakeResponse(status=429, headers={"Retry-After": "30"}),
                    FakeResponse(body={"status": "ok", "total": 0, "records": []})])
    monkeypatch.setattr(requests, "get", lambda *a, **k: next(answers))
    c, waits = client()
    c.get({"limit": 1}, "t")
    assert waits == [30]


def test_internet_down_pauses_without_using_retries(monkeypatch):
    """Incident 22 Oct: DNS failures while offline burned through days. Now: pause."""
    dns = requests.ConnectionError("Failed to resolve 'api.data.gov.in' ([Errno 11001] getaddrinfo failed)")
    answers = iter([dns, dns, dns, dns, dns,           # more failures than retries allow
                    FakeResponse(body={"status": "ok", "total": 0, "records": []})])

    def fake_get(*a, **k):
        item = next(answers)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(api.socket, "getaddrinfo", lambda *a, **k: [("ok",)])
    c, waits = client(retries=4)
    c.get({"limit": 1}, "t")                          # succeeds: offline time didn't count
    assert waits.count(30) == 5


def test_slow_trickle_is_cut_off_and_retried(monkeypatch):
    """Incident 22 Oct: one page took 12.5 minutes. Now there's a total time limit."""
    clock = iter(range(0, 10_000, 100))               # each chunk "takes" 100 s
    monkeypatch.setattr(api.time, "time", lambda: next(clock))
    answers = iter([FakeResponse(chunks=[b"x"] * 5),
                    FakeResponse(chunks=[json.dumps({"status": "ok", "total": 0, "records": []}).encode()])])
    monkeypatch.setattr(requests, "get", lambda *a, **k: next(answers))
    c, waits = client(max_seconds=180)
    c.get({"limit": 1}, "t")
    assert waits == [5]                               # one retry after the cut-off


def test_gives_up_after_retries(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(status=502, chunks=[b""]))
    c, waits = client(retries=4)
    with pytest.raises(ApiFailed):
        c.get({"limit": 1}, "t")
    assert waits == [5, 10, 20]


def test_key_never_appears_in_messages(monkeypatch, capsys):
    """Incident 22 Oct: error messages printed the full URL, including the key."""
    url_error = requests.ConnectionError(f"Max retries exceeded with url: /resource/x?api-key={KEY}&format=json")
    monkeypatch.setattr(requests, "get", lambda *a, **k: (_ for _ in ()).throw(url_error))
    c, _ = client(retries=2)
    with pytest.raises(ApiFailed) as err:
        c.get({"limit": 1}, "t")
    assert KEY not in str(err.value)
    assert KEY not in capsys.readouterr().out
    assert redact(f"api-key={KEY}&x=1") == "api-key=***&x=1"
