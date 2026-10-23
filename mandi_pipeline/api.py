"""
Talking to the data.gov.in API: one request, made safely.

Every lesson from running against the real API lives here, in one place:
  - A browser-style User-Agent: requests labelled "python-requests" get no answer.
  - Two time limits: silence from the server, and total time for one answer (a slow
    trickle of data could otherwise hold a request for many minutes).
  - Retries with growing waits for server and network errors.
  - HTTP 429 (rate limit): wait as long as the server asks.
  - HTTP 401/403 (key refused): stop at once. A wrong key won't fix itself, and
    retrying thousands of days would only hide the cause.
  - Internet down on our side: pause until it's back, without using up retries.
  - The API key never appears in any message: error texts can contain the full URL.
  - Never trust "status: ok" alone: the caller checks that the numbers add up.
"""
import re
import socket
import sys
import threading
import time

import requests

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"     # Variety-wise Daily Market Prices
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
SAMPLE_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"   # public test key, 10 rows max
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; mandi-price-intelligence/1.0)"}

_print_lock = threading.Lock()


def say(text: str) -> None:
    """Print one whole line, even when several requests run at once."""
    with _print_lock:
        print(text, flush=True)


class ApiFailed(Exception):
    """A request kept failing after every retry."""


class ApiKeyRefused(Exception):
    """The API refused the key (HTTP 401/403). Not worth retrying."""


def redact(text) -> str:
    """Blank out the API key wherever it appears, e.g. inside an error's URL."""
    return re.sub(r"(api-key=)[^&\s'\"]+", r"\1***", str(text))


def network_is_down(err: Exception) -> bool:
    """True when the failure is our own internet connection, not the server."""
    text = str(err)
    return "getaddrinfo" in text or "NameResolution" in text or "Failed to resolve" in text


def describe(err: Exception, silence_timeout: int) -> str:
    """A short, plain reason for a failure, with the API key removed."""
    text = str(err)
    if network_is_down(err):
        return "cannot look up api.data.gov.in: internet or DNS is down"
    if isinstance(err, requests.exceptions.ChunkedEncodingError) or "IncompleteRead" in text:
        return "connection dropped in the middle of the download"
    if isinstance(err, requests.ConnectTimeout):
        return "could not connect to the server in time"
    if isinstance(err, requests.Timeout):
        return text if "longer than" in text else f"no data from the server for {silence_timeout}s"
    if isinstance(err, requests.ConnectionError):
        return "connection to the server failed"
    return redact(text)[:160]


class DataGovClient:
    """Makes requests to the API. Safe to use from several threads at once."""

    def __init__(self, api_key: str, silence_timeout: int = 45, max_seconds: int = 180,
                 retries: int = 4, network_wait_minutes: int = 30, sleep=time.sleep):
        self.api_key = api_key
        self.silence_timeout = silence_timeout   # seconds of silence before giving up
        self.max_seconds = max_seconds           # total seconds allowed for one answer
        self.retries = retries
        self.network_wait_minutes = network_wait_minutes
        self.sleep = sleep                       # replaceable in tests, so they don't wait

    # ----- one HTTP request with a total time limit -------------------------------
    def _fetch(self, params: dict) -> requests.Response:
        started = time.time()
        resp = requests.get(BASE_URL, params={"api-key": self.api_key, **params}, headers=HEADERS,
                            timeout=(15, self.silence_timeout), stream=True)
        chunks = []
        for chunk in resp.iter_content(chunk_size=65536):
            chunks.append(chunk)
            if time.time() - started > self.max_seconds:
                resp.close()
                raise requests.Timeout(f"answer took longer than {self.max_seconds}s to download")
        resp._content = b"".join(chunks)          # so resp.json() and resp.text work as usual
        return resp

    def _wait_for_network(self) -> None:
        """Pause until the server's name can be looked up again; stop the run if it never is."""
        deadline = time.time() + self.network_wait_minutes * 60
        say(f"    internet looks down; checking every 30s for up to {self.network_wait_minutes} min")
        while time.time() < deadline:
            self.sleep(30)
            try:
                socket.getaddrinfo("api.data.gov.in", 443)
                say("    internet is back, continuing")
                return
            except OSError:
                pass
        sys.exit(f"Internet was down for {self.network_wait_minutes} minutes. Stopped. "
                 "Run again when you're back online; finished days are skipped.")

    # ----- one request, with every safety rule ------------------------------------
    def get(self, params: dict, label: str) -> dict:
        attempt, network_waits = 0, 0
        while attempt < self.retries:
            attempt += 1
            started = time.time()
            try:
                resp = self._fetch(params)
                took = time.time() - started
                if resp.status_code == 200:
                    body = resp.json()
                    if body.get("status") in (None, "ok"):
                        if int(params.get("limit", 0)) == 1:     # a total-only request
                            say(f"    {label}: {int(body.get('total') or 0):,} rows in total ({took:.1f}s)")
                        else:
                            say(f"    {label}: {len(body.get('records', [])):,} rows in {took:.1f}s")
                        return body
                    raise RuntimeError(f"API status={body.get('status')} message={body.get('message')}")
                if resp.status_code in (401, 403):
                    raise ApiKeyRefused(f"HTTP {resp.status_code}: the API key was refused. "
                                        "Check the key saved by setup_scheduled_task.ps1.")
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After", "")
                    wait = int(retry_after) if retry_after.isdigit() else 60 * attempt
                    say(f"    {label}: rate-limited (HTTP 429), waiting {wait}s")
                    if attempt == self.retries:
                        raise ApiFailed(f"{label}: rate limit still in force after {self.retries} attempts")
                    self.sleep(wait)
                    continue
                raise RuntimeError(f"HTTP {resp.status_code}: {redact(resp.text[:200])}")
            except (requests.RequestException, ValueError, RuntimeError) as err:
                reason = describe(err, self.silence_timeout)
                say(f"    {label}: failed after {time.time() - started:.1f}s ({reason}), attempt {attempt}")
                if network_is_down(err) and network_waits < 10:
                    network_waits += 1
                    self._wait_for_network()
                    attempt -= 1                 # our connection's fault: don't count it
                    continue
                if attempt == self.retries:
                    raise ApiFailed(f"{label}: {reason}")
                self.sleep(5 * 2 ** (attempt - 1))       # 5s, 10s, 20s
        raise ApiFailed(f"{label}: all retries used")
