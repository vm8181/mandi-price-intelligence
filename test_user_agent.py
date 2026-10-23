"""
Diagnostic: does data.gov.in treat Python requests differently because of the User-Agent?

Sends the same one-row request three ways and times each:
  A. Python's default label   ("python-requests/x.y")
  B. PowerShell's label       (which answered in about 1 second)
  C. A normal Chrome label

Run:  python test_user_agent.py
Uses your personal key from DATAGOV_API_KEY if it is set, otherwise the public sample key.
The key itself is never printed.
"""
import os
import time

import requests

URL = "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24"
SAMPLE_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"  # public sample key
api_key = os.environ.get("DATAGOV_API_KEY") or SAMPLE_KEY
print("Using", "your personal key" if api_key != SAMPLE_KEY else "the public sample key (shared, often rate-limited)", "\n")
PARAMS = {"api-key": api_key, "format": "json", "limit": 1}

LABELS = {
    "A. Python default": None,   # None = let requests send its own User-Agent
    "B. PowerShell-like": "Mozilla/5.0 (Windows NT 10.0; Microsoft Windows 10.0.26100; en-IN) WindowsPowerShell/5.1",
    "C. Chrome-like": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
    "D. Project label": "Mozilla/5.0 (compatible; mandi-price-intelligence/1.0)",  # used by our scripts
}

for name, agent in LABELS.items():
    headers = {} if agent is None else {"User-Agent": agent}
    sent = agent or requests.utils.default_user_agent()
    print(f"{name:<20} sending as: {sent[:60]}")
    started = time.time()
    try:
        resp = requests.get(URL, params=PARAMS, headers=headers, timeout=30)
        took = time.time() - started
        try:
            body = resp.json()
            if body.get("status"):
                detail = f"status={body.get('status')} total={body.get('total')}"
            else:   # error replies carry a message instead, e.g. {"error": "..."}
                detail = f"reply: {str(body)[:100]}"
        except ValueError:
            detail = f"reply: {resp.text[:100]!r}"
        if resp.status_code == 429:
            wait = resp.headers.get("Retry-After")
            detail += f"  | rate-limited, server asks to wait: {wait + ' s' if wait else 'not stated'}"
        print(f"{'':<20} HTTP {resp.status_code} in {took:.1f}s  {detail}\n")
    except requests.Timeout:
        print(f"{'':<20} NO ANSWER within 30s\n")
    except requests.RequestException as err:
        print(f"{'':<20} FAILED after {time.time() - started:.1f}s: {type(err).__name__}: {str(err)[:100]}\n")
