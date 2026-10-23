# Incident: data.gov.in API timed out for every Python request

**Date:** 22 October 2026, about 04:15 to 09:37 IST
**Impact:** No data could be pulled. Sample pulls, backfill counts and the daily feed all failed.
**Status:** Resolved. Root cause found, fix applied to both extraction scripts.

## What we saw

| Time (IST) | Client | Request | Result |
| --- | --- | --- | --- |
| ~03:57 | Python in Google Colab | Sample pull | `No route to host`: connection refused before reaching the API |
| ~04:02 | Browser | One page, no filter | Worked |
| ~04:08 | Browser | Date filter, 21-10-2026 | Worked, 3 rows |
| 04:23 to 04:50 | Python on own PC | Counts with date + commodity, date only, recent and old dates | HTTP 502 errors and timeouts |
| 04:43 | Python, personal key | Same | Same failures, so not the shared sample key |
| 04:48 | Python, 180 s timeout | Date only | HTTP 502 after exactly 61 s, twice |
| 08:54 to 09:01 | Python | State + District + date; the "current daily price" dataset | Read timeout after 60 s |
| 09:34 | PowerShell `Invoke-RestMethod` | One row, no filter | **OK in 1.08 s**, total 82,644,500 rows |
| 09:37 | Python, three User-Agent labels | One row, no filter | Default `python-requests`: no answer in 30 s. PowerShell-like and Chrome-like labels: **OK in 1.7 s** |

## Root cause

data.gov.in does not answer requests whose User-Agent header is Python's default,
`python-requests/x.y`. The connection opens, but no response comes back, so the client
times out (or a gateway in front of the API returns 502 after its own 60-second limit).
The same request with a browser-style User-Agent is answered in under 2 seconds.

Colab's failure was a separate issue: data.gov.in refuses connections from Google's
cloud servers outside India (`No route to host`), so extraction must run from an
Indian network.

## How it was found

Each test removed one possible cause:

1. Key: a personal key failed the same way as the shared sample key, so not the key.
2. Query: date-only, narrow (State + District + date), old and recent dates and a
   different, much smaller dataset all failed, so not query size.
3. Server: PowerShell got an answer in about 1 second, so the server was up.
4. Client: the same Python request succeeded once only the User-Agent changed,
   which confirmed the cause.

## Fix

Both `pull_mandi_sample.py` and `backfill_history.py` now send:

```
User-Agent: Mozilla/5.0 (compatible; mandi-price-intelligence/1.0)
```

This uses the browser-style format the server accepts while honestly naming the project.
`test_user_agent.py` stays in the repo to re-check this quickly if the API changes.

## Follow-up: rate limit (09:41)

Re-running the label test right after the fix returned **HTTP 429 (Too Many Requests)**
in about 1 second for every browser-style label, including the project label, while
the default Python label still got no answer. The label fix works; the 429 came from
the public sample key, which everyone shares and which we had used heavily that
morning.

Changes:
- `test_user_agent.py` uses the personal key from `DATAGOV_API_KEY` when set.
- `backfill_history.py` treats 429 separately: it waits as long as the server's
  `Retry-After` header asks, or 60 s, then 120 s, then 180 s, before giving up on
  that request.
- `pull_mandi_sample.py` stops with a clear rate-limit message.

## Lessons

- Change one thing at a time when diagnosing. Swapping keys, dates and datasets
  together would never have isolated a header.
- A second client (PowerShell, a browser) is the fastest way to tell "server down"
  from "server treats my client differently".
- A 502 at exactly 60 to 61 seconds points to a gateway timeout, not to an error
  in the request itself.
- Every extraction script now prints each request and its timing, so the next failure
  shows where it happens without extra debugging.
