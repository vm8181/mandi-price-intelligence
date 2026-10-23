# Incident: the nightly download used the wrong secret

**Date:** 22 Oct 2026 22:31 to 23 Oct 2026 21:43 IST
**Impact:** Two nightly runs downloaded nothing (the backfill paused for about a day).
Uploads of already-finished days were unaffected. No data was lost or corrupted.
**Status:** Resolved.

## What happened

| Time (IST) | Event |
| --- | --- |
| 22 Oct 22:31 | First scheduled run. Download stopped at the first request: `HTTP 403: the API key was refused`. Upload of finished days ran normally, then the run was interrupted when the laptop shut down (Task Scheduler result 0xC000013A) |
| 23 Oct 21:00 | Second scheduled run. Same 403 in under a second; 144 waiting days uploaded and verified |
| 23 Oct 21:22 | `status.ps1` showed downloads unchanged and "finished with problems" |
| 23 Oct 21:29 | `test_user_agent.py` with the saved key: `403 Key not authorised` for every label |
| 23 Oct 21:31 | Saved key's length checked without revealing it: 36 characters, the length of a Databricks token (`dapi` + 32). A data.gov.in key is 56 |
| 23 Oct 21:43 | Correct 56-character key saved and tested (HTTP 200); scheduled run restarted |

## Root cause

During setup, the Databricks personal access token was pasted where the data.gov.in key
was expected. Both were created the same evening and look similar (long random strings).
The setup script only checked for a minimum length, so it accepted it.

## Side effect

The data.gov.in key travels in the request URL, so the Databricks token was sent to
data.gov.in's servers in six requests (two nightly runs, four test requests). The
connection was encrypted and the recipient a government server, so the risk was low,
but the token was treated as exposed: revoked, replaced, and the CLI reconfigured.

## Fixes

- `setup_scheduled_task.ps1` now validates the secret before saving: it rejects anything
  starting with `dapi` (a Databricks token) and anything not exactly 56 characters.
- `status.ps1` now reads the latest log and prints a red warning when the API key was
  refused, and translates Task Scheduler's "interrupted" code into plain words.

## Lessons

- Validate inputs where they enter the system. Every secret has a recognisable shape
  (prefix, length); checking it costs a second and would have prevented a lost day.
- When a secret goes somewhere it shouldn't, rotate it: assume exposure.
- Diagnose secrets without revealing them: comparing a length and the last four
  characters was enough to find the cause.
- Fail fast worked: the downloader stopped on the first 403 with a clear message
  instead of retrying 3,400 days, so the log showed the cause in one line.
