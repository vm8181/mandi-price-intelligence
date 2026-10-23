# Design decision log

Each entry records a choice, the options considered, and why. Newest first.

---

## 004. Load in two modes: one-time backfill, then a rolling 7-day daily update

**Date:** 22 Oct 2026
**Decision:** The same extractor runs in two modes.
- Backfill, once: `--last-years 10`, newest day first, resumable across sessions.
- Daily: `--last-days 7`, every evening, ending at yesterday (T-1).

**Why:** Mandis upload prices days late. On 22 Oct, 20 Oct had 15,529 rows against
18,307 for 19 Oct, because uploads were still arriving. Re-pulling the last 7 days on
every run catches late rows. A day is marked finished (`_DONE`) once it is 7 days
old, so it is never downloaded again.

**Options considered:**
- Pull only yesterday: misses late uploads permanently.
- Re-pull everything each day: about 50M rows a day, far beyond the rate limit.

**Checked by:** a simulation of a backfill plus nine daily runs. Every day was pulled
while recent, marked finished at 7 days old, and never pulled after that.

---

## 003. Keep 10 years of history, stored as gzip-compressed raw pages

**Date:** 22 Oct 2026
**Decision:** Load every commodity from 22 Oct 2016 to yesterday, about 49 million
rows. Save each API page exactly as received, gzip-compressed (`page_0001.json.gz`).

**Why 10 years:** History is available back past 2015. Ten years shows long-term
trends, ten seasonal cycles, and price behaviour across several years of weather
and policy changes. Five years was the minimum considered acceptable.

**Why compressed:** Measured on a 5,000-row page shaped like real API data:

| Storage | 5 years | 10 years |
| --- | --- | --- |
| Raw JSON | 7.2 GB | 12.9 GB |
| Gzip-compressed pages (chosen) | about 0.7 GB | about 1.2 GB |
| Delta tables in Databricks | about 0.3 GB | about 0.6 GB |

Compression is about 10.6x, because every row repeats the same field names and
many values. Bronze still holds the untouched source data: compression changes how
it is stored, not what it contains. Spark and Databricks read `.json.gz` directly.

**Options considered:**
- Raw JSON: simplest, but about 13 GB on a laptop.
- Convert to Parquet on download: smallest, but bronze would no longer be the
  untouched source, and a conversion bug could silently corrupt history.
- Keep fewer years: smaller, but weaker trend analysis.

**Confirmed 22 Oct 2026:** 1 Oct 2026 downloaded as 22,873 rows in 5 pages of
5,000, taking 0.5 MB on disk, about 23 bytes per row against the 25 estimated.
Ten years should therefore take about 1.1 to 1.8 GB. The download took about
50 seconds, with single pages taking between 2.5 and 23.6 seconds, so the full
backfill is roughly 30 to 45 hours, run over several sessions.

---

## 002. Send a browser-style User-Agent that names the project

**Date:** 22 Oct 2026
**Decision:** All requests send
`User-Agent: Mozilla/5.0 (compatible; mandi-price-intelligence/1.0)`.

**Why:** data.gov.in never answers requests labelled `python-requests` (Python's
default). The same request with a browser-style label is answered in under
2 seconds. See `docs/incidents/2026-10-22-datagov-python-timeouts.md`.

**Options considered:**
- Copy a real Chrome label: works, but misrepresents the client.
- The chosen label uses the accepted format and says honestly what is calling.

---

## 001. Extract from a computer in India, not from the cloud

**Date:** 22 Oct 2026
**Decision:** The extractor runs on the project owner's computer in India and uploads
files to Databricks. The daily run will be scheduled with Windows Task Scheduler.

**Why:** data.gov.in refuses connections from cloud servers. Tested:
- Google Colab, 22 Oct 2026: `No route to host`.
- Databricks Free Edition serverless notebook, 22 Oct 2026 (20:44 IST): the same
  one-row request, with the accepted User-Agent, was refused. Free Edition also limits
  outbound internet to a set of trusted domains.

So pulling from inside Databricks (the preferred design: one platform, one place to
monitor) is not possible. Cloud runners such as GitHub Actions run on servers abroad
and would very likely be refused too (not tested).

**Options compared:**
- Databricks pulls directly: best design, but blocked (tested above).
- Laptop downloads, Databricks CLI uploads, both scheduled nightly: chosen.
- Manual upload through the website: cannot be scheduled; about 18,000 backfill
  files plus new ones daily.

**Trade-off:** the computer must be on when the daily run is due. A missed day is
caught by the next run, because each run covers the last 7 days.
