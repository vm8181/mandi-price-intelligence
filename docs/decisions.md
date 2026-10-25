# Design decision log

Each entry records a choice, the options considered, and why. Newest first.

---

## 012. A misspelt state, and duplicate rows that the source really has

**Date:** 25 Oct 2026
**Found by:** the completeness check (decision 005). 36 days, 1 Oct to 5 Nov 2025, would not
add up: the states summed to about 1% less than the day's total, and 1 to 5 Nov also had
duplicate rows. A diagnostic (`diagnose_incomplete_days.py`) asked for each day whole, in one
request, and compared it state by state with the files.

**What it showed:**
- **"Uttrakhand"** (sic): 65 to 111 rows a day sent under this spelling, sometimes beside
  "Uttarakhand" on the same day. No request asked for it, so those rows were missing.
- **Identical rows in the source itself:** 3 Nov 2025 has 19 exact copies in the API's own
  single-request answer. These are not caused by paging; they are in the data.

**Decision:**
- The downloader also asks for "Uttrakhand" (only on days that don't add up, so normal days
  cost nothing), and silver maps it to Uttarakhand in `ref_state_alias`.
- A day with duplicates passes only if the whole day, asked for again in one request, holds
  **exactly the same rows the same number of times**. That proves the copies are the source's,
  not a missing row hiding behind a copy. The copies are kept in bronze as received and silver
  keeps one (it already removes exact copies).

**Options considered:**
- Accept a day once its total matches, ignoring duplicates: simpler, but loses the pigeonhole
  proof from decision 005.
- Leave these days incomplete: honest, but a month of history would never reach Databricks.

**Trade-off:** the alias changes silver's rules fingerprint, so the next run rebuilds silver and
gold in full once (decision 010 by design). Later runs are incremental again.

---

## 011. History starts on 1 October 2025, extendable with one setting

**Date:** 25 Oct 2026
**Supersedes the window in 003** (ten years); the storage format in 003 is unchanged.

**Decision:** the nightly download covers a fixed window, from 1 October 2025 to yesterday
(`$HistoryStart` in `daily_run.ps1`, `HISTORY_START` in `status.py`). Each night still adds
yesterday, fills any missing day and refreshes the last 30 days; it no longer goes further back.

**Why:**
- **It answers the questions.** Every dashboard page has real data, and October can be compared
  across two years, which is enough to show how the Seasons page works.
- **More years cost a lot for little gain now.** The API takes about 52 seconds a day and often
  returns empty answers that need retries; ten years would be about 47 more hours of downloading.
- **Nothing is lost.** Extending the history is one setting: finished days are skipped, only the
  added days download, and the incremental pipeline (decision 010) rebuilds only the added months.

**Trade-off:** "is this price normal for the time of year?" can be judged for the months covered
twice; each added year sharpens it.

---

## 010. Silver and gold rebuild only the months that changed

**Date:** 25 Oct 2026
**In short:** incremental processing by month, driven by Delta's Change Data Feed, with an
automatic full rebuild whenever the rules change. Proved equal to a full rebuild by a test.

**Problem:** silver and gold were rebuilt from all history every morning. Fine while the
history was short, but the cost grew with the size of the history, not with what changed:
as the backfill adds years, every daily run would reprocess all of them to absorb one new day.

**Decision:**
- Bronze has **Change Data Feed** on. Each silver run reads only bronze's changes since the
  version it last processed (kept in `ops.pipeline_state`), and turns the changed days into
  **changed months**.
- Silver rebuilds those months and swaps each into the table in one Delta transaction
  (`replaceWhere`). Gold follows: the fact table and monthly prices for those months, and weekly
  prices for every week touching them. New markets and crops are appended to the dimensions.
- A **full rebuild** happens automatically on the first run, when silver's rules or the state
  mapping change (a fingerprint of the rule SQL and mapping rows is compared with the last run's),
  when gold's logic changes, or when the change feed can't be read. `mode = full` forces one.
- Every run is logged in `ops.pipeline_runs`: mode, reason, months, rows read and written, seconds.

**Why the month, not the day:** the unit and outlier flags compare a price with its market's
**monthly** median. One new day can change every flag in its month, so recomputing only the day
would give a different answer from a full rebuild. The month is the smallest unit that can be
rebuilt on its own and still give the identical result.

**How it's proved:** `tests/local_incremental_test.py` runs the real notebooks on local Spark:
a first full build, then new data (days turning final, a new provisional version including an
incomplete one, a backfilled older month), an incremental run, and a fresh full rebuild of the
same bronze. All eight output tables match row for row. It also checks that a run with no change
skips, and that a rules change forces a full rebuild.

**Reconciliation still covers all history:** `ops.silver_month_ledger` keeps one row per month
(bronze rows used, silver, quarantined, copies removed). Each run checks its own months balance,
and that the ledger's totals equal the tables' row counts, so untouched months stay accounted for.

**Measured (25 Oct 2026, serverless):**
- **Full rebuild** after the "Uttrakhand" state mapping was added (rules changed, decision 012):
  silver read 7,070,715 rows in 161.2 s; gold read 7,060,323 rows in 67.6 s.
- **The next run was incremental**: 37 changed days in 3 months. Silver read 1,292,819 rows (18%)
  in 85.8 s; gold read 1,289,036 rows in 41.3 s.
- Every run passed reconciliation. The fixed cost per run is about 30 to 40 s, so the time saved
  grows with the length of the history.

**Options considered:**
- Keep full rebuilds: simplest, but cost grows with history.
- Row-level `MERGE` on a report key: silver has no natural key that survives a provisional
  version being replaced, and the monthly flags would still need recomputing for the whole month.
- Compare bronze with silver on every run to find changes: works, but reads all of bronze daily.
- Change Data Feed + month replacement (chosen): reads only what changed, and the result is
  provably identical to a full rebuild.

**Trade-offs:**
- A changed day costs its whole month. Even so, a daily run touches one or two months against the
  whole history.
- Two state tables (`pipeline_state`, `pipeline_runs`) must be kept; they are small and self-describing.
- Changes made before the feed was switched on aren't recorded; the first run after that is a full
  rebuild anyway.

**Bug found by the test before release:** gold first stored "processed up to" as a Python datetime.
Python converts timestamps to the machine's time zone (IST here) while Spark used UTC, which shifted
the time by 5.5 hours and hid new silver runs. Times are now kept as Spark-formatted text.

---

## 009. Show recent days as provisional data; upload separately from download

**Date:** 24 Oct 2026
**In short:** day-level snapshot change data capture (CDC). Row-level CDC isn't possible
because the API has no change feed and records have no unique ID; the day is the smallest
unit whose completeness can be proved, so a changed day is re-captured and replaced whole.

**Decision:**
- Recent days (the last 30, still receiving late reports) are uploaded as **provisional
  versions** to `mandi_v2_provisional/arrival_date=D/pulled_at=T/expected_rows=N/`, but
  only when a new download has changed the day and passed both completeness checks.
- Silver uses a day's **final** data if it exists, otherwise its **latest complete**
  provisional version, and marks each row `data_status` final or provisional.
- Upload runs as its own scheduled task every 2 hours, separate from the nightly download.

**Why:**
- Holding days back until final left Databricks 30 days behind; a price tracker a month
  late is of little use.
- During the backfill, the download step ran for days, so the upload step that followed it
  never started: 119 finished days were stuck on the laptop. Separate tasks fix that.
- The row count in the folder name lets silver tell a fully uploaded version from a partly
  uploaded one, without a separate manifest.

**Options considered:**
- Overwrite the same files each night: Auto Loader doesn't reload changed files by default,
  and making it do so is fragile.
- Add every refresh to bronze: up to 30 copies of each recent day.
- Separate provisional area, uploaded only on change, superseded versions removed after
  each load (chosen): bounded storage, and every row's status is explicit.

**This is change data capture at day level (snapshot CDC).** The source has no change log,
no `updated_at` and no record ID, so row-level CDC would mean guessing which rows changed.
Instead a change is detected per day (row count on disk against the API's current total),
only changed days are captured and sent, and silver replaces the whole day.

**Safeguards:** the downloader removes a day's `_REFRESHED` marker before rewriting it, and
the uploader only sends days whose files are older than that marker and unchanged during
the upload, so a half-rewritten day is never sent.

**Known limit:** a final day overrides provisional data as soon as any of its rows load; a
final upload interrupted halfway is retried within 2 hours.

---

## 008. Gold is a star schema; national prices are medians of market prices

**Date:** 24 Oct 2026
**Decision:** Gold has dimensions (market, crop, date), one fact table at day x market x
crop x variety x grade, and weekly and monthly aggregates by crop and state plus
"All India". Keys are `xxhash64` hashes of the names.

**Why:**
- **Star schema:** the shape Power BI is built for: small dimensions filter one large
  fact table, and names are stored once.
- **Variety and grade in the fact grain:** Onion Red and White, or FAQ and Local grade,
  sell at very different prices; averaging them in the base table invents a price.
- **Median for state and national prices:** one market in the wrong unit or with bad
  data barely moves a median, but moves an average a lot.
- **Hash keys:** the same key on every full rebuild. Row-number keys would shift when a
  new market appears and silently break report relationships.
- **Equal weight per market-day:** the dataset has no arrival quantities, so a
  volume-weighted price isn't possible. Stated openly in the reports.

**Options considered:** one wide table (simple, but slow and repetitive in Power BI);
average instead of median (distorted by single markets); surrogate row numbers
(unstable on rebuild).

---

## 007. Two price flags: unit-suspect series and one-off outliers

**Date:** 24 Oct 2026
**Decision:** Silver flags a whole market-crop-month series as `is_unit_suspect` when it
is more than 10x off the typical market, and a single report as `is_price_outlier` when
it is more than 10x off its own market's month. Prices under Rs 1 are quarantined.

**Why:** the first rule (10x off the national median) flagged 2,361 rows; measuring
showed 94% were in well-covered crops and repeated every day at the same markets: those
markets price per piece, bunch or kg. Unit-suspect series are still valid for their own
market's trend, so they are kept but left out of comparisons across markets. Details and
counts in `data_quality.md`.

---

## 006. A day is "complete" only when two independent checks pass

**Date:** 23 Oct 2026
**Decision:** A day is marked finished (`_DONE`) only if (1) the states' rows add up
exactly to the day's own total, and (2) no row appears twice. Days within 30 days of
today are re-pulled every run regardless, to catch late uploads.

**Why:** Each check alone was fooled once. The paged download matched the total while
missing rows (caught only by duplicates). The state download had no duplicates while
two states answered empty (caught only by the total). Missing data now has to beat
both. See `incidents/2026-10-23-pagination-overlap.md`.

**Why 30 days:** Tamil Nadu's 1 Oct 2026 grew from 8,455 to 8,676 rows (+2.6%) in the
22 days after the date. 7 days would have frozen it too early. Cost: about 30 recent
days re-pulled each night.

**Trade-off:** the uploader sends only finished days, so Databricks lags by 30 days
until recent days are also uploaded as provisional (planned with the silver layer).

---

## 005. Download each day one state per request, not in pages

**Date:** 23 Oct 2026
**Decision:** Replace offset paging (5,000 rows per page) with one request per state
(limit 50,000), up to 4 states at a time. Saved to `data/raw/mandi_v2`.

**Why:** Paging swapped real rows for duplicates on 94 of 228 days (incident
2026-10-23). The API's order isn't stable, so page boundaries can't be trusted.

**Options considered:**
- One request per day (limit 50,000): no paging, but a whole day is about 12 MB and
  the connection was cut after about 60 s (tested on 1 Apr).
- Bulk download from the website: requires a state **and** district per download
  (about 700 districts), so not practical for 10 years.
- Paging plus a duplicate check, re-downloading flagged days by state: works, but two
  methods to maintain, and about 41% of days would need the second one anyway.
- State by state (chosen): one method, no page boundaries, small answers.

**Cost:** about 33 requests per day instead of 5. Measured at 153 s per day one state
at a time; parallel requests are expected to bring this to about 40 to 50 s (to be
measured).

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

## 003. Keep 10 years of history, stored as gzip-compressed raw pages (window replaced by 011)

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
