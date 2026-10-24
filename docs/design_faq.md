# Design FAQ: every what, why, why not and when

The reasoning behind each part of the project, with the evidence for it. Grouped by
layer, in the order the data flows.

**Contents:** [Project and business](#project-and-business) ·
[Architecture](#architecture) · [Extraction](#extraction) ·
[Completeness](#completeness) · [Storage](#storage) · [Databricks platform](#databricks-platform) ·
[Bronze](#bronze) · [Silver](#silver) · [Gold](#gold) ·
[Orchestration](#orchestration) · [Git and code](#git-and-code) ·
[Security](#security) · [Testing](#testing) · [What I'd do next](#what-id-do-next)

---

## Project and business

**What problem does this solve?**
The same crop sells at very different prices in nearby mandis, and most sellers only know
the price at the one they visit. The government publishes every mandi's daily price, but
in a raw, inconsistent form. This project turns it into reliable daily, weekly and monthly
prices that can be compared across markets and over time.

**Who uses it, and for what?**
Farmers and traders: today's price and the best nearby market. Buyers: whether today's
price is high for the season. Journalists and policy watchers: unusual price rises and
prices below MSP. The data team: whether every day loaded completely.

**Farmers care about today's price. Why keep years of history?**
Today's price alone can't be judged. "Is Rs 4,800 for onion high?" needs the usual price
for this time of year; "should I store or sell?" needs how prices usually move after this
point in the season; "is this a spike?" needs normal behaviour to compare against. Like a
stock app: the price plus the chart behind it. Cost is small: about 23 bytes per row compressed.

**Why real government data instead of a Kaggle or generated dataset?**
Real data has real problems: late uploads, inconsistent names, mixed units, an unreliable
API. Solving those is the actual work of data engineering; a clean dataset hides it.

**What's the single most important result so far?**
Proving completeness. The first download matched every row count yet was missing real
rows on 94 of 228 days. Catching that, explaining it and fixing it is the difference
between numbers that look right and numbers that are right.

---

## Architecture

**What is the medallion architecture, and why use it?**
Three layers: **bronze** keeps the source data exactly as received, **silver** cleans and
validates it, **gold** shapes it for the business. Each layer has one job, so a problem
can be traced to one place, and anything can be rebuilt from the layer below. If a
cleaning rule is wrong, silver is rebuilt from bronze without downloading again.

**Why not clean the data while downloading?**
Then the raw source would be lost. If the cleaning had a bug, history would be silently
corrupted with no way to prove what the source said. Raw first, clean later.

**Why does extraction run on a laptop?**
data.gov.in refuses connections from cloud servers. Tested: Google Colab ("No route to
host") and a Databricks serverless notebook were both refused. A network in India works.

**Isn't a laptop a weak point?**
Yes, and it's managed: the task runs at next login if the laptop was off, every run
re-checks the last 30 days so a missed night is caught up, and Databricks keeps serving
the last good data. With a budget, a small server in India would replace it with no code changes.

**Why Databricks rather than just Python and a database?**
Auto Loader for incremental file loading, Delta tables with schema enforcement, Unity
Catalog for governance, serverless compute and built-in jobs. With tens of millions of
rows over the full history, Spark scales where a single-machine script would struggle.

---

## Extraction

**Why one request per state instead of paging?**
Paging by offset needs the server to return rows in the same order every time.
data.gov.in serves this dataset from a search index without a stable order, so a row near
a page boundary could appear on two pages while another appeared on none. Each duplicate
replaced a missing row, so the total still matched. A state's day fits in one request
(limit 50,000), so there are no page boundaries at all.

**Why not one request for the whole day?**
Tested: about 12 MB per day, and the connection was cut after about 60 seconds.

**Why 4 parallel requests and not 20?**
The API rate-limits (HTTP 429). Four cut a day from about 150 s to well under a minute
without triggering limits. More would mostly produce 429s and waits.

**What's the User-Agent fix?**
data.gov.in never answered requests labelled `python-requests` (the default). A
browser-style label naming the project, `Mozilla/5.0 (compatible; mandi-price-intelligence/1.0)`,
is answered in under 2 seconds. It's honest about who is calling, unlike copying a real Chrome label.

**How do you handle failures?**
- 429 rate limit: wait for `Retry-After`, or longer each attempt.
- Server errors: retry with backoff (5, 10, 20 s).
- Internet down: wait for it to return, without using up retries.
- Stuck downloads: a total time limit per request (one page once took 12.5 minutes).
- Refused key (401/403): stop immediately. Retrying a bad key only wastes time.

**Why "fail fast" for a bad key but retry for other errors?**
Retrying helps only when the cause is temporary. A refused key will be refused every time.

---

## Completeness

**How do you know a day is complete?**
Two independent checks: the states' rows add up exactly to the day's own total, and no row
appears twice. Each check alone was fooled once: paging matched the total but had
duplicates; one state download had no duplicates but two states answered empty.

**Why does "no duplicates" prove nothing is missing?**
The pigeonhole principle: if the total matches and every row is distinct, there's no room
for a missing row. A missing row can only hide behind a duplicate.

**What were the empty answers?**
Tamil Nadu and Uttar Pradesh once answered `status: ok` with 0 rows. Asked again, Tamil
Nadu returned all 8,676 rows. Without the sum check, the day would have been saved
missing 44% of its rows. Lesson: never trust "ok"; check that the parts add up.

**Why re-check the last 30 days, not 7?**
Measured: Tamil Nadu's 1 October grew 2.6% over the 22 days after the date. Seven days
would have frozen it too early.

**What happens when the download is stopped and restarted?**
Each day is checked first: if the files on disk match the API's current total, it costs
one small request and is skipped. Only missing, partial or changed days are downloaded.

---

## Storage

**Why gzip-compressed JSON and not Parquet?**
Bronze must be the untouched source. Converting on download means a conversion bug could
corrupt history unnoticed. Gzip changes how the data is stored, not what it is, and Spark
reads `.json.gz` directly. Compression is about 10x.

**Why keep raw files when the data is in Delta tables?**
They are the system of record. Bronze, silver and gold can all be rebuilt from them, as
happened when the first download was found faulty.

---

## Databricks platform

**What is Unity Catalog?**
Databricks' governance layer: a three-level namespace `catalog.schema.table`
(`mandi_dev.silver.mandi_prices`), with permissions, lineage and volumes for files.

**Why a `mandi_dev` catalog?**
Separating environments. The same notebooks run against `mandi_prod` by changing one job
parameter. Never develop on production data.

**What is a volume?**
A governed folder for files (not tables) in Unity Catalog. The raw files land in
`/Volumes/mandi_dev/bronze/raw_files/`.

**Serverless compute or a SQL warehouse: which and when?**
Serverless notebook compute for pipelines: it runs Python and SQL. A SQL warehouse only
runs SQL and is built for many short BI queries; Power BI connects there. Separating them
also stops a heavy pipeline slowing down dashboard users.

**What is liquid clustering, and why not partitioning?**
`CLUSTER BY (arrival_date, commodity)` organises data so queries filtering on those
columns read only what they need. Unlike partitions it doesn't create thousands of small
folders, and the columns can be changed later without rewriting the table.

---

## Bronze

**What does Auto Loader do?**
Loads only files it hasn't seen before, remembering them in a checkpoint. Reruns never
duplicate rows; new files are picked up automatically.

**Why keep every value as text?**
The source isn't consistent: prices arrive sometimes as text, sometimes as numbers. Typed
columns would make bronze fail or lose values when the source changes. Silver converts types.

**Why keep the original record JSON as well?**
It's the proof of what the source sent. When silver found 2,830 copies, comparing the
original JSON showed the only difference was a trailing space.

**Why not use `mergeSchema` when the table schema didn't match?**
The mismatch came from an old version of the table. `mergeSchema` would have mixed two
designs in one table. The right fix was to rebuild bronze cleanly from the files.

**How is bronze reconciled?**
For each file, the rows loaded must equal the total the API reported for that state.
Any mismatch fails the run.

---

## Silver

**What does silver do?**
Converts types, standardises state names, applies quality rules, quarantines failures,
removes copies, flags suspect prices, and proves every bronze row is accounted for.

**Why `try_cast` instead of `cast`?**
`cast` fails the whole run on one bad value. `try_cast` returns NULL, a rule catches the
NULL, and the row is quarantined with its reason. One bad row can't stop all the good ones.

**Why quarantine instead of deleting bad rows?**
Deleted rows are invisible: nobody can review them, count them or find out the rule was
wrong. Quarantine keeps them with the reason (395 rows, 0.06%).

**Why a reference table for state names, not a CASE statement?**
When the source introduces a new spelling, you add a row; no code change, no redeploy.
Silver also lists any state not yet in the table.

**Why were rules written after profiling?**
A rule that would quarantine a large share of rows is more likely a wrong rule than bad
data. Measuring first showed which rules mattered (e.g. 0 unreadable dates, 435 rows with
min > modal in the first data).

**What is the grain of silver?**
One row per price report. Markets genuinely report several prices a day for the same crop
(separate lots); collapsing them in silver would lose information. Gold summarises.

**The downloader found no duplicates but silver removed 2,830. Which was wrong?**
Neither. The downloader asks "did the download go wrong?" (byte-identical records, like
the paging overlap). Silver asks "is this the same price report?" after trimming spaces.
Every pair differed only by a trailing space in the market name; 37 markets are published
under both names. Keeping both would double their weight in every average.

**Why not trim spaces in the downloader?**
The raw layer must keep exactly what the source sent. Cleaning belongs in silver.

**How did you design the outlier rule?**
Started simple: 10x above or below the crop's national monthly median. Measured what it
flagged: 2,361 rows. My theory (rare crops with thin references) explained only 6%. The
rest were the same markets flagged every day, and multiplying back revealed per-bunch and
per-piece prices. So one flag became two: a **unit-suspect** series flag and a **one-off
outlier** flag. Outliers fell to 45.

**Why median and not mean or standard deviation?**
Prices here have extreme values (Rs 0.05 to Rs 1.65 lakh). A mean or standard deviation is
pulled by the very values you're trying to detect; a median isn't.

**Why is the typical level the median of market medians?**
So each market counts once. A market reporting 50 times a day would otherwise dominate.

**Why not convert per-bunch prices to per quintal?**
The weight of a bunch or a coconut isn't known and varies by market. Converting would
invent numbers. Flagging is honest.

**Why quarantine prices under Rs 1 but only flag per-bunch prices?**
Rs 8 per bunch is a real price, useful for that market's own trend. Rs 0.05 per quintal is
not a real price in any unit, so it can't be used for anything.

**Why need 3 markets for the unit flag and 5 reports for the outlier flag?**
With 2 markets disagreeing, you can't tell which is right. With fewer than 5 reports, a
market's "usual price" isn't meaningful.

**What's the known weakness of comparing to the majority?**
When most markets use another unit, the correct market gets flagged: Gazipur's chicken at
Rs 95/kg is realistic, but most markets report per bird. A reference table of expected
units per crop would fix it.

**How do you prove silver lost nothing?**
bronze = silver + quarantine + copies removed. If not equal, the run fails. Counts are logged each run in `ops.silver_quality_log`.

**Why rebuild silver in full each run instead of MERGE?**
Simpler and always consistent at this size (under a minute). The job's run history will
show when it slows down; that's the measured point to switch to incremental processing.

---

## Gold

**What is a star schema, and why use one?**
A central fact table of measurements (prices) linked to small dimension tables of
descriptions (market, crop, date). It's the shape Power BI is built for: names stored
once, fast filtering, simple relationships.

**What's the grain of the fact table, and why include variety and grade?**
One row per day, market, crop, variety and grade. Onion Red and White, or FAQ and Local
grade, sell at very different prices; averaging them would invent a price nobody paid.

**How are several reports a day combined?**
Lowest minimum, highest maximum, average modal price, number of reports. Outliers are left
out of the prices but counted, so every silver row is accounted for.

**Why hash keys (`xxhash64`) instead of row numbers?**
Hashes of the names give the same key on every rebuild. Row numbers shift when a new market
appears and silently break relationships in reports. A check confirms no two markets share a key.

**Why a date dimension when the fact already has dates?**
Week, month and quarter for time intelligence, and days with no trade still exist, so
charts don't skip gaps.

**Why median for state and national prices?**
One market in the wrong unit barely moves a median but moves an average a lot.

**Why build weekly and monthly tables if Power BI can aggregate?**
Speed (thousands of rows instead of tens of millions) and correctness: a median can't be
built from medians, so Power BI can't roll daily medians into a correct monthly one.

**What does `GROUPING SETS` do?**
Computes several groupings in one pass: by state and for All India. `grouping(state) = 1`
marks the national rows.

**Why not weight prices by volume?**
The dataset has no arrival quantities, so every market-day counts equally. Stated openly.

**Why is `days_covered` in the weekly table?**
So a partial week (the first and last in the data) isn't read as a full one.

**How is gold checked?**
All silver reports in the fact table; no key clashes; no duplicate grain; every fact
linked to its market and date. Any failure stops the run.

---

## Orchestration

**Why a Databricks job and not running notebooks by hand?**
Runs daily without anyone, in order, stops on failure, emails an alert, and keeps a run
history as evidence.

**Why three tasks instead of one big notebook?**
You see exactly which layer failed, and a failed run can be repaired from that task
without redoing the ones that succeeded.

**How does the job stop bad data reaching the dashboard?**
Each layer ends with a check that raises an error. Each task runs only if the previous
succeeded. The dashboard keeps yesterday's correct data instead of today's wrong data.

**Why 1 retry, not 3? Why turn off auto-optimisation?**
A retry helps with temporary platform failures. A failed check fails again identically,
so more retries only delay the alert. Auto-optimisation can add up to 3 hidden retries;
turning it off keeps behaviour predictable.

**Is it safe to retry?**
Yes, every task is idempotent: Auto Loader skips loaded files, silver and gold rebuild in full.

**Why fetch notebooks from GitHub on every run?**
The code that runs is exactly the committed code: no manual sync, no drift.

**Why 07:00?**
The laptop downloads from 21:00 and long uploads can run late. Morning leaves a safe gap,
and a run with nothing new is cheap.

**Why not use "Run backfill"?**
That runs the job once per date for jobs that process one date at a time. These notebooks
load whatever is new and rebuild from everything, so one run covers every date.

**Why is the job definition in the repository?**
If the job is deleted, or needed in production, the YAML recreates it exactly. It's
infrastructure as code; the next step is Databricks Asset Bundles.

---

## Git and code

**What's a Databricks Git folder?**
A clone of the GitHub repository inside Databricks. Push from the laptop, pull in
Databricks: one source of truth instead of copies drifting apart.

**Your push was rejected. What did you do?**
Someone (me, on GitHub's website) had changed the remote. Fetch, inspect, pull, push.
Never force-push: it can delete other people's work.

**Why `.gitattributes`?**
Consistent line endings across Windows and Linux, so diffs show real changes, not line-ending noise.

**What's not in the repository, and why?**
Data, logs, secrets and local settings, via `.gitignore`. Data is too large and
reproducible from the source; secrets must never be in Git history.

---

## Security

**Where are the secrets?**
The data.gov.in key in a Windows user environment variable; the Databricks token in
`.databrickscfg` outside the project. Never in code, notebooks, logs or GitHub.

**What happened when a secret was exposed?**
It was rotated (a new one issued, the old one revoked) first. Hiding an exposed secret
isn't enough; it must be made useless.

**How do logs avoid leaking the key?**
Every error message passes through a redaction function before it is printed or logged.

**Why least-privilege token scopes?**
If a token leaks, it can only do what it was scoped for. The GitHub link for Databricks
is limited to this one repository.

**Why was the alert email replaced with a variable in the job YAML?**
The repository is public; personal addresses in public files attract spam scrapers.

---

## Testing

**What do the tests cover?**
26 tests, each reproducing a failure met against the real API: the User-Agent, refused
keys, rate limits, internet outages, slow downloads, key redaction, empty answers, unknown
state names, duplicates, partly written files, late uploads, and restarting without re-downloading.

**How do you test without calling the API?**
A fake client answers like the API, including its failures (empty answers, duplicates), so
tests are fast, repeatable and need no network.

**How do you test the Spark code?**
Each notebook checks its own output on every run (reconciliation, key and grain checks)
and fails if wrong. Unit tests for Spark logic are a next step.

---

## What I'd do next

- **Recent days as provisional data** so gold reaches yesterday instead of 30 days back.
- **Incremental silver** once run times grow with the full history.
- **Market reference table** for markets published under two names.
- **Expected units per crop** for poultry and flowers, where most markets don't use quintals.
- **Power BI report** on gold, and a public price tracker.
- **Price signals:** best nearby mandi, unusual rises, prices below MSP.
- **CI:** run the tests on every push with GitHub Actions.
