# Mandi Price Intelligence

An end-to-end data platform on **Databricks** that turns India's official daily mandi
(wholesale market) prices into answers for farmers, buyers and anyone tracking food
prices: where a crop sells best nearby, where it trades below the government's support
price, and which staples are rising unusually fast.

> **Status: in progress.** Extraction, upload and the bronze layer are built and
> verified. Silver (cleaning) is next. Sections marked *(planned)* describe work not
> yet done.

---

## Contents
1. [The problem](#the-problem)
2. [Questions it answers](#questions-it-answers)
3. [Project status](#project-status)
4. [Architecture](#architecture)
5. [The data](#the-data)
6. [How the pipeline works](#how-the-pipeline-works)
7. [Data quality and reliability](#data-quality-and-reliability)
8. [Engineering challenges solved](#engineering-challenges-solved)
9. [Key design decisions](#key-design-decisions)
10. [Results and insights](#results-and-insights)
11. [Repository structure](#repository-structure)
12. [How to run it](#how-to-run-it)
13. [Tests](#tests)
14. [Tech stack](#tech-stack)
15. [Roadmap](#roadmap)
16. [Lessons learned](#lessons-learned)
17. [Data source and licence](#data-source-and-licence)

---

## The problem

The same crop can sell at very different prices in mandis a few hours apart, and most
sellers only know the price at the one they visit. Price spikes in staples such as
onion, tomato and potato reach households before anyone flags them.

The data to see this is public, on data.gov.in, but hard to use: it arrives as one raw
row per market, crop and day (about 17,000 rows a day), with inconsistent names, prices
stored as text, late uploads for weeks after the date, and an API that is slow,
rate-limited and, as this project found, unreliable when paged.

## Questions it answers

| Who | Question | Answered by |
| --- | --- | --- |
| Farmer or producer group | Which mandi near me paid best for my crop this week? | Best mandi finder *(planned)* |
| Trader or retail buyer | Where is supply cheap, and how stable is the price? | Price explorer *(planned)* |
| Policy watcher or journalist | Where is a crop selling below its support price (MSP)? | Support price watch *(planned)* |
| Household or consumer group | Which staples are rising unusually fast in my state? | Public price tracker *(planned)* |
| Data team | Did today's load succeed, and is every day complete? | Status report and checks (built); health dashboard *(planned)* |

## Project status

| Phase | What | Status |
| --- | --- | --- |
| 1. Extraction | Download 10 years of daily prices from data.gov.in, provably complete | Built; 10-year backfill running |
| 1. Automation (laptop) | Nightly download and upload via Windows Task Scheduler, with logs and a status report | Built |
| 2. Upload | Finished days sent to a Databricks volume, each verified | Built |
| 2. Bronze | Auto Loader into a Delta table, reconciled against the source | Built and verified on version 1 data; reloading from version 2 |
| 3. Silver | Types, quality rules, quarantine, de-duplication, standard names | Profiling done; rules next |
| 4. Gold | Star schema; daily, weekly and monthly summaries; price signals | *(planned)* |
| 5. Governance | Access by layer, documentation, lineage | *(planned)* |
| 6. Orchestration and CI/CD | Databricks job for bronze to gold; tests and deployment from GitHub | Tests in CI *(planned)* |
| 7. Reporting | Dashboards, Power BI report, public price tracker | *(planned)* |

## Architecture

<!-- TODO: add docs/architecture.png (exported from the build plan diagram) -->

```
data.gov.in API
      |   (only reachable from a network in India)
      v
Laptop: backfill_history.py  ---- nightly, Windows Task Scheduler ----
      |   one request per state; checks every day is complete
      v
data/raw/mandi_v2/arrival_date=YYYY-MM-DD/commodity=ALL/state_*.json.gz
      |
      v
upload_to_databricks.py  (finished days only, each verified)
      |
      v
Databricks volume: /Volumes/mandi_dev/bronze/raw_files/mandi_v2/
      |   Auto Loader (incremental, checkpointed)
      v
bronze.mandi_prices_raw  -->  silver (planned)  -->  gold (planned)  -->  dashboards (planned)
```

Why the extraction runs on a laptop: data.gov.in refused connections from Google Colab
and from Databricks Free Edition (tested), so the download must run from an Indian
network. See [decision 001](docs/decisions.md).

## The data

| | |
| --- | --- |
| Dataset | *Variety-wise Daily Market Prices Data of Commodity*, Department of Agriculture and Farmers Welfare (AGMARKNET), via data.gov.in |
| Fields | State, District, Market, Commodity, Commodity code, Variety, Grade, Arrival date, Min, Max and Modal price (Rs per quintal) |
| Grain | One price report per market, commodity, variety, grade and day. Some markets report several times a day |
| Volume | About 11,000 to 25,000 rows a day; 10 years is about 45 to 50 million rows |
| Size | About 23 bytes per row compressed on disk; about 1.1 to 1.8 GB for 10 years |
| Scope | All commodities, all states, 23 Oct 2016 to yesterday |

<!-- TODO: add a data dictionary for silver and gold tables (docs/data_dictionary.md) -->

## How the pipeline works

### 1. Extraction ([`mandi_pipeline/`](mandi_pipeline), [`backfill_history.py`](backfill_history.py))
- Each day is downloaded **one state per request**, up to 4 at a time, with no paging.
- Before downloading, each day is **checked first**: if the files on disk already match
  the API's current total, it is skipped; only missing, partial or changed days are
  downloaded again.
- Recent days (30 days) are refreshed at most once every 20 hours to catch late uploads.
- Raw API answers are saved unchanged, gzip-compressed.

### 2. Upload ([`upload_to_databricks.py`](upload_to_databricks.py))
Finished days only; `_DONE` uploaded last; sizes compared after upload; `_UPLOADED`
marker written only when verified.

### 3. Bronze (Databricks notebook)
<!-- TODO: add the notebook to the repository (notebooks/10_bronze_mandi_prices) -->
Auto Loader reads only new files (checkpoint), keeps every value as text plus the
original record JSON, and adds lineage columns (source file, load time).

### 4. Silver *(planned)*
<!-- TODO: types, quality rules, quarantine, de-duplication, name standardisation, late corrections -->

### 5. Gold *(planned)*
<!-- TODO: star schema; daily market prices; weekly and monthly summaries; price signals -->

### 6. Orchestration and CI/CD *(planned)*
<!-- TODO: Databricks job, alerts, GitHub Actions running the tests -->

### 7. Reporting *(planned)*
<!-- TODO: dashboards, Power BI report, public page link -->

## Data quality and reliability

- **A day is complete only if two independent checks pass:** the states add up to the
  day's own total, and no row appears twice. Each check alone was fooled once in testing.
- **Reconciliation in bronze:** loaded rows compared with the source's totals.
- **Idempotent and resumable:** every step can be stopped and rerun without duplicates.
- **Fail fast** on a refused API key; **retries** for server errors; **waits** for rate
  limits and internet outages.
- **Secrets** never in code or logs; setup scripts validate each secret before saving.

<!-- TODO: silver quality rules and their results (docs/data_quality.md) -->

## Engineering challenges solved

| Challenge | What happened | Fix | Write-up |
| --- | --- | --- | --- |
| API ignored Python | Every Python request timed out; browsers worked | Browser-style User-Agent naming the project | [incident](docs/incidents/2026-10-22-datagov-python-timeouts.md) |
| Cloud servers blocked | Colab and Databricks could not reach data.gov.in | Extract on a laptop in India, upload to Databricks | [decision 001](docs/decisions.md) |
| Wrong secret saved | The Databricks token was saved as the data.gov.in key | Setup scripts validate each secret's format | [incident](docs/incidents/2026-10-23-wrong-secret-saved.md) |
| **Silent data loss from paging** | Row counts matched, yet 94 of 228 days had duplicates replacing real rows | State-by-state download, two completeness checks | [incident](docs/incidents/2026-10-23-pagination-overlap.md) |
| Empty "successful" answers | Two states returned "ok" with 0 rows | Sum check catches it; targeted retry | same incident |

## Key design decisions

Full reasoning, with alternatives considered, in [`docs/decisions.md`](docs/decisions.md).

1. Extract from a computer in India, not the cloud
2. Browser-style User-Agent that names the project
3. 10 years of history, stored as compressed raw pages
4. One-time backfill plus a rolling daily update
5. Download one state per request, not in pages
6. A day is complete only when two independent checks pass

## Results and insights

*(planned)*

<!-- TODO: screenshots of dashboards; link to the public price tracker; 3 to 5 headline
insights from the data (e.g. price spread between nearby mandis, seasonal patterns,
below-MSP frequency) -->

## Repository structure

```
mandi_pipeline/          core extraction code: api.py (requests), download.py (a day's logic)
tests/                   automated tests (pytest)
tools/                   one-off diagnostics: compare_download.py, test_user_agent.py, pull_mandi_sample.py
docs/
  decisions.md           design decisions and alternatives
  incidents/             what went wrong, root cause, fix, lessons
backfill_history.py      download command (backfill and daily)
upload_to_databricks.py  upload of finished days to the Databricks volume
daily_run.ps1            nightly run: download, then upload, with logs
setup_scheduled_task.ps1 one-time: save the API key, create the nightly task
setup_databricks_token.ps1  one-time: save and test the Databricks token
status.ps1 / status.py   progress, gaps, failures, last run
requirements.txt         Python packages
```
<!-- TODO: notebooks/ (Databricks), dashboards/, site/ (public page) -->

## How to run it

### Prerequisites
- Windows with Python 3.12+, Git, and the Databricks CLI
- A data.gov.in account and API key
- A Databricks workspace (Free Edition works) with a catalog `mandi_dev`, schemas
  `bronze`, `silver`, `gold`, `ops`, and volumes `bronze.raw_files` and `bronze.checkpoints`

### Setup (once)
```powershell
python -m pip install -r requirements.txt
powershell -ExecutionPolicy Bypass -File .\setup_databricks_token.ps1   # Databricks token, tested
powershell -ExecutionPolicy Bypass -File .\setup_scheduled_task.ps1     # data.gov.in key + nightly task
```

### Daily operation
- Runs automatically every night at 21:00, or at next login if the laptop was off.
- Check progress: `powershell -ExecutionPolicy Bypass -File .\status.ps1`
- Start now: `Start-ScheduledTask -TaskName 'Mandi Price Intelligence - daily run'`

<!-- TODO: Databricks steps (bronze notebook, job) once orchestration is built -->

## Tests

```powershell
python -m pytest -v
```
26 tests, each reproducing a failure met against the real API: browser User-Agent,
refused key, rate limits, internet outages, slow downloads, key redaction, empty
answers, unknown state names, duplicate rows, partly written files, late uploads, and
resuming without re-downloading.

<!-- TODO: GitHub Actions badge once CI runs the tests -->

## Tech stack

| Area | Tools |
| --- | --- |
| Extraction | Python (requests), concurrent requests, gzip |
| Scheduling (laptop) | Windows Task Scheduler, PowerShell |
| Platform | Databricks Free Edition: Unity Catalog, volumes, Auto Loader, Delta Lake, serverless |
| Testing and version control | pytest, Git, GitHub |
| Reporting | Databricks dashboards, Power BI *(planned)* |

## Roadmap

- [x] Extraction with completeness checks, retries and resume
- [x] Nightly automation, status report, incident and decision docs
- [x] Upload with verification; bronze with Auto Loader and reconciliation
- [ ] 10-year backfill complete (running)
- [ ] Silver: types, quality rules, quarantine, de-duplication, standard names
- [ ] Gold: star schema, weekly and monthly summaries, price signals
- [ ] Databricks job with alerts; provisional upload of recent days
- [ ] Dashboards, Power BI report, public price tracker
- [ ] CI: tests on every push

## Lessons learned

- **Matching counts don't prove completeness.** Check content too: duplicates exposed
  rows that a count check could never see.
- **Never trust "status: ok".** Check that the parts add up to the whole.
- **Test a theory before acting on it.** The first comparison found no problem; only a
  day the profile flagged proved it.
- **Check before you download.** Comparing what's on disk with the source makes
  restarts cheap and catches late changes.
- **Fix the process, not the symptom.** Repeated mistakes became validation in the
  setup scripts.

<!-- TODO: add lessons from silver, gold and reporting -->

## Data source and licence

Data: *Variety-wise Daily Market Prices Data of Commodity*, Directorate of Marketing and
Inspection, Department of Agriculture and Farmers Welfare, Government of India, published
on the [Open Government Data Platform India](https://data.gov.in) (sourced from AGMARKNET).
Prices are as reported by mandis and are not trading advice.

<!-- TODO: confirm the dataset's licence on its data.gov.in page and name it here -->

Author: Vikash Mishra
