# Incident: paged downloads silently swapped real rows for duplicates

**Found:** 23 Oct 2026, while profiling bronze for the silver layer
**Impact:** In version 1 of the raw data (paged downloads), 94 of 228 days held exact
duplicate rows, from 1 to 1,446 extra copies per day, and the same number of real rows
was missing. Every day's row count still matched the API's total, so the reconciliation
check passed.
**Status:** Resolved in version 2 (state-by-state downloads, `data/raw/mandi_v2`).
Version 1 is kept only for comparison; bronze will be rebuilt from version 2.

## How it was found

| Step | Evidence |
| --- | --- |
| Profile bronze for duplicates on (date, state, district, market, commodity, variety, grade) | 82,821 duplicated keys: 32,209 with identical prices, 50,612 with different prices |
| Where are the identical copies? | **All 32,209 span two different page files; none sit within one page.** Duplicates in the source itself would mostly be neighbours on the same page |
| Which days? | 94 of 228 days, all from 2 May onwards; March and April clean |
| Re-download 1 Apr (a clean day) state by state and compare | Identical: 28,008 rows, nothing missed. Overlap does not happen every day |
| Re-download 1 Oct (634 extra copies) state by state and compare | Version 1 **missed 257 real rows** in the states compared, e.g. Diglipur Vegetable Market (Andaman and Nicobar), Jaggayyapeta and Thiruchanur (Andhra Pradesh) |
| Inspect version 1's pages for 1 Oct | Tamil Nadu rows on **every** page (2,096, 1,877, 2,035, 1,530, 917): the API's order is not by market and commodity as its metadata claims |

## Root cause

Paging by offset ("rows 5,001 to 10,000") only works if the server returns rows in the
same order on every request. data.gov.in serves this dataset from a search index
(`agmarknet_new`) without a stable sort, so the order can differ between requests.
A row near a page boundary could then be returned on two pages while another row was
returned on neither. Each duplicate replaced exactly one missing row, so the total
always matched.

## A second failure found by the fix

The first state-by-state download of 1 Oct came up 10,306 rows short: Tamil Nadu and
Uttar Pradesh answered `"status": "ok"` with **0 rows**. Asked again, Tamil Nadu
returned all 8,676 rows in 13 s, so the empty answer was temporary. Without the check
that states add up to the day's total, the day would have been saved as complete while
missing 44% of its rows.

## Fixes

- **No paging.** Each day is downloaded one state per request, with a limit (50,000)
  no state reaches in a day, so there are no page boundaries. Up to 4 states are
  requested at once.
- **Two completeness checks; a day is finished only if both pass:**
  1. the states add up exactly to the day's own total;
  2. no row appears twice. Pigeonhole: once the total matches, a missing row can
     only hide behind a duplicate.
- **Targeted retry** of states that answered empty, biggest first, stopping as soon
  as the day adds up.
- **Older state spellings** (Puducherry, Chhattisgarh, ...) are tried only when the
  sum falls short.
- Automated tests (`tests/`) reproduce each failure: overlap, empty answers, unknown
  state names, a state too big for one request.

## Lessons

- **A matching count is not proof of completeness.** Check content as well: here,
  duplicates within a day.
- **Never trust "status: ok".** Check that the parts add up to the whole.
- **Offset paging over a source without a stable order is unreliable.** Prefer
  natural partitions (here, state) that each fit in one request.
- **A theory isn't proven until it's tested.** The first comparison (1 Apr) found no
  overlap; only choosing a day the profile flagged (1 Oct) proved it.
- **Pause ingestion when data is known to be wrong.** Downloads were stopped until the
  fix was proven, rather than accumulating more data to redo.
