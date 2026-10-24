# Data quality: what profiling found, and the rules it led to

Silver's rules were written after measuring bronze, not from assumptions. A rule that
would quarantine a large share of rows is more likely a wrong rule than bad data, and
only measuring shows which.

## Profile of bronze (version 1 data, 4,984,836 rows, 2 Mar to 15 Oct 2026)

| Check | Rows | Share | Silver rule |
| --- | --- | --- | --- |
| Date not readable as dd/MM/yyyy | 0 | 0% | Quarantine (kept as a guard) |
| Record's date differs from its folder's date | 0 | 0% | Quarantine (guard) |
| Price not a number | 0 | 0% | Quarantine (guard) |
| Price zero or negative | 19 | 0.0004% | Quarantine |
| min <= modal <= max broken | 435 | 0.009% | Quarantine |
| State, market or commodity missing | 0 | 0% | Quarantine (guard) |
| Modal price range | Rs 0.01 to Rs 16.5 lakh per quintal | | Outlier flag (below) |

Rules with zero failures are kept on purpose: they cost nothing and catch the first
time the source changes.

## Extreme prices: flag, don't delete

The highest modal prices included cherry at Rs 16.5 lakh per quintal (Rs 16,500/kg),
cashew at Rs 8,400/kg and coriander leaves at Rs 3,500/kg, which are almost certainly
entry errors, while cardamom at Rs 3,500/kg is realistic. A single limit for all crops
cannot separate these.

**Rule:** a price is flagged `is_price_outlier` when the modal price is more than 10
times above or below **that crop's median for that month**. Flagged rows stay in silver
(nothing is lost) and gold leaves them out of averages.

## Duplicates and the grain of the data

Profiling found 82,821 keys (date, state, district, market, commodity, variety, grade)
appearing more than once:

- **32,209 exact copies, all spanning two download pages.** Caused by unstable paging
  in the version 1 download, not by the source. Fixed at the source by downloading
  state by state; see `incidents/2026-10-23-pagination-overlap.md`. Silver still keeps
  exact copies only once, as a guard.
- **50,612 keys with different prices**, including 10,548 within a single page, which
  paging can't explain. Confirmed in version 2 downloads too. **Markets genuinely
  report several prices a day for the same crop, variety and grade** (separate lots or
  sessions).

**Decision:** silver's grain is one row per **price report**, identified by
`report_id`. Gold summarises to one row per market per day (lowest minimum, highest
maximum, average modal, number of reports).

## Names

- **States:** the source uses "Kerala" and "Keralam" (82 and 198 of 228 days),
  "Pondicherry", "Chattisgarh", "NCT of Delhi". Silver maps every spelling to one
  standard name through `silver.ref_state_alias` and keeps the original as `state_raw`.
  "Kerala" is the chosen standard; changing it is one row in the reference table.
- **Markets:** some markets appear under two names with identical prices, e.g.
  "Parimpore APMC" and "Parimpore Srinagar (F&V)", "Munnar APMC" and "Munnar Market".
  Counting both would double-count them. *To do:* a market reference table.
- **Market types in names:** e.g. "Theni(Uzhavar Sandhai)" (Tamil Nadu farmers'
  markets). *To do:* split the type into its own column.

## Reconciliation

Each silver run checks that bronze = silver + quarantine + exact copies removed, and
fails if not. Each run's counts are appended to `ops.silver_quality_log`.

<!-- TODO: add the profile of version 2 data once the backfill is loaded -->
