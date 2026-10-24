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

**First rule (replaced):** flag a price more than 10 times above or below the crop's
national median for the month. On version 2 data (25 Aug to 24 Sep) it flagged 2,361
rows. Measuring them showed the rule was catching something else:

- **Thin references were not the cause.** 94% of flags (2,218) were crops sold in 10+
  markets, where the median is reliable; only 143 were crops in fewer than 10 markets.
- **The flags were systematic, not one-offs.** The top 30 market-crop series were flagged
  on *every* day they reported. Ratio times median gives the price actually entered,
  which reveals the unit: Mapusa coconut about Rs 40 (per nut), Pune coriander, spinach
  and mint about Rs 8 (per bunch), Diglipur lemon and Mapusa pineapple Rs 7 to 35 (per
  piece). Patti APMC (Punjab) reports every crop at under Rs 2 per quintal.
- **The majority can be the one in the wrong unit.** Gazipur's "Cock" at about Rs 9,500
  per quintal (Rs 95/kg) is realistic; most other markets report it per bird, so the
  national median itself (Rs 230) is not per quintal.

**Finding:** the source column is labelled "Rs per quintal", but some markets fill it per
piece, per bunch or per kg. These are unit mismatches, not outliers, and they need
different handling.

**Current rules (two flags, both keep the row):**

| Flag | Level | Rule | How gold uses it |
| --- | --- | --- | --- |
| `is_unit_suspect` | Whole market-crop-month series | Series median more than 10x off the typical market's level (median of market medians, so each market counts once); only when 3+ markets sell the crop | Excluded from comparisons across markets; kept for that market's own trend (it is consistent in its own unit) |
| `is_price_outlier` | Single report | More than 10x off its own market's median that month; only when that series has 5+ reports | Excluded everywhere |

`level_ratio` (series median divided by the typical level) is stored on every row so the
size of the difference is visible.

**Why not convert per-bunch or per-piece prices to per quintal?** The weight of a bunch
or a coconut isn't known and varies by market; converting would invent numbers.

**Result on the same 31 days:** outliers fell from 2,361 to **45**; **2,219** rows are
unit-suspect. The suspect series show the expected values: Pune spinach Rs 4 to 9 and
coriander Rs 3 to 6 (per bunch), Mapusa coconut Rs 40 and pineapple Rs 22, Diglipur lemon
Rs 5 (per piece). The remaining outliers are genuine slips inside otherwise normal series,
e.g. Mandya coconut entered per nut on 5 of 21 days, Akbarpur wood on 11 of 31 days.

**Prices under Rs 1 are quarantined.** Patti APMC (Punjab) reported tomato at Rs 0.08,
potato Rs 0.05 and onion Rs 0.14 per quintal, every day. Unlike a per-bunch price, no unit
makes 5 paise a real price, so these rows can't be used even for the market's own trend.
The rule is general (any market, any day), not a hard-coded market name.

**Known limit: the majority can use another unit.** For Gazipur "Cock" (Rs 9,500 per
quintal, realistic) and Gudimalkapur Astera (Rs 12,000, level ratio 800), most other
markets report per bird or per bunch, so the market that is right gets flagged.
<!-- TODO: a reference table of expected units per commodity (poultry, flowers) -->
On the 31 days it quarantined **283** rows (quarantine 112 to 395); unit-suspect rows fell
by the same 283 (2,219 to 1,936): the same rows, moved.

Flagged rows stay in silver (nothing is lost).

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

### Version 2: copies that only appear after cleaning (25 Aug to 24 Sep 2026)

The downloader found no byte-identical records in any day, yet silver removed 2,830
copies. Every one of the 2,830 pairs differs **only in the market name: a trailing
space** (`"APMC Kolhapur"` and `"APMC Kolhapur "`). Measured across all spaced names:

| | Markets | Rows under the spaced name | With an identical twin under the plain name | Alone |
| --- | --- | --- | --- | --- |
| All spaced names (265 Maharashtra, 5 Telangana) | 270 | 36,627 | 2,830 | 33,797 |
| Of which also published under the plain name | 37 | 3,633 | 2,830 | 803 |

- **233 markets** (e.g. APMC Mumbai, APMC Pune) are always published with the space and
  never twinned: untidy text on genuine reports. Trimming just gives them a clean name.
- **37 markets** (e.g. APMC Kolhapur, APMC Jalgaon, Vanasthalipuram RBZ) are published
  under **both** names, and 78% of the spaced rows have an identical twin: the same
  market registered twice in the source. Keeping both would count the market twice and
  double its weight in averages, so silver keeps each report once.
- The twins add up to exactly 2,830, the number silver removed: nothing else is hidden
  in that count.

**Two checks, two questions.** The downloader asks "did the download go wrong?" (byte
copies, e.g. paging overlap). Silver asks "is this the same price report?" (copies after
cleaning). Bronze keeps the original text, so the raw source can always be proved and
silver rebuilt if this rule changes.

**Known limitation:** in the 37 double-registered markets, a report published under both
names *with different prices* stays as two reports. Gold's market-day summary treats
them as two reports of the same market, which is accurate to the source.

### The source changed in mid-2026 (measured by month, 26 Apr to 24 Sep 2026)

| Month | Market names with stray spaces | Copies after cleaning | Quarantined |
| --- | --- | --- | --- |
| May | 0% | 0.001% | 0.009% |
| Jun | 0.42% | 0.087% | 0.012% |
| Jul | 0.32% | 0.067% | 0.013% |
| Aug | **5.78%** | 0.012% | 0.033% |
| Sep (to 24th) | 5.57% | **0.542%** | **0.062%** |

- **Stray spaces began in June and became widespread in August**, mostly Maharashtra
  markets. In August most markets were simply *renamed* with the space (few copies); in
  September many were published under **both** names (copies jumped about 45 times).
- **Quarantine rates rose about 5 times from July to September**, mainly prices under Rs 1.
- **Why trimming matters beyond copies:** without it, "APMC Mumbai" would become a new
  market in August and every trend line for it would break in two. Silver's cleaning
  keeps each market's history continuous across the source's change.
- **Takeaway:** source quality drifts. These per-month rates are worth tracking as a
  monitor, so the next change is noticed in days, not found months later.
<!-- TODO: add per-month quality rates to ops.silver_quality_log or a monitoring view -->

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
