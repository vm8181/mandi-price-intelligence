# Power BI dashboard

`MandiPriceIntelligence.pbip` is a **Power BI Project**: the data model is stored as TMDL
text files and the report as PBIR JSON (one file per page and visual), so every measure
and visual is version-controlled and reviewable like the rest of the code.

## Pages

| Page | Question it answers |
| --- | --- |
| **Start here** | What is this, where does the data come from, and what do the words mean? |
| **Price pulse** | How is a crop's All India price moving, and which crops rose or fell most? |
| **Cheapest states** | Which states pay the most and least, and how did that change over 12 weeks? |
| **Market finder** | Which mandis paid the best price for a crop, in a state or across India? Filters for variety and grade compare like with like. |
| **Market detail** (drillthrough) | What did one mandi pay for each variety and grade, and does it pay more than its state day after day? Right-click a market on Market finder > Drill through. |
| **Seasons** | Is today's price normal for the time of year? |
| **Data health** | How fresh and complete is the data, and what did the quality checks find? |

Every page has a title and subtitle saying what it shows, a **sentence that writes itself**
from the data (for example the crop's price, its weekly change and its distance from the
12-week high), a **"How to read this page"** panel, and a footer with the source and how
recent the data is. Provisional weeks are shown dashed in amber.

## Data model

A star schema over the gold tables in Databricks (`mandi_dev.gold`), plus the silver
quality log (`mandi_dev.ops.silver_quality_log`):

| Table | Grain | Loaded |
| --- | --- | --- |
| `Date`, `Commodity`, `Market`, `State` | dimensions | in full |
| `Market Day Price` | day × market × crop × variety × grade | **last 15 months** (parameter `FactMonths`) |
| `Weekly Price`, `Monthly Price` | period × crop × state, plus All India | full history |
| `Quality Log` | one row per silver run | in full |
| `Period` | last 7 or 30 days, for the market and state pages | calculated |

86 measures, each with a description, are grouped in display folders on the
`_Measures` table.

**Why only 15 months of the daily table?** The market and state pages use the last 7 or 30
days; trends and seasons use the weekly and monthly tables, which are small and hold the
full history. Loading 10 years of daily rows would make the file large and slow for no
gain. With a Power BI Pro licence, incremental refresh (store 10 years, refresh the last
2 months) would be the next step.

**Why no map?** Power BI's older map visuals are being retired and the newer one needs an
online sign-in, so a map could break when opened elsewhere. The states page uses a ranked
bar chart and a state-by-week heat grid instead.

## Open it

1. Open `MandiPriceIntelligence.pbip` in Power BI Desktop.
2. **Transform data → Edit parameters**: set `DatabricksHost` and `DatabricksHttpPath` from
   Databricks (**SQL Warehouses → your warehouse → Connection details**).
3. **Refresh**. When asked, sign in with a Databricks **personal access token**. Power BI
   keeps it in its own credential store; it is never written to these files.

The repository holds placeholder values for the two parameters, so the workspace address
is not published. After setting your own, keep them out of commits with:

```powershell
git update-index --skip-worktree powerbi/MandiPriceIntelligence.SemanticModel/definition/expressions.tmdl
```

## Checked on every push

Because PBIP stores the report as text, CI checks it without opening Power BI
([`tools/check_powerbi.py`](../tools/check_powerbi.py)):

- **Structure:** every page and visual file is valid against Microsoft's published schema.
- **Formatting names:** every setting exists for its visual type (Desktop silently drops unknown ones).
- **Fields:** every column and measure a visual uses exists in the model.
- **Layout:** no visual overlaps another or runs off the page.

Run it locally the same way CI does: see the workflow in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).
