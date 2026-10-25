# Images

Screenshots and diagrams used in the main README. Save each as PNG with exactly the file
name below, so the README picks it up.

| File | What to capture | Status |
| --- | --- | --- |
| `architecture.png` | Pipeline diagram (generated from `architecture.mmd` with Mermaid) | ✅ |
| `job_run.png` | Databricks job run: bronze → silver → gold, all green | ✅ |
| `unity_catalog.png` | Catalog Explorer: `mandi_dev` expanded, showing the bronze, silver, gold and ops schemas and their tables | ✅ |
| `silver_reconciliation.png` | Silver notebook: the counts line and "Reconciliation passed", plus the "What failed" table | ✅ |
| `silver_unit_suspect.png` | Silver notebook: the "Series in a different unit" table | ✅ |
| `gold_checks.png` | Gold notebook: the checks line, "Gold checks passed" and the row-count summary | ✅ |
| `gold_weekly_prices.png` | Gold notebook: All India weekly onion, tomato and potato, shown as a line chart | ✅ |
| `status_report.png` | Laptop: output of `status.ps1` | ✅ |
| `dashboard_start_here.png` | Power BI page: Start here (Crop = Onion, Period = Last 7 days) | ✅ |
| `dashboard_price_pulse.png` | Power BI page: Price pulse (Crop = Onion, Period = Last 7 days) | ✅ |
| `dashboard_cheapest_states.png` | Power BI page: Cheapest states (Crop = Onion, Period = Last 7 days) | ✅ |
| `dashboard_market_finder.png` | Power BI page: Market finder (Crop = Onion, Period = Last 7 days) | ✅ |
| `dashboard_market_detail.png` | Power BI page: Market detail, opened by drillthrough from a market (Crop = Onion, Period = Last 7 days) | ✅ |
| `dashboard_seasons.png` | Power BI page: Seasons (Crop = Onion, Period = Last 7 days) | ✅ |
| `dashboard_data_health.png` | Power BI page: Data health (Crop = Onion, Period = Last 7 days) | ✅ |

Dashboard pages are exported with File > Export > Export to PDF, then converted to PNG (1600 px wide).

**Before saving a screenshot, check it shows no:** email address, API key, token, or
workspace URL. Crop or blur them out.
