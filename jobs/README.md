# Jobs

`mandi_daily.yml` defines the daily Databricks job that runs the three notebooks in order:

```
bronze  ->  silver  ->  gold        daily at 07:00 IST, serverless
```

| Setting | Value | Why |
| --- | --- | --- |
| Code source | GitHub, branch `main` | Every run uses exactly the committed code; no manual sync |
| Dependencies | each task runs only if the previous one succeeded | A failed check stops bad data before it reaches gold |
| Retries | 1, after 5 minutes; no hidden auto-optimisation retries | Covers temporary platform failures; a failed check fails again, so more retries only delay the alert |
| Alerts | email on failure only | A daily success email gets ignored |
| Concurrency | 1 run at a time (default), extra runs queue | Two runs never rebuild silver at once |
| Parameter | `catalog = mandi_dev` | Point the same job at `mandi_prod` by changing one value |
| Schedule | 07:00 IST (paused now that the dataset is final) | The laptop downloads from 21:00 and uploads every 2 hours; the morning run picks up the night's data |

All three tasks are **idempotent**: bronze's Auto Loader skips files it has already loaded,
and silver and gold replace whole months, so a retry or a repeat run never creates duplicates.

## Recreate the job

1. Databricks: **Jobs & Pipelines -> Create -> Job**, then **Edit as YAML** (or **View as code -> Edit**).
2. Paste the contents of `mandi_daily.yml`.
3. Replace `${var.alert_email}` with the address that should receive failure alerts.
4. Save, then **Run now** to test.

## Deploy it as code (Databricks Asset Bundle)

`mandi_daily.yml` is in the Asset Bundles format, and [`databricks.yml`](../databricks.yml) at the
repository root turns it into a bundle. From the project folder, with the Databricks CLI signed in
(`databricks auth login`):

```powershell
databricks bundle validate
# First time only: link the bundle to the job that already exists, so deploy updates it instead of creating a copy
databricks bundle deployment bind mandi_daily <job id from the job's URL>
databricks bundle deploy --var alert_email=you@example.com
```

CI checks this file on every push without signing in ([`tools/check_bundle.py`](../tools/check_bundle.py)):
it validates against the official bundle schema, confirms each notebook exists and that tasks run
bronze → silver → gold. Deployment stays on the owner's computer, so no Databricks token is stored in
GitHub (decision 013).
