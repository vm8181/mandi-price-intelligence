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
| Schedule | 07:00 IST | The laptop downloads and uploads from 21:00; the morning leaves a safe gap |

All three tasks are **idempotent**: bronze's Auto Loader skips files it has already loaded,
and silver and gold rebuild in full, so a retry or a repeat run never creates duplicates.

## Recreate the job

1. Databricks: **Jobs & Pipelines -> Create -> Job**, then **Edit as YAML** (or **View as code -> Edit**).
2. Paste the contents of `mandi_daily.yml`.
3. Replace `${var.alert_email}` with the address that should receive failure alerts.
4. Save, then **Run now** to test.

The `resources:` layout is the Databricks Asset Bundles format, so this file can later be
deployed from the command line (`databricks bundle deploy`) with a `databricks.yml` that
defines the `alert_email` variable.
