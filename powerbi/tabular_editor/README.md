# Tabular Editor 2 scripts

Run these from **Tabular Editor 2 opened through Power BI Desktop** (External Tools ribbon),
in the **C# Script** tab: open the file (or paste it), then press **F5**.

| Script | What it does | Changes the model? |
| --- | --- | --- |
| `1_apply_model.csx` | Creates or updates every measure (expression, format, folder, description), the relationships, the date table setting, sort orders and hidden keys. Safe to run again. | Yes: press **Ctrl+S** afterwards to save into Power BI Desktop |
| `2_check_measures.csx` | Lists measures with DAX errors, counts the rows in each table, and shows every measure's value for one crop | No |

Both scripts are generated from the same definitions as the TMDL files, so they never
disagree with the model in this folder.
