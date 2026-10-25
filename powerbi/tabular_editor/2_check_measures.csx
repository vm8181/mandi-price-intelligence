// Mandi Price Intelligence: check every measure against the loaded data.
// Run in Tabular Editor 2 connected to Power BI Desktop after the data has loaded.
// Shows (1) measures with errors, (2) table row counts, (3) every measure for Onion, India-wide.

// 1. Measures with DAX errors
var errors = new System.Text.StringBuilder();
foreach (var m in Model.AllMeasures)
    if (!string.IsNullOrEmpty(m.ErrorMessage)) errors.AppendLine(m.Name + ": " + m.ErrorMessage);
if (errors.Length > 0) Output("Measures with errors:\n" + errors.ToString());
else Info("No measure has a DAX error.");

// 2. Row counts
var counts = EvaluateDax(@"EVALUATE ROW(
    ""Date"", COUNTROWS('Date'),
    ""Commodity"", COUNTROWS(Commodity),
    ""Market"", COUNTROWS(Market),
    ""State"", COUNTROWS('State'),
    ""Market Day Price"", COUNTROWS('Market Day Price'),
    ""Weekly Price"", COUNTROWS('Weekly Price'),
    ""Monthly Price"", COUNTROWS('Monthly Price'),
    ""Quality Log"", COUNTROWS('Quality Log'))");
Output(counts);

// 3. Every measure for one crop (change Onion to test another)
var crop = "Onion";
var sb = new System.Text.StringBuilder();
sb.Append("EVALUATE CALCULATETABLE(ROW(");
var first = true;
foreach (var m in Model.Tables["_Measures"].Measures) {
    if (!first) sb.Append(", ");
    sb.Append("\"" + m.Name + "\", " + "[" + m.Name + "] & \"\"");
    first = false;
}
sb.Append("), Commodity[Crop] = \"" + crop + "\")");
var values = EvaluateDax(sb.ToString()) as System.Data.DataTable;
// One row per measure, easier to read than one very wide row
var report = new System.Data.DataTable();
report.Columns.Add("Measure"); report.Columns.Add("Value for " + crop);
foreach (System.Data.DataColumn c in values.Columns)
    report.Rows.Add(c.ColumnName.Trim(new[]{'[', ']'}), values.Rows[0][c].ToString());
Output(report);
