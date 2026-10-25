// Mandi Price Intelligence: apply the semantic model's measures, relationships and settings.
// Safe to run more than once: existing items are updated, missing ones are created.
// Run in Tabular Editor 2 opened from Power BI Desktop (External Tools), then press Ctrl+S.

var created = 0; var updated = 0;

// ---------- 1. Measures table
var mt = Model.Tables["_Measures"];

// ---------- 2. Measures
System.Action<string, string, string, string, string> upsert = (name, folder, fmt, desc, expr) => {
    Measure m;
    if (mt.Measures.Contains(name)) { m = mt.Measures[name]; updated++; }
    else { m = mt.AddMeasure(name); created++; }
    m.Expression = expr;
    m.DisplayFolder = folder;
    m.FormatString = fmt;
    m.Description = desc;
};

upsert(@"Latest Data Date", @"Freshness", @"dd mmm yyyy", @"Most recent day with prices in the model (yesterday, once the nightly refresh has run).",
    @"CALCULATE ( MAX ( 'Market Day Price'[Date] ), REMOVEFILTERS () )");
upsert(@"Latest Final Date", @"Freshness", @"dd mmm yyyy", @"Most recent day that is final. Days after it are provisional: markets are still reporting.",
    @"CALCULATE ( MAX ( 'Market Day Price'[Date] ), REMOVEFILTERS (), 'Market Day Price'[Data Status] = ""final"" )");
upsert(@"Provisional Days", @"Freshness", @"0", @"Number of recent days that are provisional.",
    @"CALCULATE ( DISTINCTCOUNT ( 'Market Day Price'[Date] ), REMOVEFILTERS (), 'Market Day Price'[Data Status] = ""provisional"" )");
upsert(@"Footer Text", @"Freshness", @"", @"Source and freshness line shown at the bottom of every page.",
    @"""Source: data.gov.in (AGMARKNET daily mandi prices)  ·  Data to ""
    & FORMAT ( [Latest Data Date], ""d mmm yyyy"" )
    & ""  ·  Days after ""
    & FORMAT ( [Latest Final Date], ""d mmm"" )
    & "" are provisional and may still change  ·  Wholesale prices, ₹ per quintal (100 kg)""");
upsert(@"National Price", @"National Price", @"₹#,0", @"All India weekly price: the median of market prices for the crop, Rs per quintal. Use with one crop, or with crop on the legend.",
    @"CALCULATE ( SUM ( 'Weekly Price'[Median Price] ), 'Weekly Price'[State] = ""All India"", REMOVEFILTERS ( 'State' ) )");
upsert(@"National Provisional Days", @"National Price", @"0", @"Provisional days in the week, for the All India figure.",
    @"CALCULATE ( MAX ( 'Weekly Price'[Provisional Days] ), 'Weekly Price'[State] = ""All India"", REMOVEFILTERS ( 'State' ) )");
upsert(@"Price Final Weeks", @"National Price", @"₹#,0", @"All India weekly price for weeks that are final, over the last 12 months (solid line).",
    @"VAR wk = MAX ( 'Weekly Price'[Week Start] )
VAR p = [National Price]
RETURN
    IF ( wk > [Latest Data Date] - 371 && NOT ISBLANK ( p ) && [National Provisional Days] = 0, p )");
upsert(@"Price Provisional Weeks", @"National Price", @"₹#,0", @"All India weekly price for weeks that are still provisional (dashed line). The last final week is included so the two lines join.",
    @"VAR wk = MAX ( 'Weekly Price'[Week Start] )
VAR p = [National Price]
VAR provHere = [National Provisional Days]
VAR provNext =
    CALCULATE (
        MAX ( 'Weekly Price'[Provisional Days] ),
        'Weekly Price'[State] = ""All India"",
        REMOVEFILTERS ( 'State' ),
        REMOVEFILTERS ( 'Date' ),
        REMOVEFILTERS ( 'Weekly Price'[Week Start] ),
        'Weekly Price'[Week Start] = wk + 7
    )
RETURN
    IF ( wk > [Latest Data Date] - 371 && NOT ISBLANK ( p ) && ( provHere > 0 || provNext > 0 ), p )");
upsert(@"Headline Week", @"National Price", @"dd mmm yyyy", @"The week the headline price refers to: the latest week, unless it has fewer than 3 days of prices so far, then the week before.",
    @"VAR latest =
    CALCULATE (
        MAX ( 'Weekly Price'[Week Start] ),
        'Weekly Price'[State] = ""All India"",
        REMOVEFILTERS ( 'State' ),
        REMOVEFILTERS ( 'Date' ),
        REMOVEFILTERS ( 'Weekly Price'[Week Start] )
    )
VAR days =
    CALCULATE (
        MAX ( 'Weekly Price'[Days Covered] ),
        'Weekly Price'[State] = ""All India"",
        REMOVEFILTERS ( 'State' ),
        REMOVEFILTERS ( 'Date' ),
        REMOVEFILTERS ( 'Weekly Price'[Week Start] ),
        'Weekly Price'[Week Start] = latest
    )
RETURN
    IF ( days >= 3, latest, latest - 7 )");
upsert(@"Headline Price", @"National Price", @"₹#,0", @"All India price in the headline week, Rs per quintal.",
    @"VAR w = [Headline Week]
RETURN
    CALCULATE ( [National Price], REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ), 'Weekly Price'[Week Start] = w )");
upsert(@"Headline Price per kg", @"National Price", @"₹#,0.0", @"Headline price per kilogram (a quintal is 100 kg).",
    @"DIVIDE ( [Headline Price], 100 )");
upsert(@"Price Week Before", @"National Price", @"₹#,0", @"All India price in the week before the headline week.",
    @"VAR w = [Headline Week] - 7
RETURN
    CALCULATE ( [National Price], REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ), 'Weekly Price'[Week Start] = w )");
upsert(@"Price 4 Weeks Before", @"National Price", @"₹#,0", @"All India price four weeks before the headline week.",
    @"VAR w = [Headline Week] - 28
RETURN
    CALCULATE ( [National Price], REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ), 'Weekly Price'[Week Start] = w )");
upsert(@"Change vs Week Before", @"National Price", @"+0.0%;-0.0%;0.0%", @"Change in the All India price from the week before.",
    @"VAR b = [Price Week Before] RETURN IF ( NOT ISBLANK ( b ), DIVIDE ( [Headline Price] - b, b ) )");
upsert(@"Change vs 4 Weeks Before", @"National Price", @"+0.0%;-0.0%;0.0%", @"Change in the All India price over four weeks.",
    @"VAR b = [Price 4 Weeks Before] RETURN IF ( NOT ISBLANK ( b ), DIVIDE ( [Headline Price] - b, b ) )");
upsert(@"Price Same Week Last Year", @"National Price", @"₹#,0", @"All India price in the same week a year earlier (52 weeks back). Blank until that history is loaded.",
    @"VAR w = [Headline Week] - 364
RETURN
    CALCULATE ( [National Price], REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ), 'Weekly Price'[Week Start] = w )");
upsert(@"Change vs Last Year", @"National Price", @"+0.0%;-0.0%;0.0%", @"Change from the same week a year earlier.",
    @"VAR b = [Price Same Week Last Year] RETURN IF ( NOT ISBLANK ( b ), DIVIDE ( [Headline Price] - b, b ) )");
upsert(@"Peak Price 12 Weeks", @"National Price", @"₹#,0", @"Highest All India weekly price in the 12 weeks up to the headline week.",
    @"VAR w = [Headline Week]
RETURN
    CALCULATE (
        MAXX ( VALUES ( 'Weekly Price'[Week Start] ), [National Price] ),
        REMOVEFILTERS ( 'Date' ),
        REMOVEFILTERS ( 'Weekly Price'[Week Start] ),
        'Weekly Price'[Week Start] > w - 84 && 'Weekly Price'[Week Start] <= w
    )");
upsert(@"Peak Week 12 Weeks", @"National Price", @"dd mmm yyyy", @"Week of the 12-week high.",
    @"VAR w = [Headline Week]
VAR weeks =
    CALCULATETABLE (
        ADDCOLUMNS ( VALUES ( 'Weekly Price'[Week Start] ), ""@p"", [National Price] ),
        REMOVEFILTERS ( 'Date' ),
        REMOVEFILTERS ( 'Weekly Price'[Week Start] ),
        'Weekly Price'[Week Start] > w - 84 && 'Weekly Price'[Week Start] <= w
    )
RETURN
    MAXX ( TOPN ( 1, weeks, [@p], DESC ), 'Weekly Price'[Week Start] )");
upsert(@"Below 12 Week Peak", @"National Price", @"0.0%", @"How far the headline price is below its 12-week high (0% means it is at the high).",
    @"DIVIDE ( [Headline Price] - [Peak Price 12 Weeks], [Peak Price 12 Weeks] )");
upsert(@"Markets In Headline Week", @"National Price", @"#,0", @"Markets behind the All India figure in the headline week.",
    @"VAR w = [Headline Week]
RETURN
    CALCULATE ( MAX ( 'Weekly Price'[Markets Reporting] ), 'Weekly Price'[State] = ""All India"", REMOVEFILTERS ( 'State' ), REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ), 'Weekly Price'[Week Start] = w )");
upsert(@"Days In Headline Week", @"National Price", @"0", @"Days with prices in the headline week (7 = full week).",
    @"VAR w = [Headline Week]
RETURN
    CALCULATE ( MAX ( 'Weekly Price'[Days Covered] ), 'Weekly Price'[State] = ""All India"", REMOVEFILTERS ( 'State' ), REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ), 'Weekly Price'[Week Start] = w )");
upsert(@"Provisional Days In Headline Week", @"National Price", @"0", @"Provisional days in the headline week.",
    @"VAR w = [Headline Week]
RETURN
    CALCULATE ( MAX ( 'Weekly Price'[Provisional Days] ), 'Weekly Price'[State] = ""All India"", REMOVEFILTERS ( 'State' ), REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ), 'Weekly Price'[Week Start] = w )");
upsert(@"Mover Change 4 Weeks", @"National Price", @"+0.0%;-0.0%;0.0%", @"Four-week change for the movers table: only crops with at least 50 markets in the headline week, so a few markets can't make a crop look volatile.",
    @"IF ( [Markets In Headline Week] >= 50, [Change vs 4 Weeks Before] )");
upsert(@"Price Pulse Title", @"Narrative", @"", @"Dynamic title for the price trend chart.",
    @"VAR crop = SELECTEDVALUE ( Commodity[Crop] )
RETURN
    IF ( ISBLANK ( crop ), ""All India weekly price"", crop & "": All India weekly price, last 12 months"" )");
upsert(@"Crop Summary", @"Narrative", @"", @"A sentence that writes itself: the selected crop's price, its weekly change and its distance from the 12-week high.",
    @"VAR crop = SELECTEDVALUE ( Commodity[Crop] )
VAR price = [Headline Price]
VAR w = [Headline Week]
VAR c = [Change vs Week Before]
VAR bp = [Below 12 Week Peak]
VAR days = [Days In Headline Week]
VAR prov = [Provisional Days In Headline Week]
VAR changeText =
    IF (
        ISBLANK ( c ), """",
        IF ( ABS ( c ) < 0.005, "", about the same as the week before"",
            "", "" & IF ( c > 0, ""up "", ""down "" ) & FORMAT ( ABS ( c ), ""0%"" ) & "" on the week before"" )
    )
VAR peakText =
    IF (
        ISBLANK ( bp ), """",
        IF ( bp > -0.005, "". That is its highest in 12 weeks"",
            "", and "" & FORMAT ( ABS ( bp ), ""0%"" ) & "" below its 12-week high of ₹""
                & FORMAT ( [Peak Price 12 Weeks], ""#,0"" ) & "" (week of "" & FORMAT ( [Peak Week 12 Weeks], ""d mmm"" ) & "")"" )
    )
VAR partialText = IF ( days < 7, "" ("" & days & "" days of prices so far)"", """" )
VAR provText = IF ( prov > 0, "" These recent figures are provisional: some markets are still reporting."", """" )
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( crop ), ""Pick one crop to see what its price is doing."",
        ISBLANK ( price ), crop & "" has no All India price in recent weeks."",
        crop & "" is selling at about ₹"" & FORMAT ( price, ""#,0"" ) & "" a quintal (₹"" & FORMAT ( price / 100, ""#,0.0"" )
            & "" a kg) across India's mandis in the week of "" & FORMAT ( w, ""d mmm"" ) & partialText
            & changeText & peakText & ""."" & provText
    )");
upsert(@"Movers Summary", @"Narrative", @"", @"Which well-traded crop rose and fell most over four weeks.",
    @"VAR crops =
    FILTER (
        ADDCOLUMNS ( ALL ( Commodity[Crop] ), ""@c"", [Mover Change 4 Weeks] ),
        NOT ISBLANK ( [@c] )
    )
VAR up = TOPN ( 1, crops, [@c], DESC )
VAR down = TOPN ( 1, crops, [@c], ASC )
VAR upName = MAXX ( up, Commodity[Crop] )
VAR upVal = MAXX ( up, [@c] )
VAR downName = MAXX ( down, Commodity[Crop] )
VAR downVal = MAXX ( down, [@c] )
RETURN
    IF (
        COUNTROWS ( crops ) = 0, ""Not enough well-traded crops to compare yet."",
        ""Over four weeks, among crops sold in at least 50 markets, "" & upName & "" rose most (""
            & FORMAT ( upVal, ""+0%;-0%"" ) & "") and "" & downName & "" fell most ("" & FORMAT ( downVal, ""+0%;-0%"" ) & "").""
    )");
upsert(@"Period Days", @"Markets", @"0", @"Days in the selected period (7 by default).",
    @"SELECTEDVALUE ( Period[Days], 7 )");
upsert(@"Period Label", @"Markets", @"", @"Selected period, in words.",
    @"LOWER ( SELECTEDVALUE ( Period[Period], ""Last 7 days"" ) )");
upsert(@"Market Price", @"Markets", @"₹#,0", @"Average modal price at a market over the selected period, Rs per quintal. Series priced in another unit and one-off outliers are left out, so markets can be compared.",
    @"VAR d = [Latest Data Date]
VAR n = [Period Days]
RETURN
    CALCULATE (
        AVERAGE ( 'Market Day Price'[Modal Price] ),
        REMOVEFILTERS ( 'Date' ),
        'Market Day Price'[Date] > d - n,
        'Market Day Price'[Date] <= d,
        'Market Day Price'[Is Unit Suspect] = FALSE ()
    )");
upsert(@"Market Reports", @"Markets", @"#,0", @"Price reports behind the market price in the selected period.",
    @"VAR d = [Latest Data Date]
VAR n = [Period Days]
RETURN
    CALCULATE (
        SUM ( 'Market Day Price'[Reports] ),
        REMOVEFILTERS ( 'Date' ),
        'Market Day Price'[Date] > d - n,
        'Market Day Price'[Date] <= d,
        'Market Day Price'[Is Unit Suspect] = FALSE ()
    )");
upsert(@"Market Days Reported", @"Markets", @"0", @"Days in the period on which the market reported the crop.",
    @"VAR d = [Latest Data Date]
VAR n = [Period Days]
RETURN
    CALCULATE (
        DISTINCTCOUNT ( 'Market Day Price'[Date] ),
        REMOVEFILTERS ( 'Date' ),
        'Market Day Price'[Date] > d - n,
        'Market Day Price'[Date] <= d,
        'Market Day Price'[Is Unit Suspect] = FALSE ()
    )");
upsert(@"Markets With Price", @"Markets", @"#,0", @"Markets with a comparable price for the crop in the period.",
    @"COUNTROWS ( FILTER ( VALUES ( Market[market_key] ), NOT ISBLANK ( [Market Price] ) ) )");
upsert(@"Median Market Price", @"Markets", @"₹#,0", @"Median of the market prices in the current selection (state or all India).",
    @"MEDIANX ( FILTER ( VALUES ( Market[market_key] ), NOT ISBLANK ( [Market Price] ) ), [Market Price] )");
upsert(@"Best Market Price", @"Markets", @"₹#,0", @"Highest market price in the selection: the best place to sell. Only markets that reported on at least 2 days of the period count, so one odd report can't top the list.",
    @"MAXX ( FILTER ( VALUES ( Market[market_key] ), [Market Days Reported] >= 2 ), [Market Price] )");
upsert(@"Lowest Market Price", @"Markets", @"₹#,0", @"Lowest market price in the selection: the cheapest place to buy. Only markets that reported on at least 2 days count.",
    @"MINX ( FILTER ( VALUES ( Market[market_key] ), [Market Days Reported] >= 2 && NOT ISBLANK ( [Market Price] ) ), [Market Price] )");
upsert(@"Price Spread", @"Markets", @"₹#,0", @"Gap between the dearest and cheapest market in the selection, Rs per quintal.",
    @"[Best Market Price] - [Lowest Market Price]");
upsert(@"State Median For Market", @"Markets", @"₹#,0", @"Median price across all markets in the same state, for comparing one market with its state.",
    @"VAR s = SELECTEDVALUE ( Market[State] )
RETURN
    CALCULATE ( [Median Market Price], REMOVEFILTERS ( Market ), Market[State] = s )");
upsert(@"vs State Median", @"Markets", @"+0%;-0%;0%", @"How a market's price compares with its state's median.",
    @"VAR m = [State Median For Market] RETURN IF ( NOT ISBLANK ( [Market Price] ), DIVIDE ( [Market Price] - m, m ) )");
upsert(@"Best Market Name", @"Markets", @"", @"Market with the highest price in the selection, among markets that reported on at least 2 days.",
    @"VAR t =
    FILTER (
        ADDCOLUMNS ( SUMMARIZE ( Market, Market[Market], Market[District], Market[State] ), ""@p"", [Market Price], ""@d"", [Market Days Reported] ),
        NOT ISBLANK ( [@p] ) && [@d] >= 2
    )
RETURN
    MAXX ( TOPN ( 1, t, [@p], DESC ), Market[Market] & "" ("" & Market[District] & "", "" & Market[State] & "")"" )");
upsert(@"Finder Summary", @"Markets", @"", @"A sentence that writes itself for the market finder.",
    @"VAR crop = SELECTEDVALUE ( Commodity[Crop] )
VAR place = SELECTEDVALUE ( 'State'[State], ""India"" )
VAR n = [Markets With Price]
VAR best = [Best Market Price]
VAR med = [Median Market Price]
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( crop ), ""Pick one crop to find where it sells best."",
        ISBLANK ( n ) || n = 0, ""No comparable "" & crop & "" prices in "" & place & "" in the "" & [Period Label] & ""."",
        n & IF ( n = 1, "" market"", "" markets"" ) & "" in "" & place & "" reported "" & crop & "" prices in the "" & [Period Label]
            & "". The best price was ₹"" & FORMAT ( best, ""#,0"" ) & "" a quintal at "" & [Best Market Name]
            & "", "" & FORMAT ( DIVIDE ( best - med, med ), ""0%"" ) & "" above the median of ₹"" & FORMAT ( med, ""#,0"" )
            & "". Between the cheapest and dearest market, the gap was ₹"" & FORMAT ( [Price Spread], ""#,0"" ) & "" a quintal.""
    )");
upsert(@"State Price", @"States", @"₹#,0", @"Median of market prices in a state over the selected period, Rs per quintal.",
    @"[Median Market Price]");
upsert(@"All India Market Median", @"States", @"₹#,0", @"Median of all market prices in India over the selected period.",
    @"CALCULATE ( [Median Market Price], REMOVEFILTERS ( 'State' ), REMOVEFILTERS ( Market ) )");
upsert(@"State vs India", @"States", @"+0%;-0%;0%", @"How a state's price compares with the all-India market median.",
    @"VAR i = [All India Market Median] RETURN IF ( NOT ISBLANK ( [State Price] ), DIVIDE ( [State Price] - i, i ) )");
upsert(@"State Weekly Price", @"States", @"₹#,0", @"A state's weekly price (median of its markets) for the last 12 weeks, for the heat grid.",
    @"VAR wk = MAX ( 'Weekly Price'[Week Start] )
RETURN
    IF ( wk > [Latest Data Date] - 84, SUM ( 'Weekly Price'[Median Price] ) )");
upsert(@"States Summary", @"States", @"", @"Cheapest and dearest state for the selected crop.",
    @"VAR crop = SELECTEDVALUE ( Commodity[Crop] )
VAR t =
    FILTER (
        ADDCOLUMNS ( VALUES ( 'State'[State] ), ""@p"", [State Price], ""@n"", [Markets With Price] ),
        NOT ISBLANK ( [@p] ) && [@n] >= 3
    )
VAR lo = TOPN ( 1, t, [@p], ASC )
VAR hi = TOPN ( 1, t, [@p], DESC )
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( crop ), ""Pick one crop to compare states."",
        COUNTROWS ( t ) < 2, ""Too few states with at least 3 reporting markets to compare "" & crop & ""."",
        crop & "" over the "" & [Period Label] & "": cheapest in "" & MAXX ( lo, 'State'[State] ) & "" (₹""
            & FORMAT ( MAXX ( lo, [@p] ), ""#,0"" ) & ""), dearest in "" & MAXX ( hi, 'State'[State] ) & "" (₹""
            & FORMAT ( MAXX ( hi, [@p] ), ""#,0"" ) & ""), across "" & COUNTROWS ( t )
            & "" states with at least 3 reporting markets.""
    )");
upsert(@"Season Price", @"Seasons", @"₹#,0", @"All India weekly price, for comparing the same week across years.",
    @"[National Price]");
upsert(@"Years Available", @"Seasons", @"0.0", @"Years of All India weekly prices loaded for the selected crop (weeks divided by 52).",
    @"CALCULATE (
    DIVIDE ( COUNTROWS ( DISTINCT ( SELECTCOLUMNS ( 'Weekly Price', ""@w"", 'Weekly Price'[Week Start] ) ) ), 52 ),
    'Weekly Price'[State] = ""All India"",
    REMOVEFILTERS ( 'State' ),
    REMOVEFILTERS ( 'Date' ),
    REMOVEFILTERS ( 'Weekly Price'[Week Start] )
)");
upsert(@"History Start", @"Seasons", @"dd mmm yyyy", @"First week with an All India price for the selected crop.",
    @"CALCULATE ( MIN ( 'Weekly Price'[Week Start] ), 'Weekly Price'[State] = ""All India"", REMOVEFILTERS ( 'State' ), REMOVEFILTERS ( 'Date' ), REMOVEFILTERS ( 'Weekly Price'[Week Start] ) )");
upsert(@"Seasons Summary", @"Seasons", @"", @"Explains how much history the seasons chart has.",
    @"VAR crop = SELECTEDVALUE ( Commodity[Crop] )
VAR yrs = [Years Available]
VAR ly = [Change vs Last Year]
RETURN
    SWITCH (
        TRUE (),
        ISBLANK ( crop ), ""Pick one crop to compare its seasons."",
        crop & "" has All India weekly prices from the week of "" & FORMAT ( [History Start], ""d mmm yyyy"" )
            & IF ( yrs < 1, "" (about "" & ROUND ( yrs * 12, 0 ) & "" months so far). "",
                  "" (about "" & FORMAT ( yrs, ""0.0"" ) & "" years so far). "" )
            & IF (
                ISBLANK ( ly ),
                ""Same-week comparisons with last year appear once a full year of history has loaded."",
                ""This week's price is "" & FORMAT ( ABS ( ly ), ""0%"" ) & IF ( ly >= 0, "" higher"", "" lower"" )
                    & "" than in the same week last year.""
            )
    )");
upsert(@"Last Silver Run", @"Data Health", @"dd mmm yyyy hh:nn", @"When the cleaning step (silver) last ran, Indian time.",
    @"MAX ( 'Quality Log'[Run At (IST)] )");
upsert(@"Latest Rows Used", @"Data Health", @"#,0", @"Rows the latest silver run used from bronze.",
    @"VAR r = MAX ( 'Quality Log'[Run At (UTC)] ) RETURN CALCULATE ( SUM ( 'Quality Log'[Rows Used] ), 'Quality Log'[Run At (UTC)] = r )");
upsert(@"Latest Silver Rows", @"Data Health", @"#,0", @"Rows that passed every rule in the latest run.",
    @"VAR r = MAX ( 'Quality Log'[Run At (UTC)] ) RETURN CALCULATE ( SUM ( 'Quality Log'[Silver Rows] ), 'Quality Log'[Run At (UTC)] = r )");
upsert(@"Latest Quarantine Rate", @"Data Health", @"0.000%", @"Share of rows quarantined in the latest run.",
    @"VAR r = MAX ( 'Quality Log'[Run At (UTC)] ) RETURN CALCULATE ( MAX ( 'Quality Log'[Quarantine Rate] ), 'Quality Log'[Run At (UTC)] = r )");
upsert(@"Latest Unit Suspect Rate", @"Data Health", @"0.00%", @"Share of rows in series priced in another unit, latest run.",
    @"VAR r = MAX ( 'Quality Log'[Run At (UTC)] ) RETURN CALCULATE ( MAX ( 'Quality Log'[Unit Suspect Rate] ), 'Quality Log'[Run At (UTC)] = r )");
upsert(@"Latest Outlier Rate", @"Data Health", @"0.000%", @"Share of rows flagged as one-off outliers, latest run.",
    @"VAR r = MAX ( 'Quality Log'[Run At (UTC)] ) RETURN CALCULATE ( MAX ( 'Quality Log'[Outlier Rate] ), 'Quality Log'[Run At (UTC)] = r )");
upsert(@"Latest Copies Rate", @"Data Health", @"0.000%", @"Share of rows removed as copies after cleaning, latest run.",
    @"VAR r = MAX ( 'Quality Log'[Run At (UTC)] ) RETURN CALCULATE ( MAX ( 'Quality Log'[Copies Rate] ), 'Quality Log'[Run At (UTC)] = r )");
upsert(@"Markets Reporting", @"Data Health", @"#,0", @"Markets with at least one price.",
    @"DISTINCTCOUNT ( 'Market Day Price'[market_key] )");
upsert(@"Markets On Latest Day", @"Data Health", @"#,0", @"Markets with prices on the latest day. Low at first, because markets keep reporting for days.",
    @"VAR d = [Latest Data Date] RETURN CALCULATE ( DISTINCTCOUNT ( 'Market Day Price'[market_key] ), REMOVEFILTERS (), 'Market Day Price'[Date] = d )");
upsert(@"Typical Markets Per Day", @"Data Health", @"#,0", @"Median markets reporting per final day over the last 60 days: what a complete day looks like.",
    @"VAR f = [Latest Final Date]
RETURN
    CALCULATE (
        MEDIANX ( VALUES ( 'Market Day Price'[Date] ), CALCULATE ( DISTINCTCOUNT ( 'Market Day Price'[market_key] ) ) ),
        REMOVEFILTERS (),
        'Market Day Price'[Date] > f - 60,
        'Market Day Price'[Date] <= f
    )");
upsert(@"Markets Reporting Last 60 Days", @"Data Health", @"#,0", @"Markets reporting each day, last 60 days: the dip at the end shows reports still arriving.",
    @"VAR d = MAX ( 'Date'[Date] )
RETURN
    IF ( d > [Latest Data Date] - 60 && d <= [Latest Data Date], [Markets Reporting] )");
upsert(@"Reconciliation Status", @"Data Health", @"", @"Every silver run is only logged after its reconciliation passes, so a logged run is a passed run.",
    @"VAR runs = COUNTROWS ( 'Quality Log' )
RETURN
    IF ( runs > 0, ""✅  Passed on all "" & runs & "" logged runs: every row used is accounted for"", ""No runs logged yet"" )");
upsert(@"Quarantine Status", @"Data Health", @"", @"Traffic light for the quarantine rate.",
    @"IF ( [Latest Quarantine Rate] <= 0.001, ""✅  Quarantine under 0.1%: normal"", ""⚠️  Quarantine above 0.1%: check the source"" )");
upsert(@"Freshness Status", @"Data Health", @"", @"Traffic light for how recent the data is.",
    @"VAR age = TODAY () - [Latest Data Date]
RETURN
    IF ( age <= 2, ""✅  Up to date: latest prices from "" & FORMAT ( [Latest Data Date], ""d mmm"" ),
        ""⚠️  Latest prices are "" & age & "" days old: check the nightly run"" )");
upsert(@"Mover Price", @"Movers", @"₹#,0", @"Headline price, shown only for crops sold in at least 50 markets that week.",
    @"IF ( [Markets In Headline Week] >= 50, [Headline Price] )");
upsert(@"Mover Change Week", @"Movers", @"+0.0%;-0.0%;0.0%", @"Weekly change, shown only for crops sold in at least 50 markets.",
    @"IF ( [Markets In Headline Week] >= 50, [Change vs Week Before] )");
upsert(@"Mover Markets", @"Movers", @"#,0", @"Markets behind the figure, shown only for crops sold in at least 50 markets.",
    @"IF ( [Markets In Headline Week] >= 50, [Markets In Headline Week] )");
upsert(@"National Monthly Price", @"National Price", @"₹#,0", @"All India monthly price: the median of market prices, Rs per quintal.",
    @"CALCULATE ( SUM ( 'Monthly Price'[Median Price] ), 'Monthly Price'[State] = ""All India"", REMOVEFILTERS ( 'State' ) )");
upsert(@"Run Quarantine Rate", @"Data Health", @"0.000%", @"Quarantine rate of a silver run.",
    @"MAX ( 'Quality Log'[Quarantine Rate] )");
upsert(@"Run Outlier Rate", @"Data Health", @"0.000%", @"Outlier rate of a silver run.",
    @"MAX ( 'Quality Log'[Outlier Rate] )");
upsert(@"Run Copies Rate", @"Data Health", @"0.000%", @"Copies-after-cleaning rate of a silver run.",
    @"MAX ( 'Quality Log'[Copies Rate] )");

// ---------- 3. Relationships (many-to-one, single direction)
System.Action<string, string, string, string> rel = (ft, fc, tt, tc) => {
    var from = Model.Tables[ft].Columns[fc]; var to = Model.Tables[tt].Columns[tc];
    foreach (var r in Model.Relationships)
        if (r.FromColumn == from && r.ToColumn == to) return;
    var nr = Model.AddRelationship(); nr.FromColumn = from; nr.ToColumn = to;
};
rel(@"Market Day Price", @"Date", @"Date", @"Date");
rel(@"Market Day Price", @"market_key", @"Market", @"market_key");
rel(@"Market Day Price", @"commodity_key", @"Commodity", @"commodity_key");
rel(@"Market", @"State", @"State", @"State");
rel(@"Weekly Price", @"Week Start", @"Date", @"Date");
rel(@"Weekly Price", @"commodity_key", @"Commodity", @"commodity_key");
rel(@"Weekly Price", @"State", @"State", @"State");
rel(@"Monthly Price", @"Month Start", @"Date", @"Date");
rel(@"Monthly Price", @"commodity_key", @"Commodity", @"commodity_key");
rel(@"Monthly Price", @"State", @"State", @"State");

// ---------- 4. Date table, sorting, hidden keys
Model.Tables["Date"].DataCategory = "Time";
Model.Tables["Date"].Columns["Date"].IsKey = true;
Model.Tables["Date"].Columns["Month"].SortByColumn = Model.Tables["Date"].Columns["Month Number"];
Model.Tables["Period"].Columns["Period"].SortByColumn = Model.Tables["Period"].Columns["Days"];
foreach (var t in Model.Tables)
    foreach (var c in t.Columns)
        if (c.Name.EndsWith("_key") || c.Name == "Dummy") c.IsHidden = true;

Info("Measures created: " + created + ", updated: " + updated + ". Press Ctrl+S to save to Power BI Desktop.");
