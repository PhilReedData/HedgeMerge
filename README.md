# HedgeMerge
Merge hedge funds data from Eurekahedge and TASS into a single database. Uses Python and SQLite.

## Steps
0. Define tables for Return, AUM, Characteristics
1. Create sub-table of Characteristics with standardized names etc.
2. Merge the fund IDs, make a new ID.
3. Look for duplicates, for each new ID, for each currency.
4. Decide which duplicates to keep.

## Step 0 - load big files
1. Prepare by saving the Eureka data from Excel as several tab-delimited text files, then fill in the paths file. Unzip the TASS CSV files.
2. Create the database file HedgeMerge.db in SQLite with empty tables.
3. Load the TASS files into the database. For companies, use "Management Firm" lines only.
4. Load the Eurekahedge files into the database.

The tables will have these headings. The pair `Source, SourceFundID` is the unique primary key in each.
### Returns
`Source, SourceFundID, 1994-01, 1994-02, ..., 2016-12`
...where Source is 'T'|'E', SourceFundID is the fund ID given by the Source, and the remaining columns are Return on the given month.

### AUM
`Source, SourceFundID, 1994-01, 1994-02, ..., 2016-12`
...where Source is 'T'|'E', SourceFundID is the fund ID given by the Source, and the remaining columns are AUM on the given month.

### TASSCharacteristics
`Source, SourceFundID, T_Dead, [All TASS fields]`
...where Source is 'T', SourceFundID is the fund ID given by the Source, T_Dead is '1' for the funds given in the dead files (filenames end with '3'), and the remaining columns are the values for the fields in TASS. Used while the data is being imported.

### EurekeaCharacteristics
`Source, SourceFundID, [All Eureka fields]`
...where Source is 'E', SourceFundID is the fund ID given by the Source, and the remaining columns are the values for the fields in Eureka. Used while the data is being imported.

#### MergedCharacteristics
We will be developing this table as we go along. It starts as a reduced version of the EurekaCharacteristics and TASSCharacteristics tables combined:
`Source, SourceFundID, FundName, Currency, CompanyName, CompanyID, ManagementFee, IncentiveFee, LockUp, Notice, HWM, Leverage, MinimumInvestment, RedemptionFrequency, SubscriptionFrequency, Strategy, Domicile, Closed, Liquidated`

We will fill in the remaining coloumns later:
`StdCompanyName, MergedFundID, LongestHist`
...where MergedFundID is not unique, and LongestHist is 1 or 0.


## Step 1 - refine characteristics
1. Create MergedCharacteristics table. Keep company name, currency and fund name (until now, different field names for each source) as well as primary keys of Source and SourceFundID and a selection of others. Read from both EurkeaCharacteristics and TASSCharacteristics to populate this table.

2. Create standardized names for companies as a new column. Use method from old inherited code. List of company suffixes and country suffixes can be checked at the top of this script.

## Step 2 - merge fund IDs
1. The `StdCompanyName` field acts as an identifier for where a company is known to be the same across both funds. No need to create a numerical 'duplicate' field for company ID (unless performance becomes an issue, since integer IDs are faster than string IDs). Not ready to populate a merged fund ID yet. No action necessary here.

## Step 3 - look for duplicates, for each StdCompanyName, for each Currency
(_I am up to here so far with the code!_)

1. Find and report duplicate funds to MergedCharacteristics.

## Step 4 - determine longest history for each merged fund
1. Populate the LongestHist field for each group of unique MergedFundID


## Data
Data is stored in an SQLite database. It could be exported to flat text, easiest with the [command line SQLite3 tools](https://sqlite.org/download.html) and Windows batch scripts.

