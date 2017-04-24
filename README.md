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
5. Create the View of both characteristics tables together.

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

### SourceCharactaristics
`Source, SourceFundID, [All TASS fields], [All Eurkea fields]`
...where Source is 'T'|'E', SourceFundID is the fund ID given by the Source, and the remaining columns are the values for the fields in TASS or in Eureka. Apart from the first two columns, the table will be half empty (no TASS fields for Eureka funds, and vice versa). Formed as a View of a full outer join of  `TASSCharacteristics` and `EurekeaCharacteristics` (strictly, a union of two left outer joins, since SQLite does not have the full outer join function).

#### MergedCharacteristics
We will be developing this table as we go along. It starts as a reduced version of the SourceCharacteristics:
`Source, SourceFundID, FundName, Currency, CompanyName, CompanyID, ManagementFee, IncentiveFee, LockUp, Notice, HWM, Leverage, MinimumInvestment, RedemptionFrequency, SubscriptionFrequency, Strategy, Domicile, Closed, Liquidated`

We will fill in the remaining coloumns later:
`StdCompanyName, MergedFundID, LongestHist`
...where MergedFundID is not unique, and LongestHist is 1 or 0.

(_I am up to here so far with the code!_)

## Step 1 - refine characteristics
1. Create MergedCharacteristics table. Keep company name, currency and fund name (until now, different field names for each source) as well as primary keys of Source and SourceFundID.

2. Create standardized names for companies as a new column. Use method from old inherited code.

## Data
Data is stored in an SQLite database. It could be exported to flat text, easiest with the [command line SQLite3 tools](https://sqlite.org/download.html) and Windows batch scripts.

