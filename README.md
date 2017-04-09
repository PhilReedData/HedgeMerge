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
...where Source is TASS|Eureka, SourceFundID is the fund ID given by the Source, and the remaining columns are Return on the given month.

### AUM
`Source, SourceFundID, 1994-01, 1994-02, ..., 2016-12`
...where Source is TASS|Eureka, SourceFundID is the fund ID given by the Source, and the remaining columns are AUM on the given month.

### SourceCharactaristics
`Source, SourceFundID, [All TASS fields], [All Eurkea fields]`
...where Source is TASS|Eureka, SourceFundID is the fund ID given by the Source, and the remaining columns are the values for the fields in TASS or in Eureka. Apart from the first two columns, the table will be half empty (no TASS fields for Eureka funds, and vice versa).

#### MergedCharacteristics
We will be developing this table as we go along. It starts as a reduced version of the SourceCharacteristics:
`Source, SourceFundID, FundName, Currency, CompanyName`

We will fill in the remaining coloumns later:
`StdCompanyName, MergedFundID, UseDummy`
...where MergedFundID is not unique, and UseDummy is 1 or 0.


