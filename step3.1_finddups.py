#!/usr/bin/python    
# Step 3.1
# Find duplicate funds in MergedCharacteristics

# We are populating the MergedFundID field

#DEV 28/4/2017
# New ID works if you manually set correlationR. Only outputs to temp table for now.

# MERGEDCHARACTERISTICS EXAMPLE
# Source, SourceFundID, StdCompanyName, Currency, [CompCurr], MergedFundID
# T,      1,            Apple,          USD,      1,          ?
# T,      2,            Apple,          USD,      1,          ?
# T,      3,            Apple,          GBP,      2,            ?
# T,      4,            Banana,         USD,      3,              ?
# T,      5,            Banana,         USD,      3,              ?
# T,      6,            ,               EUR,      ,
# E,      10,           Apple,          USD,      1,          ?
# E,      11,           Apple,          GBP,      2,            ?
# E,      12,           Carrot,         USD,      4,                 ?
# E,      13,           Carrot,         USD,      4,                 ?
# E,      14,           Date,           EUR,      5,                   ?
# E,      15,           Apple Pie,      USD,      6,                     ?

# [CompCurr] is not saved in the table, 
# it just illustrates which sets of funds are checked in pairs for return correln.

# THOUGHT: sorting by StdCompanyName,Currency will make things easier...

# The best company identifer that is share between all funds is StdCompanyName.
# Counter for mergedFundID starts from 1, increment each time accessed.
# Wipe all existing MergedFundIDs before starting.
# For each fund (row in MergedCharacteristics):
    # If StdCompanyName is blank, skip.
    # If fund already has a MergedFundID, skip.
    # Get all other rows with same StdCompanyName and no MergedFundID -> list F.
    # For each fund in set F:
        # Get all other rows with same StdCompanyName, same Currency and no MergedFundID -> list G.
        # Get all currencies in list G.
        # For each currency:
            # If size(G) = 1:
                # Record next MergedFundID 
            # Else:
                # List all pair combinations of members of G -> P.
                # For each pair (p1, p2) in P:
                    # Get the returns data for each (p1, p2) -> (r1, r2)
                    # Look at correlation of (r1, r2)
                    # If correlation >= 99%:
                        # Match! record next MergedFundID, same for both
                # For each unmatched pair:
                    # No match! record next MergedFundID 

import ConfigParser
import sqlite3
import pandas as pd
import itertools

doFindDups = True

nextMergedFundID = 1
def getNextMergedFundID():
    global nextMergedFundID
    id = nextMergedFundID
    nextMergedFundID = nextMergedFundID + 1
    return id

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')
print("Opening database at: " + dbPath)

try:
    db=sqlite3.connect(dbPath)
    #db.row_factory = sqlite3.Row
    db.text_factory = str
    print ("Database created and opened successfully.")
    
    cursor = db.cursor()
    
    sql = "SELECT count(*) FROM MergedCharacteristics;"
    cursor.execute(sql)
    print ("How many rows in MergedCharacteristics?")
    reply = cursor.fetchone()
    print(reply)
    
    if doFindDups:
        print ("Finding duplicate funds in MergedCharacteristics. May overwrite!")
        # Start with a clean slate
        sql = "UPDATE MergedCharacteristics SET MergedFundID = NULL; "
        cursor.execute(sql)
        
        # Get all columns and all rows from MergedCharacteristics
        sql = "SELECT Source, SourceFundID, StdCompanyName, Currency, MergedFundID  FROM MergedCharacteristics ORDER BY StdCompanyName, Currency LIMIT 500 OFFSET 15000;" # TEMP #######
        df = pd.read_sql(sql, db)
        df = df.set_index(['StdCompanyName', 'Currency']).reset_index()
        #print(df)
        
        uniqueNames = df.StdCompanyName.unique()
        #print(uniqueNames[:5])
        
        for name in uniqueNames[0:20]: # TEMP ###########################
            if len(name) == 0:
                # We can't do anything for funds with no company name
                continue
            uniqueCurrencies = df[df.StdCompanyName == name].Currency.unique()
            #print (name, len(uniqueCurrencies))
            #print (name)
            for currency in uniqueCurrencies:
                #print(currency)
                # Get all funds for this CompCurr (as dataframe)
                compCurrFunds = df[(df.StdCompanyName == name) & (df.Currency == currency)]
                #print(len(compCurrFunds))
                if len(compCurrFunds) < 2:
                    # RECORD next MergedFundID (should be 1 iteration!)
                    for i in compCurrFunds.index:
                        id = getNextMergedFundID()
                        df.set_value(i, 'MergedFundID', id) 
                    continue
                # DO PAIRING
                # List of all pairs of funds in compCurrFunds
                fundIndexList = compCurrFunds.index
                pairedFundIndexList = []
                pairs = list(itertools.combinations(fundIndexList, 2))
                for pair in pairs:
                    fundIndex1 = pair[0]
                    fundIndex2 = pair[1]
                    # Get Source and SourceFundID for each
                    source1 = df.get_value(fundIndex1, 'Source')
                    source2 = df.get_value(fundIndex2, 'Source')
                    sourceFundID1 = df.get_value(fundIndex1, 'SourceFundID')
                    sourceFundID2 = df.get_value(fundIndex2, 'SourceFundID')
                    # MIGHT PUT THIS BIT IN ANOTHER SCRIPT...
                    # Get Return data for each
                    sql1 = 'SELECT * FROM RateOfReturn WHERE Source = "' + source1 
                    sql1 = sql1 + '" AND SourceFundID = "' + sourceFundID1 + '";'
                    sql2 = 'SELECT * FROM RateOfReturn WHERE Source = "' + source2 
                    sql2 = sql2 + '" AND SourceFundID = "' + sourceFundID2 + '";'
                    #...
                    # Compare return data, correlation
                    #...
                    # ...MIGHT PUT THIS BIT IN ANOTHER SCRIPT
                    correlationR = 0.99 # for example
                    if correlationR >= 0.99:
                        # Consider this a match
                        # What if this already has been matched? Share ID
                        id = 0
                        id1 = df.get_value(fundIndex1, 'MergedFundID')
                        id2 = df.get_value(fundIndex2, 'MergedFundID')
                        if id1 or id2:
                            # Use existing id
                            id = id1 if id1 else id2
                            if (id1 and id2) and (id1 <> id2):
                                print("Error merging")
                        else:
                            # RECORD next MergedFundID for all
                            id = getNextMergedFundID()
                        df.set_value(fundIndex1, 'MergedFundID', id)
                        df.set_value(fundIndex2, 'MergedFundID', id)
                        pairedFundIndexList.append(fundIndex1)
                        pairedFundIndexList.append(fundIndex2)
                    else:
                        # Consider this not a match
                        pass
                # What funds remain unpaired?
                for i in fundIndexList:
                    if i not in pairedFundIndexList:
                        # RECORD next MergedFundID
                        df.set_value(i, 'MergedFundID', getNextMergedFundID()) 
                
        # Write back df to SQLite
        # THIS WOULD LOOSE ANY COLUMNS NOT PRESENT IN THE DATAFRAME!
        df.to_sql(name='MergedCharacteristics2', con=db, index=False, if_exists='replace')
        # If this doesn't work, will have to go through row by row and do UPDATE statements
        #...
        # Commit
        db.commit()
        
        sql = "SELECT * FROM MergedCharacteristics2 LIMIT 10;"
        cursor.execute(sql)
        print ("First rows of MergedCharacteristics2")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        
    else: # not doFindDups
        sql = "SELECT count(*) FROM MergedCharacteristics WHERE 'StdCompanyName' NOT NULL;"
        cursor.execute(sql)
        print ("How many rows with StdCompanyName in MergedCharacteristics:")
        reply = cursor.fetchone()
        names = [description[0] for description in cursor.description]
        print (names)
        print(reply)
    
except Exception as e:
    db.rollback()
    import sys
    print 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno)
    raise e
finally:
    db.close()
