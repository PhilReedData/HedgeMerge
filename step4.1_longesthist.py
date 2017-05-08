#!/usr/bin/python    
# Step 4.1
# Determine longest history of merged funds in MergedCharacteristics

# We are populating the LongestHist field from the MergedFundID field

# For each set of funds with the same MergedFundID
    # Get the first non-nan date and the last
    # The one with the biggest range gets LongestHist = 1
    # The rest get LongestHist = 0
    
import ConfigParser
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import itertools

doLongestHist = True


# Give a SQL statement on RateOfReturn or AUM tables, 
# Get the data as a Pandas DataFrame transposed
def getReturnSeries(db, cursor, sql, fundIDX, pairX):
    # Get rows of funds and columns of months
    # Source, SourceFundID, 1990-01, ...
    # E, id0, val, ...
    # T, id1, val, ...
    # ...
    dfT = pd.read_sql(sql, db)
    
    # What if no rows?
    if dfT.shape[0] < 1:
        print ('No return data found for fund ' + fundIDX + ' (pair '+pairX + ')')
        return pd.DataFrame() # return an empty data frame
    
    # Need a single index variable fundID = Source+SourceFundID
    # FundID, 1990-01, ...
    # Eid0, val, ...
    # Tid1, val, ...
    # ...
    dfT['FundID'] = dfT.apply(lambda row: row['Source'] + row['SourceFundID'], axis=1)
    dfT.set_index('FundID', inplace=True)
    dfT.drop('Source', axis=1, inplace=True)
    dfT.drop('SourceFundID', axis=1, inplace=True)
    # Transpose to rows of months and columns of funds
    # FundID: Eid0, Tid1, ...
    # 1990-01: val, val, ...
    # ...
    df = dfT.T # hope that there are no other columns, just the months
    dateStrings = list(dfT.columns.values)
    def handleDate(date):
        return dt.datetime(year=int(date[0:4]), month=int(date[5:7]), day=1)
    # Set index as monthly date
    df.rename(index=handleDate,inplace=True)
    df.index = pd.PeriodIndex(list(df.index), freq='M')
    def forceToFloat(x):
        try:
            return np.float64(x)
        except:
            return np.nan
    df = df.applymap(forceToFloat)
    return df

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
    
    sql = "SELECT count(*) FROM MergedCharacteristics2;"
    cursor.execute(sql)
    print ("How many rows in MergedCharacteristics2?")
    reply = cursor.fetchone()
    print(reply)
    
    if doLongestHist:
        print ("Determining longest history for same fund in MergedCharacteristics2. May overwrite!")
        # Start with a clean slate
        sql = "UPDATE MergedCharacteristics3 SET MergedFundID = NULL; "
        cursor.execute(sql)
        
        # Get all columns and all rows from MergedCharacteristics
        sql = "SELECT Source, SourceFundID, StdCompanyName, Currency, MergedFundID  FROM MergedCharacteristics2 ORDER BY MergedFundID LIMIT 500 OFFSET 15000;" # TEMP #######
        df = pd.read_sql(sql, db)
        df = df.set_index(['MergedFundID']).reset_index()
        #print(df)
        
        uniqueMFIDs = df.MergedFundID.unique()
        #print(uniqueMFID[:5])
        for mergedFundID in uniqueMFIDs[0:20]: # TEMP ###########################
            if len(mergedFundID) == 0:
                # We can't do anything for funds with no company name
                continue
            mergedFunds = df[df.MergedFundID == mergedFundID]
            if len(mergedFunds) < 2:
                # Has to be the longest if there is only 1!
                for i in mergedFunds.index:
                    df.set_value(i, 'LongestHist', 1) 
                continue
            fundIDs = []
            ranges = {} # fundID:range
            for mergedFund in mergedFunds:
                fundIndex = mergedFund.index
                # Get Source and SourceFundID
                source = df.get_value(fundIndex, 'Source')
                sourceFundID = df.get_value(fundIndex, 'SourceFundID')
                fundID = source+sourceFundID
                fundIDs.append(fundID)
                # Get Return data
                sql = 'SELECT * FROM RateOfReturn WHERE Source = "' + source 
                sql = sql + '" AND SourceFundID = "' + sourceFundID + '";'
                df = getReturnSeries(db, cursor, sql, fundID, '')
                earliest = df.first_valid_index()
                last = df.last_valid_index()
                ranges[fundID] = last - earliest
            # Which fund has the largest range?
            v=list(d.values())
            k=list(d.keys())
            longestHistFundID = k[v.index(max(v))]
            # Set the LongestHist values
            for mergedFund in mergedFunds:
                fundIndex = mergedFund.index
                source = df.get_value(fundIndex, 'Source')
                sourceFundID = df.get_value(fundIndex, 'SourceFundID')
                fundID = source+sourceFundID
                value = 1 if fundID == longestHistFundID else 0
                df.set_value(fundIndex, 'LongestHist', value)
                
            
            #...
        
        # Will update the real MergedCharacteristics later
        df.to_sql(name='MergedCharacteristics3', con=db, index=False, if_exists='replace')
        # Commit
        db.commit()
        
        sql = "SELECT * FROM MergedCharacteristics3 LIMIT 10;"
        cursor.execute(sql)
        print ("First rows of MergedCharacteristics3")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        
    else: # not doLongestHist
        sql = "SELECT count(*) FROM MergedCharacteristics3 WHERE 'LongestHist' NOT NULL;"
        cursor.execute(sql)
        print ("How many rows with LongestHist in MergedCharacteristics3:")
        reply = cursor.fetchone()
        names = [description[0] for description in cursor.description]
        print (names)
        print(reply)
        
        
except sqlite3.Error as e:
#except Exception as e:
    db.rollback()
    import sys
    print 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno)
    raise e
finally:
    db.close()
