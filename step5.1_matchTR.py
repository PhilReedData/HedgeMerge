#!/usr/bin/python    
# Step 5.1
# Match companies with Thomson Reuters (TR) holdings list
# Should we add fuzzy?

# Read from MergedCharacteristics3, get StdCompanyName
# Put unique names into new dataframe mapTR
# Read table of TR names to new dataframe allTR, cols MGRNAME, MGRCODE
# Standardize names in allTR, new column StdMGRNAME
# Extra step to replace all shorthand of "MANAGEMENT" to full one.
# For each name in map13F
    # apply name matching function looking in allTR
    # if good match, add the name and code from allTR to mapTR
# Export mapTR to SQLite and/or text


LF_LEGAL = [' LTD$',' Limited$',' LLC$',' LP$', ' LC$', ' Inc$',' Corporation$',' Corp$', ' Co$', ' Corporation$', ' AG$', ' GMBH$', ' SA$', ' PTE$', ' PLC$', ' PTY$', ' CP$', ' NV$', ' LLP$']
LF_COUNTRY = [' \(CAYMAN\)$', ' \(UK\)$', ' OVERSEAS$', ' OFFSHORE$', ' \(CYPRUS\)$', ' INTERNATIONAL$', ' COMPANY$' , ' \(BERMUDA\)$', ' BERMUDA$', ' BVI$' , ' \(SINGAPORE\)$', ' \(INDIA\)$', ' \(HK\)$', ' \(BAHAMAS\)$', ' \(UK\)$', ' \(ZUG\)$', ' \(ISRAEL\)$', ' \(HK\)$', ' \(IRELAND\)$', ' \(GUERNSEY\)$', ' \(ASIA\)$', ' \(SWEDEN\)$', ' \(USA\)$', ' \(HKG\)$', ' \(SING\)$', ' \(SWITZERLAND\)$', ' \(BVI\)', ' UK$', ' \(UTAH\)$', ' \(US\)$', ' \(CAYMAN ISLANDS\)$', ' \(THAILAND\)$', ' \(JERSEY\)$', ' \(DUBLIN\)$', ' CAYMAN ISLANDS$', ' \(HONG KONG\)$']
LF_BUZZWORDS = [ ' MANAGEMENT$', ' MANAGERS$', ' MANGEMENT$', ' MGT$', ' MGMT$', '^THE', ' ASSET$', ' CAPITAL$', ' MANAGERMENT$', ' INVESTMENT$',  ' PARTNERS$', ' ADVISORY$', ' ADVISORS$', ' ASSET MANAGEMENT$', ' CAPITAL MANAGEMENT$', ' ASSOCIATES$', ' PRIVATE$', ' PVT$', ' ASSET MANAGEMENT$', ' FUNDS$', ' ADVISERS$', ' TRADING$', ' GROUP$']

# LF_ALL = LF_LEGAL+LF_COUNTRY+LF_BUZZWORDS
LF_ALL = LF_LEGAL+LF_COUNTRY

SPECIAL_CHARACTERS = ['[\,\.\-]']

REPLACEMENTS = [("\&"," AND "),("\+"," AND "),(" MGMT"," MANAGEMENT")]

# Remove legal forms and country from end, etc.
def createStdCompanyName(inputName):
    import re

    def applyRegExp(regExpA, regExpB, string):
        return re.sub("(?i)"+regExpA, regExpB, string)
    
    standardName = inputName
    midKickout = SPECIAL_CHARACTERS
    endKickout = LF_ALL
    replacements = REPLACEMENTS

    # convert to uppercase
    standardName = standardName.upper()

    # kill leading and trailing spaces:
    standardName = standardName.strip()

    # kill midKickouts
    for cExp in midKickout:
        standardName = applyRegExp(cExp, "", standardName)

    # handle replacements
    for (cExp,cRep) in replacements:
        standardName = applyRegExp(cExp, cRep, standardName)

    # replace double-blanks by single blanks:
    for i in range(0,5):
        standardName = applyRegExp("  ", " ", standardName)

    for i in range(0,5):
        # kill leading and trailing spaces:
        standardName = standardName.strip()

        # kill endKickout:
        for cExp in endKickout:
            standardName = applyRegExp(cExp,'',standardName)

    return standardName

import ConfigParser
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import itertools

doMatchTR = True


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
    
    if doMatchTR:
        print ("Mapping StdCompanyName to Thomson Reuters holdings")

        # Get StdCompanyName (unique) from MergedCharacteristics
        sql = "SELECT DISTINCT StdCompanyName FROM MergedCharacteristics ORDER BY StdCompanyName;"# LIMIT 500 OFFSET 15000;" # TEMP #######
        mapTR = pd.read_sql(sql, db)
        #mapTR = mapTR.set_index(['StdCompanyName']).reset_index()
        #print(mapTR)
        
        # Get TR names
        allTR = pd.read_csv(config.get('SourceFiles', 'source.tr.companies'))
        #print(allTR)
        
        # Standardize names in allTR, new column StdMGRNAME
        allTR['StdMgrname'] = allTR['mgrname'].map(createStdCompanyName)
        #print(allTR)
        
        # Replace MGMT with MANAGEMENT
        # (this is done by adding an extra item to REPLACEMENTS)
        
        # Find matches
        # # For each row in mapTR (as in, pass the following function to map)
        # # # Look up name in allTR, return number
        def getNumber(name):
            matchedDF = allTR[allTR['StdMgrname'] == name]
            return matchedDF['mgrno'].iloc[0] if not matchedDF.empty else None
        mapTR['mgrno'] = mapTR['StdCompanyName'].map(getNumber)
        #print(mapTR)
        
        # Count matches
        countRows = len(mapTR['mgrno'])
        countValidRows = mapTR['mgrno'].count()
        countNaN = countRows - countValidRows
        print('Names with no match:',countNaN,'of',countRows)
        
        # print out table of matches 
        mapTRPath = config.get('OutputFiles', 'output.matchtr')
        mapTR.to_csv(mapTRPath, sep=',', index=False)
        print('Written to file: ' + mapTRPath)
        # Matched 1239 of 10560 names
        
    else: # not doMatchTR
        sql = "SELECT count(*) FROM MergedCharacteristics3 WHERE 'LongestHist' NOT NULL;"
        # cursor.execute(sql)
        # print ("How many rows with LongestHist in MergedCharacteristics3:")
        # reply = cursor.fetchone()
        # names = [description[0] for description in cursor.description]
        # print (names)
        # print(reply)
        
        
except sqlite3.Error as e:
#except Exception as e:
    db.rollback()
    import sys
    print 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno)
    raise e
finally:
    db.close()
