#!/usr/bin/python    
# Step 2.1
# Standardized company names : For each row in MergedCharacteristics, create StdCompanyName.

LF_LEGAL = [' LTD$',' Limited$',' LLC$',' LP$', ' LC$', ' Inc$',' Corporation$',' Corp$', ' Co$', ' Corporation$', ' AG$', ' GMBH$', ' SA$', ' PTE$', ' PLC$', ' PTY$', ' CP$', ' NV$', ' LLP$']
LF_COUNTRY = [' \(CAYMAN\)$', ' \(UK\)$', ' OVERSEAS$', ' OFFSHORE$', ' \(CYPRUS\)$', ' INTERNATIONAL$', ' COMPANY$' , ' \(BERMUDA\)$', ' BERMUDA$', ' BVI$' , ' \(SINGAPORE\)$', ' \(INDIA\)$', ' \(HK\)$', ' \(BAHAMAS\)$', ' \(UK\)$', ' \(ZUG\)$', ' \(ISRAEL\)$', ' \(HK\)$', ' \(IRELAND\)$', ' \(GUERNSEY\)$', ' \(ASIA\)$', ' \(SWEDEN\)$', ' \(USA\)$', ' \(HKG\)$', ' \(SING\)$', ' \(SWITZERLAND\)$', ' \(BVI\)', ' UK$', ' \(UTAH\)$', ' \(US\)$', ' \(CAYMAN ISLANDS\)$', ' \(THAILAND\)$', ' \(JERSEY\)$', ' \(DUBLIN\)$', ' CAYMAN ISLANDS$', ' \(HONG KONG\)$']
LF_BUZZWORDS = [ ' MANAGEMENT$', ' MANAGERS$', ' MANGEMENT$', ' MGT$', ' MGMT$', '^THE', ' ASSET$', ' CAPITAL$', ' MANAGERMENT$', ' INVESTMENT$',  ' PARTNERS$', ' ADVISORY$', ' ADVISORS$', ' ASSET MANAGEMENT$', ' CAPITAL MANAGEMENT$', ' ASSOCIATES$', ' PRIVATE$', ' PVT$', ' ASSET MANAGEMENT$', ' FUNDS$', ' ADVISERS$', ' TRADING$', ' GROUP$']

# LF_ALL = LF_LEGAL+LF_COUNTRY+LF_BUZZWORDS
LF_ALL = LF_LEGAL+LF_COUNTRY

SPECIAL_CHARACTERS = ['[\,\.\-]']

REPLACEMENTS = [("\&"," AND "),("\+"," AND ")]

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

doStdCompanyName = True

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')
print("Opening database at: " + dbPath)

try:
    db=sqlite3.connect(dbPath)
    db.row_factory = sqlite3.Row
    db.text_factory = str
    print ("Database created and opened successfully.")
    
    cursor = db.cursor()
    
    sql = "SELECT count(*) FROM MergedCharacteristics;"
    cursor.execute(sql)
    print ("How many rows in MergedCharacteristics?")
    reply = cursor.fetchone()
    print(reply)
    
    if doStdCompanyName:
        # Get all rows from MergedCharacteristics
        # For each row
            # Get CompanyName
            # Call function to make its StdCompanyName
            # Update the row with StdCompanyName
        # Commit changes
        
        print ("Generating standardized company names in MergedCharacteristics. May overwrite!")
        # Get all columns and all rows from MergedCharacteristics
        sql = "SELECT Source, SourceFundID, CompanyName  FROM MergedCharacteristics ;"
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        for row in rows:
            ## Get the data from row
            source = row['Source']
            sourceFundID = row['SourceFundID']
            companyName = row['CompanyName']
            stdCompanyName = createStdCompanyName(companyName)
            
            sql = 'UPDATE MergedCharacteristics SET StdCompanyName = "' + stdCompanyName
            sql = sql + '" WHERE Source = "' + source + '" AND SourceFundID = "'
            sql = sql + sourceFundID + '";'
            cursor.execute(sql)
        db.commit()
    
    else: # not doStdCompanyName
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
