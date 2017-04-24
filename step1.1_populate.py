#!/usr/bin/python    
# Step 1.1
# Populate: For each row in (TASS|Eureka)Characteristics, populate MergedCharacteristics.

import ConfigParser
import sqlite3

doPopulate = True

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
    
    if doPopulate:
        cursor.execute("DELETE FROM MergedCharacteristics;")
        sources = ["TASSCharacteristics" , "EurekaCharacteristics"]
        # OUTER LOOP For each of TASSCharacteristics and EurekaCharacteristics
        #   LOOP For each row in XCharacteristics, select ... from XCharacteristics
        #     Do the merging
        #     insert into MergedCharacteristics ...  values ...
        # Commit
        
        for source in sources:
            print ("Populating MergedCharacteristics from " + source)
            # Get all columns and all rows from EurekaCharacteristics
            sql = "SELECT * FROM " + source + " ;" # TEMP LIMIT WHERE SourceFundID = '42464'
            cursor.execute(sql)
            rows = cursor.fetchall()
            #names = [description[0] for description in cursor.description]
            #print names
            
            for i, row in enumerate(rows):
                # Do the merging
                ## Get the data from row
                source = row['Source']
                sourceFundID = row['SourceFundID']
                tass = True if source == 'T' else False
                ## Do the merging
                fundName = row['T_Name'] if tass else row['E_FundName']
                currency = row['T_CurrencyCode'] if tass else row['E_Currency']
                companyName = row['T_CompanyName'] if tass else row['E_ManagementCompany']
                # Eureka has no companyID, use the row index instead.
                companyID = row['T_CompanyID'] if tass else str(i+1)
                managementFee = row['T_ManagementFee'] if tass else row['E_ManagementFee_pc']
                incentiveFee = row['T_IncentiveFee'] if tass else row['E_PerformanceFee_pc']
                lockUp = row['T_LockUpPeriod'] if tass else row['E_LockUp']
                notice = row['T_RedemptionNoticePeriod'] if tass else row['E_RedemptionNotificationPeriod']
                hwm = row['T_HighWaterMark'] if tass else row['E_HighWaterMark']
                leverage = row['T_Leveraged'] if tass else row['E_Leverage']
                minimumInvestment = row['T_MinimumInvestment'] if tass else row['E_MinimumInvestmentSize']
                redemptionFrequency = row['T_RedemptionFrequency'] if tass else row['E_RedemptionFrequency']
                subscriptionFrequency = row['T_SubscriptionFrequency'] if tass else row['E_SubscriptionFrequency']
                strategy = row['T_PrimaryCategory'] if tass else row['E_MainInvestmentStrategy']
                domicile = row['T_DomicileCountry'] if tass else row['E_Domicile']
                closed = ''
                if tass:
                    t_open = row['T_OpenToPublic']
                    # Invert open to closed for TASS
                    closed = '1' if t_open == '0' else '0'
                else:
                    e_closed = row['E_Closed']
                    closed = '1' if e_closed == 'Yes' else '0'
                liquidated = ''
                if tass:
                    liquidated = row['T_Dead']
                else:
                    liquidated = '1' if row['E_Dead'] == 'Yes' else '0'
                
                mergedFields = (fundName, currency, companyName, companyID, managementFee, incentiveFee, lockUp, notice, hwm, leverage, minimumInvestment, redemptionFrequency, subscriptionFrequency, strategy, domicile, closed, liquidated)
                
                # Put this row into MergedCharacteristics
                sql = '''INSERT INTO MergedCharacteristics (
                    source,
                    sourceFundID,
                            
                    FundName,
                    Currency, 
                    CompanyName,
                    CompanyID ,
                    ManagementFee ,
                    IncentiveFee ,
                    LockUp ,
                    Notice ,
                    HWM ,
                    Leverage ,
                    MinimumInvestment ,
                    RedemptionFrequency ,
                    SubscriptionFrequency ,
                    Strategy ,
                    Domicile ,
                    Closed ,
                    Liquidated
                ) VALUES (
                '''
                sql = sql + '"' + source + '", "' + sourceFundID + '"'
                for j, field in enumerate(mergedFields):
                    if field == None: 
                        field = ''
                    sql = sql + ', "' + str(field) + '"'
                    #print ('field ' + str(j) + ': ' + str(field))
                sql = sql + '''
                )
                ;'''
                cursor.execute(sql)
        # Commit
        db.commit()
        
        # Did it work?
        sql = "SELECT count(*) FROM MergedCharacteristics;"
        cursor.execute(sql)
        print ("How many rows in MergedCharacteristics?")
        reply = cursor.fetchone()
        print(reply)
        
        sql = "SELECT * FROM MergedCharacteristics LIMIT 1;"
        cursor.execute(sql)
        print ("First row in MergedCharacteristics:")
        reply = cursor.fetchone()
        names = [description[0] for description in cursor.description]
        print (names)
        print(reply)
    
    else: # not doPopulate
        sql = "SELECT * FROM MergedCharacteristics LIMIT 1;"
        cursor.execute(sql)
        print ("First row in MergedCharacteristics:")
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