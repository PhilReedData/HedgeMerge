#!/usr/bin/python    
# Step 0.3
# Load: Read TASS from text to database
# Change first two variables True/False to trigger each part of this script.

import ConfigParser
import os
import csv
import sqlite3

doCharacteristics = True
doReturns = True 

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')

print("Opening database at: " + dbPath)
if True:
#try:
    db=sqlite3.connect(dbPath)
    print ("Database created and opened successfully.")
    
    # Fill in tables...
    cursor = db.cursor()
    
    sql = "SELECT count(*) FROM TASSCharacteristics;"
    cursor.execute(sql)
    print ("What entries already exist, if any?")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    # Read live then dead files for return/AUM and characteristics
    # read source.tass.live.companies
    # read source.tass.live.productdetails
    # read source.tass.live.productperformance
    # read source.tass.dead.companies
    # read source.tass.dead.productdetails
    # read source.tass.dead.productperformance
    livePaths = (config.get('SourceFiles', 'source.tass.live.companies'), config.get('SourceFiles', 'source.tass.live.productdetails'), config.get('SourceFiles', 'source.tass.live.productperformance'))
    deadPaths = (config.get('SourceFiles', 'source.tass.dead.companies'), config.get('SourceFiles', 'source.tass.dead.productdetails'), config.get('SourceFiles', 'source.tass.dead.productperformance'))
    alltimeFilePaths = [livePaths, deadPaths]
    fundsMatchedToCompany = 0
    fundsNotMatchedToCompany = 0
    
    # all companies (tuple of companyID, companyName), indexed by fund ID

    cursor.execute("DELETE FROM TASSCharacteristics;")
    if doCharacteristics:
        for filePaths in alltimeFilePaths:
            companies = {}
            companiesPath = filePaths[0]
            productDetailsPath = filePaths[1]
            performancePath = filePaths[2]
            #print('co = ' + companiesPath + ', de = ' + productDetailsPath + ', pe = ' + performancePath)
            
            # Load companies into memory first
            # # Read CSV
            try:
                with open(companiesPath, 'r') as companiesFile:
                    rows = csv.reader(companiesFile)
                    next(rows) # skip header
                    for row in rows:
                        try:
                            # Format = "ProductReference","CompanyID","CompanyName","CompanyType"
                            if row[3] == "Management Firm":
                                #print ('Found management firm for ' + row[0])
                                if row[0] in companies: 
                                    print ('Overwriting companies list at ' + row[0] )
                                companies[row[0]] = (row[1], row[2])
                        except Exception as e:
                            print ('Exception raised reading file ' + companiesPath)
                        #    raise e # skip?
                print ('How many companies loaded = ' + str(len(companies)))
                #print ('First company: ')
                #print (companies['21'])
            except IOError as ioe:
                print ('Exception raised reading or closing file ' + companiesPath)
            
            # Load product details, with companies from memory, for TASSCharacteristics table
            with open(productDetailsPath, 'r') as productDetailsFile:
                    rows = csv.reader(productDetailsFile)
                    print('Read lines from ' + productDetailsPath)
                    next(rows) # skip header
                    for row in rows:
                        sql = '''INSERT INTO TASSCharacteristics(
            Source,
            SourceFundID,        
            "T_Name" ,
            "T_PrimaryCategory" ,
            "T_CurrencyCode"  ,
            "T_CurrencyDescription",
            "T_LegalStructure" ,
            "T_ClosedToInvestDate" ,
            "T_ReopenToInvestDate" ,
            "T_InceptionDate" ,
            "T_PerformanceStartDate" ,
            "T_PerformanceEndDate" ,
            "T_GrossNett" ,
            "T_InitialNAV" ,
            "T_InitialSharePrice" ,
            "T_Guaranteed" ,
            "T_NavROR" ,
            "T_MinimumInvestment" ,
            "T_ManagementFee" ,
            "T_IncentiveFee" ,
            "T_ManagementFeePayablePeriod" ,
            "T_HighWaterMark" ,
            "T_ShareEqualisationMethod" ,
            "T_Leveraged" ,
            "T_MaxLeverage" ,
            "T_AvgLeverage" ,
            "T_Futures" ,
            "T_Derivatives" ,
            "T_Margin" ,
            "T_FXCredit" ,
            "T_PersonalCapital" ,
            "T_PersonalCapitalAmount" ,
            "T_CurrencyExposure" ,
            "T_OpenEnded" ,
            "T_OpenToPublic" ,
            "T_InvestsInManagedAccounts" ,
            "T_InvestsInOtherFunds" ,
            "T_AcceptsManagedAccounts" ,
            "T_ManagedAccountsMinAmount" ,
            "T_TrackingFrequency" ,
            "T_SubscriptionFrequency" ,
            "T_RedemptionFrequency" ,
            "T_RedemptionNoticePeriod" ,
            "T_LockUpPeriod" ,
            "T_LockUpComment" ,
            "T_PayOutPeriod" ,
            "T_PayOutComment" ,
            "T_AuditDate" ,
            "T_RegisteredInvestmentAdviser" ,
            "T_DomicileState" ,
            "T_DomicileCountry" ,
            "T_CompanyID" ,
            "T_CompanyName" ) VALUES ('''
                        
                        #0"ProductReference",1"Name",2"PrimaryCategory",3"CurrencyCode",4"CurrencyDescription",5"LegalStructure",6"ClosedToInvestDate",7"ReopenToInvestDate",8"InceptionDate",9"PerformanceStartDate",10"PerformanceEndDate",11"GrossNett",12"InitialNAV",13"InitialSharePrice",14"Guaranteed",15"NavROR",16"MinimumInvestment",17"ManagementFee",18"IncentiveFee",19"ManagementFeePayablePeriod",20"HighWaterMark",21"ShareEqualisationMethod",22"Leveraged",23"MaxLeverage",24"AvgLeverage",25"Futures",26"Derivatives",27"Margin",28"FXCredit",29"PersonalCapital",30"PersonalCapitalAmount",31"CurrencyExposure",32"OpenEnded",33"OpenToPublic",34"InvestsInManagedAccounts",35"InvestsInOtherFunds",36"AcceptsManagedAccounts",37"ManagedAccountsMinAmount",38"TrackingFrequency",39"SubscriptionFrequency",40"RedemptionFrequency",41"RedemptionNoticePeriod",42"LockUpPeriod",43"LockUpComment",44"PayOutPeriod",45"PayOutComment",46"AuditDate",47"RegisteredInvestmentAdviser",48"DomicileState",49"DomicileCountry"
                        sql = sql + '"T"' + ', '
                        # For all fields 0 to 49 above
                        for cell in row:
                            sql = sql + '"' + cell + '", '
                        if row[0] in companies:
                            company = companies[row[0]]
                            companyID = company[0]
                            companyName = company[1]
                            sql = sql + "\"" + companyID + "\", \"" + companyName + "\""
                            fundsMatchedToCompany =  fundsMatchedToCompany + 1
                        else:
                            sql = sql + "NULL, NULL"
                            fundsNotMatchedToCompany = fundsNotMatchedToCompany + 1
                        sql = sql + '''
    );'''
                        #print (sql)
                        cursor.execute(sql)
                    db.commit()
            
        cursor.execute("SELECT count(*) FROM TASSCharacteristics;")
        print('Rows in TASSCharacteristics: ' + str(cursor.fetchall()[0][0]))
        pcFundsMatchedToCompany = 100 * fundsMatchedToCompany/(fundsMatchedToCompany + fundsNotMatchedToCompany)
        print('Funds matched to company ' + str(fundsMatchedToCompany))
        print('Funds not matched to company ' + str(fundsNotMatchedToCompany))
        print('Funds matched to company ' + str(pcFundsMatchedToCompany) + '%')
    else:
        # Don't doCharacteristics
        sql = "SELECT count(*) FROM TASSCharacteristics;"
        cursor.execute(sql)
        print ("How many funds are in TASSCharacteristics?")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        
    # Load product performance for RateOfReturn and AUM tables
    if doReturns:
        cursor.execute("DELETE FROM RateOfReturn;")
        cursor.execute("DELETE FROM AUM;") # TEMP
        for filePaths in alltimeFilePaths:
            companies = {}
            companiesPath = filePaths[0]
            productDetailsPath = filePaths[1]
            performancePath = filePaths[2]
            
            # Read performance file
            with open(performancePath, 'r') as performanceFile:
                rows = csv.reader(performanceFile)
                print('Read lines from ' + productDetailsPath)
                next(rows) # skip header
                fundID = '0'
                fundIDPrev = '-1'
                date = 'YYYY-MM'
                ror = '0'
                aum = '0'
                fundRORs = {} # date:value
                fundAUMs = {} # date:value
                sqlROR = ""
                sqlAUM = ""
                veryFirstRow = True
                for row in rows:
                    #"ProductReference","Date","RateOfReturn","NAV","EstimatedAssets","EstimatedActual"
                    #21,1990-01-31 00:00:00,-2.4,97.6,48746,"A"
                    fundIDPrev = fundID
                    fundID = row[0]
                    date = row[1][0:7]
                    ror = row[2]
                    aum = row[4]
                    
                    if fundID != fundIDPrev:
                        #First line of new fund, do something with previous block, if any, then reset
                        if veryFirstRow:
                            veryFirstRow = False
                        else:
                            # Make the SQL and execute                
                            # Don't forget to do the last block!
                            datesROR = fundRORs.keys()
                            datesAUM = fundAUMs.keys()
                            valuesROR = list(fundRORs.values())
                            valuesAUM = list(fundAUMs.values())
                            sqlROR = '''INSERT INTO RateOfReturn (
                    Source,
                    SourceFundID'''
                            for dateROR in datesROR:
                                sqlROR = sqlROR + ', "' + dateROR + '" '
                            sqlROR = sqlROR + '''
            )  VALUES (
                    "T", "''' + fundIDPrev + '"'
                            for valueROR in valuesROR:
                                sqlROR = sqlROR + ', "' + valueROR + '" '
                            sqlROR = sqlROR + '''
             );'''   
                            sqlAUM = '''INSERT INTO AUM (
                    Source,
                    SourceFundID'''
                            for dateAUM in datesAUM:
                                sqlAUM = sqlAUM + ', "' + dateAUM + '" '
                            sqlAUM = sqlAUM + '''
            )  VALUES (
                    "T", "''' + fundIDPrev + '"'
                            for valueAUM in valuesAUM:
                                sqlAUM = sqlAUM + ', "' + valueAUM + '" '
                            sqlAUM = sqlAUM + '''
             );'''   
                            # Call DB
                            cursor.execute(sqlROR)
                            cursor.execute(sqlAUM)
                            #db.commit()
                            # Reset block
                            fundRORs = {}
                            fundAUMs = {}
                    
                    # If before 1990, skip!
                    if int(date[0:4]) < 1990:
                        #print("Discarding " + date + " for fund " + fundID)
                        continue
                    # This is a normal line, even if it's the last of a fund block
                    # ... save monthly data to a dict
                    fundRORs[date] = ror
                    fundAUMs[date] = aum
                    # Need to group all values for each fundID before writing SQL line
                # End for loop    
                # Don't forget to do the last block!
                # Make the SQL and execute...
                datesROR = fundRORs.keys()
                datesAUM = fundAUMs.keys()
                valuesROR = list(fundRORs.values())
                valuesAUM = list(fundAUMs.values())
                sqlROR = '''INSERT INTO RateOfReturn (
        Source,
        SourceFundID'''
                for dateROR in datesROR:
                    sqlROR = sqlROR + ', "' + dateROR + '" '
                sqlROR = sqlROR + '''
)  VALUES (
        "T", "''' + fundID + '"'
                for valueROR in valuesROR:
                    sqlROR = sqlROR + ', "' + valueROR + '" '
                sqlROR = sqlROR + '''
 );'''   
                sqlAUM = '''INSERT INTO AUM (
        Source,
        SourceFundID'''
                for dateAUM in datesAUM:
                    sqlAUM = sqlAUM + ', "' + dateAUM + '" '
                sqlAUM = sqlAUM + '''
)  VALUES (
        "T", "''' + fundID + '"'
                for valueAUM in valuesAUM:
                    sqlAUM = sqlAUM + ', "' + valueAUM + '" '
                sqlAUM = sqlAUM + '''
 );'''   
                # Call DB
                cursor.execute(sqlROR)
                cursor.execute(sqlAUM)
                db.commit()
                # Reset block (just to clear memory, really)
                fundRORs = {}
                fundAUMs = {}
                
                # Print summary
                cursor.execute("SELECT count(*) FROM RateOfReturn;")
                print('Rows in RateOfReturn: ' + str(cursor.fetchall()[0][0]))
                cursor.execute("SELECT count(*) FROM AUM;")
                print('Rows in AUM: ' + str(cursor.fetchall()[0][0]))
    # END IF doReturns
    else:
        # Print summary
        cursor.execute("SELECT count(*) FROM RateOfReturn;")
        print('Rows in RateOfReturn: ' + str(cursor.fetchall()[0][0]))
        cursor.execute("SELECT count(*) FROM AUM;")
        print('Rows in AUM: ' + str(cursor.fetchall()[0][0]))
    
    
    # Repeats for live/dead funds, appending (insert if not exists?)
    

#except Exception as e:
#    db.rollback()
#    raise e
#finally:
db.close()