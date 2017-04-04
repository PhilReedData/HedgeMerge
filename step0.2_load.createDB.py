#!/usr/bin/python    
# Step 0.2
# Load: Create database

import ConfigParser
import os
import sqlite3

def deleteDatabase(dbPath):
    if os.path.exists(dbPath):
        os.remove(dbPath)
        print("Deleted previous database.")

# yes, there are better ways to do this! 
MONTHS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
YEARS = ['1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016'] 

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')
print("Opening or creating database at: " + dbPath)

# Uncomment this line if you want to reset (or manually delete file)
# deleteDatabase(dbPath)

try:
    db=sqlite3.connect(dbPath)
    print ("Database created and opened successfully.")
    
    # Fill in tables...
    cursor = db.cursor()
    
    sql = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor.execute(sql)
    print ("What tables already exist, if any?")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    # RETURN TABLE
    sql = '''CREATE TABLE IF NOT EXISTS Return (
        Source CHAR(1) NOT NULL,
        SourceFundID VARCHAR(50) NOT NULL,
        '''
    for y in YEARS:
        for m in MONTHS:
            line = '"' + y + '-' + m + '" FLOAT, \n'
            sql = sql + line
    sql = sql + '''PRIMARY KEY (Source, SourceFundID)
);'''
    cursor.execute(sql)
    db.commit()
    
    # AUM TABLE
    sql = '''CREATE TABLE IF NOT EXISTS AUM (
        Source CHAR(1) NOT NULL,
        SourceFundID VARCHAR(50) NOT NULL,
        '''
    for y in YEARS:
        for m in MONTHS:
            line = '"' + y + '-' + m + '" FLOAT, \n'
            sql = sql + line
    sql = sql + '''PRIMARY KEY (Source, SourceFundID)
);'''
    cursor.execute(sql)
    db.commit()
    
    # TASSCharacteristics
    sql = '''CREATE TABLE IF NOT EXISTS TASSCharacteristics (
        Source CHAR(1) NOT NULL,
        SourceFundID VARCHAR(50) NOT NULL,
        
        "T_ProductReference" INTEGER,
        "T_Name" VARCHAR(255),
        "T_PrimaryCategory" VARCHAR(50),
        "T_CurrencyCode"  VARCHAR(3),
        "T_CurrencyDescription" VARCHAR(50),
        "T_LegalStructure" VARCHAR(50),
        "T_ClosedToInvestDate" DATE,
        "T_ReopenToInvestDate" DATE,
        "T_InceptionDate" DATE,
        "T_PerformanceStartDate" DATE,
        "T_PerformanceEndDate" DATE,
        "T_GrossNett" VARCHAR(1),
        "T_InitialNAV" FLOAT,
        "T_InitialSharePrice" FLOAT,
        "T_Guaranteed" INTEGER,
        "T_NavROR" VARCHAR(1),
        "T_MinimumInvestment" INTEGER,
        "T_ManagementFee" FLOAT,
        "T_IncentiveFee" FLOAT,
        "T_ManagementFeePayablePeriod" VARCHAR(50),
        "T_HighWaterMark" INTEGER,
        "T_ShareEqualisationMethod" INTEGER,
        "T_Leveraged" INTEGER,
        "T_MaxLeverage" FLOAT,
        "T_AvgLeverage" FLOAT,
        "T_Futures" INTEGER,
        "T_Derivatives" INTEGER,
        "T_Margin" INTEGER,
        "T_FXCredit" INTEGER,
        "T_PersonalCapital" INTEGER,
        "T_PersonalCapitalAmount" FLOAT,
        "T_CurrencyExposure" INTEGER,
        "T_OpenEnded" INTEGER,
        "T_OpenToPublic" INTEGER,
        "T_InvestsInManagedAccounts" INTEGER,
        "T_InvestsInOtherFunds" INTEGER,
        "T_AcceptsManagedAccounts" INTEGER,
        "T_ManagedAccountsMinAmount" INTEGER,
        "T_TrackingFrequency" VARCHAR(50),
        "T_SubscriptionFrequency" VARCHAR(50),
        "T_RedemptionFrequency" VARCHAR(50),
        "T_RedemptionNoticePeriod" INTEGER,
        "T_LockUpPeriod" INTEGER,
        "T_LockUpComment" VARCHAR(255),
        "T_PayOutPeriod" INTEGER,
        "T_PayOutComment" VARCHAR(255),
        "T_AuditDate" DATE,
        "T_RegisteredInvestmentAdviser" INTEGER,
        "T_DomicileState" VARCHAR(50),
        "T_DomicileCountry" VARCHAR(50),
        
        PRIMARY KEY (Source, SourceFundID)
);'''
    
    
        # "T_CompanyID" INTEGER,
        # "T_CompanyName" VARCHAR(255),
        # "T_CompanyType" VARCHAR(50),
    cursor.execute(sql)
    db.commit()
    
    # OTHER TABLES...

    print ("TODO: fill in other tables")
    db.commit()
except Exception as e:
    db.rollback()
    raise e
finally:
    db.close()