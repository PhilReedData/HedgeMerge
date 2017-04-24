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
YEARS = ['1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016'] 

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
    
    # RateOfReturn TABLE
    sql = '''CREATE TABLE IF NOT EXISTS RateOfReturn (
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
    
    # TASSCharacteristics TABLE
    # SourceFundID is T_ProductReference
    # Add T_Dead to day which files it came from (file1 = alive -> 0, file3 = dead -> 1)
    
    sql = '''CREATE TABLE IF NOT EXISTS TASSCharacteristics (
        Source CHAR(1) NOT NULL,
        SourceFundID VARCHAR(50) NOT NULL,
        
        "T_Dead" INTEGER,
        "T_Name" VARCHAR(255),
        "T_PrimaryCategory" VARCHAR(50),
        "T_CurrencyCode"  VARCHAR(50),
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
        "T_CompanyID" INTEGER,
        "T_CompanyName" VARCHAR(255),
        
        PRIMARY KEY (Source, SourceFundID)
);'''
        # Leave out "T_CompanyType" as we only want the "Management Firm" 
    cursor.execute(sql)
    db.commit()
    
    # EurekaCharacteristics TABLE
    # SourceFundID is E_FundID
    sql = '''CREATE TABLE IF NOT EXISTS EurekaCharacteristics (
        Source CHAR(1) NOT NULL,
        SourceFundID VARCHAR(50) NOT NULL,
        
        "E_FundName" VARCHAR(255),
        "E_DateAdded" DATE,
        "E_Flagship" VARCHAR(3),
        "E_Closed" VARCHAR(3),
        "E_Limited" VARCHAR(3),
        "E_Dead" VARCHAR(3),
        "E_DeadDate" DATE,
        "E_DeadReason" VARCHAR(255),
        
        "E_1MPriorReturn_pc" FLOAT,
        "E_2MPriorReturn_pc" FLOAT,
        "E_3MPriorReturn_pc" FLOAT,
        "E_AnnualisedReturn_pc" FLOAT,
        "E_BestMonthlyReturn_pc" FLOAT,
        "E_WorstMonthlyReturn_pc" FLOAT,
        "E_5YPriorReturn_pc" FLOAT,
        "E_4YPriorReturn_pc" FLOAT,
        "E_3YPriorReturn_pc" FLOAT,
        "E_2YPriorReturn_pc" FLOAT,
        "E_1YPriorReturn_pc" FLOAT,
        "E_ReturnSinceInception_pc" FLOAT,
        "E_Last3Months_pc" FLOAT,
        "E_OneYearRollingReturn_pc" FLOAT,
        "E_TwoYearRollingReturn_pc" FLOAT,
        "E_FiveYearRollingReturn_pc" FLOAT,
        "E_SharpeRatio" FLOAT,
        "E_AnnualisedStandardDeviation_pc" FLOAT,
        "E_DownsideDeviation_pc" FLOAT,
        "E_UpsideDeviation_pc" FLOAT,
        "E_SortinoRatio" FLOAT,
        "E_MaximumDrawdown_pc" FLOAT,
        "E_PercentaageOfPositiveMonths_pc" FLOAT,
        "E_VaR90_pc" FLOAT,
        "E_Var95_pc" FLOAT,
        "E_Var99_pc" FLOAT,
        
        "E_MainInvestmentStrategy" VARCHAR(255),
        "E_SecondaryInvestmentStrategy" VARCHAR(255),
        "E_GeographicalMandate" VARCHAR(255),
        "E_FundSizeUSDm" FLOAT,
        "E_FundCapacityUSDm" FLOAT,
        "E_FirmsTotalAssetsUSDm" FLOAT,
        "E_TotalAssetsInHedgeFundsUSDm" FLOAT,
        "E_AuMUpdateDate" DATE,
        "E_InceptionDate" DATE,
        "E_Domicile" VARCHAR(50),
        "E_Currency" VARCHAR(50),
        "E_DividendPolicy" VARCHAR(255),
        "E_HurdleRate" VARCHAR(255),
        "E_HighWaterMark" VARCHAR(255),
        "E_ListedOnExchange" VARCHAR(3),
        "E_ExchangeName" VARCHAR(255),
        "E_MinimumInvestmentCurrency" VARCHAR(50),
        "E_MinimumInvestmentSize" VARCHAR(255),
        "E_SubsequentInvestmentCurrency" VARCHAR(50),
        "E_SubsequentInvestmentSize" VARCHAR(255),
        "E_Leverage" VARCHAR(255),
        "E_AccountingMethodForPerformanceFees" VARCHAR(255),
        "E_AnnualizedTargetReturn" VARCHAR(255),
        "E_AnnualizedTargetVolatility" VARCHAR(255),
        "E_MinNetExposure" FLOAT,
        "E_MaxNextExposure" FLOAT,
        "E_MinGrossExposure" FLOAT,
        "E_MaxGrossExposure" FLOAT,
        "E_InvestInPrivatePlacesments" VARCHAR(3),
        "E_ManagedAccountsOffered" VARCHAR(3),
        "E_UCITSCompliant" VARCHAR(3),
        "E_HMRCReportingStatus" VARCHAR(3),
        "E_SECException" VARCHAR(50),
        
        "E_WomenOwnedMinorityOwned" VARCHAR(3),
        
        "E_AdvisoryCompany" VARCHAR(255),
        "E_ManagementCompany" VARCHAR(255),
        "E_Country" VARCHAR(50),
        
        "E_YearOfIncorporation" INTEGER,
        "E_SECRegisteredFirm" VARCHAR(3),
        
        "E_EurekahedgeID" VARCHAR(50),
        "E_ISIN" VARCHAR(50),
        "E_SEDOL" VARCHAR(50),
        "E_Valoren" VARCHAR(50),
        "E_CUSIP" VARCHAR(50),
        "E_Bloomberg" VARCHAR(50),
        "E_Reuters" VARCHAR(50),
        
        "E_SubscriptionFrequency" VARCHAR(255),
        "E_SubscriptionNotificationPeriod" VARCHAR(255),
        "E_RedemptionFrequency" VARCHAR(255),
        "E_RedemptionNotificationPeriod" VARCHAR(255), 
        "E_LockUp" VARCHAR(255),
        "E_Penalty" VARCHAR(255),
        "E_KeyManClause" VARCHAR(255),
        "E_ManagementFee_pc" VARCHAR(255),
        "E_PerformanceFee_pc" VARCHAR(255),
        "E_OtherFee_pc" VARCHAR(255),
        
        "E_IndustryFocus" VARCHAR(255),
        
        "E_CountryFocus" VARCHAR(255),
        
        "E_Administrator" VARCHAR(255),
        "E_Auditor" VARCHAR(255),
        "E_Custodian" VARCHAR(255),
        "E_PrincipalPrimeBrokerBroker" VARCHAR(255),
        "E_SecondaryPrimeBrokerBroker" VARCHAR(255),
        
        "E_SyntheticPrimeBroker" VARCHAR(255),
        "E_LegalAdvisorOffshore" VARCHAR(255),
        "E_LegalAdvisorOnshore" VARCHAR(255),
        "E_RiskPlatform" VARCHAR(255),
        
        "E_ManagerProfile" VARCHAR(255),
        "E_Strategy" VARCHAR(255),
        
        PRIMARY KEY (Source, SourceFundID)
);'''
    cursor.execute(sql)
    db.commit()
    
    # SourceCharacteristics is a VIEW (outer join), not a pure TABLE. 
    # Create it later once the TASS and Eureka tables are populated.
    # SQLite does not have full outer join, so use union of two left outer joins instead.
    # Join on two-variable primary key Source and SourceFundID
    #### ALERT: IT DOES NOT WORK, WILL REMOVE IT.
    sql = '''CREATE VIEW "SourceCharacteristics" AS

SELECT * FROM TASSCharacteristics 
LEFT OUTER JOIN EurekaCharacteristics 
ON TASSCharacteristics.Source = EurekaCharacteristics.Source 
    AND TASSCharacteristics.SourceFundID = EurekaCharacteristics.SourceFundID

UNION

SELECT * FROM EurekaCharacteristics 
LEFT OUTER JOIN TASSCharacteristics 
ON TASSCharacteristics.Source = EurekaCharacteristics.Source 
   AND TASSCharacteristics.SourceFundID = EurekaCharacteristics.SourceFundID
;'''
    # Use this code later...
    
    # MergedCharacteristics TABLE
    # May need to change this later
    sql = '''CREATE TABLE IF NOT EXISTS MergedCharacteristics (
        Source CHAR(1) NOT NULL,
        SourceFundID VARCHAR(50) NOT NULL,
        
        FundName VARCHAR(255) NOT NULL,
        Currency VARCHAR(50) NOT NULL, 
        CompanyName VARCHAR(255) NOT NULL,
        CompanyID INTEGER NOT NULL,
        ManagementFee VARCHAR(255) NOT NULL,
        IncentiveFee VARCHAR(255) NOT NULL,
        LockUp VARCHAR(255) NOT NULL,
        Notice VARCHAR(255) NOT NULL,
        HWM VARCHAR(255) NOT NULL,
        Leverage VARCHAR(255) NOT NULL,
        MinimumInvestment VARCHAR(255) NOT NULL,
        RedemptionFrequency VARCHAR(255) NOT NULL,
        SubscriptionFrequency VARCHAR(255) NOT NULL,
        Strategy VARCHAR(255) NOT NULL,
        Domicile VARCHAR(50) NOT NULL,
        Closed INTEGER NOT NULL,
        Liquidated INTEGER NOT NULL,
        
        StdCompanyName VARCHAR(255), 
        MergedFundID VARCHAR(50), 
        LongestHist VARCHAR(1),
        
        PRIMARY KEY (Source, SourceFundID)
);'''
    cursor.execute(sql)
    db.commit()


except Exception as e:
    db.rollback()
    raise e
finally:
    db.close()