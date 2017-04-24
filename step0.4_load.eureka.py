#!/usr/bin/python    
# Step 0.3
# Load: Read Eureka from text to database
# Change first three variables True/False to trigger each part of this script.
# Note: the ROR and AUM data in Excel was split into two text files for ease here.

import ConfigParser
import os
import csv
import sqlite3

doCharacteristics = True 
doROR = True 
doAUM = True

MONTHS = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12' }
# Convert date from 'MMM YY' to 'YYYY-MM'
def convertFromEurekaDate(dateIn) :
    yy = dateIn[4:6]
    mmm = dateIn[0:3]
    yyyy = '20' + yy if int(yy) < 90 else '19' + yy
    mm = MONTHS[mmm]
    return yyyy + '-' + mm

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')
print("Opening database at: " + dbPath)
fundID = ''

try:
    db=sqlite3.connect(dbPath)
    print ("Database created and opened successfully.")
    
    # Fill in tables...
    cursor = db.cursor()
    
    sql = "SELECT count(*) FROM EurekaCharacteristics;"
    cursor.execute(sql)
    print ("What entries already exist, if any?")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    if doCharacteristics:
        characteristicsPath = config.get('SourceFiles', 'source.eureka.funddetails')
        
        cursor.execute("DELETE FROM EurekaCharacteristics;")
        with open(characteristicsPath, 'r') as characteristicsFile:
            rows = csv.reader(characteristicsFile, delimiter='\t')
            next(rows) # skip sub heading line
            next(rows) # skip header
            next(rows) # skip blank line ?
            for row in rows:
                fundID = row[0]
                if fundID == '':
                    continue
                #Fund ID	Fund Name	Date Added 	Flagship	Closed	Limited	Dead	Dead Date	Dead Reason	Aug -16 Return(%)	Jul -16 Return(%)	Jun -16 Return(%)	Annualised Return (%)	Best Monthly Return (%)	Worst Monthly Return (%)	2012 Return(%)	2013 Return(%)	2014 Return(%)	2015 Return(%)	2016 Return(%)	Return Since Inception (%)	Last 3 Months (%)	One Year Rolling Return (%)	Two Year Rolling Return (%)	Five Year Rolling Return (%)	Sharpe Ratio 	Annualised Standard Deviation (%)	Downside Deviation (%)	Upside Deviation (%)	Sortino Ratio	Maximum Drawdown (%)	Percentage of Positive Months (%)	VaR (90%)	VaR (95%)	VaR (99%)	Main Investment Strategy	Secondary Investment Strategy	Geographical Mandate	Fund Size (US$m)	Fund Capacity (US$m)	Firm's Total Assets (US$m)	Total Assets in Hedge Funds (US$m) 	AuM Update Date	Inception Date	Domicile	Currency	Dividend Policy	Hurdle Rate	High Water Mark	Listed on Exchange	Exchange Name	Minimum Investment Currency	Minimum Investment Size	Subsequent Investment Currency	Subsequent Investment Size	Leverage	Accounting Method for Performance Fees	Annualized Target Return	Annualized Target Volatility	Min Net Exposure	Max Net Exposure	Min Gross Exposure	Max Gross Exposure	Invest In Private Placements	Managed Accounts Offered	UCITS Compliant	HMRC Reporting Status	SEC Exemption	Women-Owned/Minority-Owned	Advisory Company 	Management Company	Country	Year of Incorporation	SEC Registered Firm	Eurekahedge ID	ISIN	SEDOL	Valoren	CUSIP	Bloomberg 	Reuters	Subscription Frequency	Subscription Notification Period	Redemption Frequency	Redemption Notification Period	Lock-up	Penalty	Key Man Clause	Management Fee(%)	Performance Fee(%)	Other Fee(%)	Industry Focus	Country Focus	Administrator	Auditor	Custodian	Principal Prime Broker/ Broker	Secondary Prime Broker/ Broker	Synthetic Prime Broker	Legal Advisor (Offshore)	Legal Advisor (Onshore)	Risk Platform	Manager Profile 	Strategy
                
                sql = u'''INSERT INTO EurekaCharacteristics (
                    Source,
                    SourceFundID,
                    
                    "E_FundName" ,
                    "E_DateAdded" ,
                    "E_Flagship",
                    "E_Closed" ,
                    "E_Limited" ,
                    "E_Dead" ,
                    "E_DeadDate" ,
                    "E_DeadReason" ,
                    
                    "E_1MPriorReturn_pc" ,
                    "E_2MPriorReturn_pc" ,
                    "E_3MPriorReturn_pc" ,
                    "E_AnnualisedReturn_pc" ,
                    "E_BestMonthlyReturn_pc" ,
                    "E_WorstMonthlyReturn_pc" ,
                    "E_5YPriorReturn_pc" ,
                    "E_4YPriorReturn_pc" ,
                    "E_3YPriorReturn_pc" ,
                    "E_2YPriorReturn_pc" ,
                    "E_1YPriorReturn_pc" ,
                    "E_ReturnSinceInception_pc" ,
                    "E_Last3Months_pc" ,
                    "E_OneYearRollingReturn_pc" ,
                    "E_TwoYearRollingReturn_pc" ,
                    "E_FiveYearRollingReturn_pc" ,
                    "E_SharpeRatio" ,
                    "E_AnnualisedStandardDeviation_pc" ,
                    "E_DownsideDeviation_pc" ,
                    "E_UpsideDeviation_pc" ,
                    "E_SortinoRatio" ,
                    "E_MaximumDrawdown_pc" ,
                    "E_PercentaageOfPositiveMonths_pc" ,
                    "E_VaR90_pc" ,
                    "E_Var95_pc" ,
                    "E_Var99_pc" ,
                    
                    "E_MainInvestmentStrategy" ,
                    "E_SecondaryInvestmentStrategy" ,
                    "E_GeographicalMandate" ,
                    "E_FundSizeUSDm" ,
                    "E_FundCapacityUSDm" ,
                    "E_FirmsTotalAssetsUSDm" ,
                    "E_TotalAssetsInHedgeFundsUSDm" ,
                    "E_AuMUpdateDate" ,
                    "E_InceptionDate" ,
                    "E_Domicile",
                    "E_Currency" ,
                    "E_DividendPolicy",
                    "E_HurdleRate" ,
                    "E_HighWaterMark" ,
                    "E_ListedOnExchange" ,
                    "E_ExchangeName" ,
                    "E_MinimumInvestmentCurrency" ,
                    "E_MinimumInvestmentSize" ,
                    "E_SubsequentInvestmentCurrency" ,
                    "E_SubsequentInvestmentSize" ,
                    "E_Leverage" ,
                    "E_AccountingMethodForPerformanceFees" ,
                    "E_AnnualizedTargetReturn" ,
                    "E_AnnualizedTargetVolatility" ,
                    "E_MinNetExposure" ,
                    "E_MaxNextExposure" ,
                    "E_MinGrossExposure" ,
                    "E_MaxGrossExposure" ,
                    "E_InvestInPrivatePlacesments" ,
                    "E_ManagedAccountsOffered" ,
                    "E_UCITSCompliant" ,
                    "E_HMRCReportingStatus" ,
                    "E_SECException" ,
                    
                    "E_WomenOwnedMinorityOwned" ,
                    
                    "E_AdvisoryCompany" ,
                    "E_ManagementCompany" ,
                    "E_Country" ,
                    
                    "E_YearOfIncorporation" ,
                    "E_SECRegisteredFirm" ,
                    
                    "E_EurekahedgeID" ,
                    "E_ISIN" ,
                    "E_SEDOL" ,
                    "E_Valoren" ,
                    "E_CUSIP" ,
                    "E_Bloomberg" ,
                    "E_Reuters" ,
                    
                    "E_SubscriptionFrequency" ,
                    "E_SubscriptionNotificationPeriod" ,
                    "E_RedemptionFrequency" ,
                    "E_RedemptionNotificationPeriod" , 
                    "E_LockUp" ,
                    "E_Penalty" ,
                    "E_KeyManClause" ,
                    "E_ManagementFee_pc" ,
                    "E_PerformanceFee_pc" ,
                    "E_OtherFee_pc" ,
                    
                    "E_IndustryFocus" ,
                    
                    "E_CountryFocus" ,
                    
                    "E_Administrator" ,
                    "E_Auditor" ,
                    "E_Custodian" ,
                    "E_PrincipalPrimeBrokerBroker" ,
                    "E_SecondaryPrimeBrokerBroker" ,
                    
                    "E_SyntheticPrimeBroker" ,
                    "E_LegalAdvisorOffshore" ,
                    "E_LegalAdvisorOnshore" ,
                    "E_RiskPlatform" ,
                    
                    "E_ManagerProfile" ,
                    "E_Strategy" 
                    ) VALUES ( "E" '''
                for cell in row:
                    sql = sql + u', ?' # don't use cell here, escape probs
                sql = sql + u'''
                    );'''
                args = tuple(row)
                db.text_factory = str
                cursor.execute(sql, args)
            db.commit()
            sql = "SELECT count(*) FROM EurekaCharacteristics;"
            cursor.execute(sql)
            print ("How many EurekaCharacteristics rows exist?")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
    
    else: # not doCharacteristics
        sql = "SELECT count(*) FROM EurekaCharacteristics;"
        cursor.execute(sql)
        print ("How many EurekaCharacteristics rows exist?")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        sql = "SELECT * FROM EurekaCharacteristics  LIMIT 1;"
        cursor.execute(sql)
        print ("One row in EurekaCharacteristics:")
        reply = cursor.fetchone()
        names = [description[0] for description in cursor.description]
        print (names)
        print(reply)
    
    if doROR:
        # The returns sheet in Excel comes with ROR/AUM alternating lines.
        # They have been separated into two text files for ease of importing.
        # The separation was done in Excel using filters and exporting.
        
        rorPath =  config.get('SourceFiles', 'source.eureka.return')
        
        cursor.execute("DELETE FROM RateOfReturn WHERE Source = \"E\";")
        with open(rorPath, 'r') as rorFile:
            #[0]	[1]Fund ID	[2]Fund Name	[3]Aug 16	Jul 16	...May 94	Apr 94
            #Return	33992	168 Growth Fund LP				...	-6.45	4.6
            rows = csv.reader(rorFile, delimiter='\t')
            # Get dates from header row, cells 3 to end = rows[0][3:]
            dates = []
            firstRow = True
            for row in rows:
                if firstRow:
                    for eurekaDate in row[3:]:
                        dates.append(convertFromEurekaDate(eurekaDate))
                    #print(dates)
                    firstRow = False
                    continue
                fundID = row[1]
                if fundID == '':
                    continue
                sql = 'INSERT INTO RateOfReturn (Source, SourceFundID'
                for d in dates:
                    sql = sql + ', "' + d + '"'
                sql = sql + ') VALUES ("E", "' + fundID + '"'
                for cell in row[3:]:
                    v = 'NULL' if cell=='' else '"' + cell + '"'
                    sql = sql + ', ' + v
                sql = sql + ');'
                cursor.execute(sql)
            db.commit()
            sql = "SELECT count(*) FROM RateOfReturn WHERE Source = \"E\";"
            cursor.execute(sql)
            print ("How many RateOfReturn E rows exist?")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
                #...
        # ...
    else: # not doROR
        sql = "SELECT count(*) FROM RateOfReturn WHERE Source = \"E\";"
        cursor.execute(sql)
        print ("How many RateOfReturn E rows exist?")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    
    if doAUM:
        # The returns sheet in Excel comes with ROR/AUM alternating lines.
        # They have been separated into two text files for ease of importing.
        # The separation was done in Excel using filters and exporting.
        
        aumPath =  config.get('SourceFiles', 'source.eureka.aum')
        
        cursor.execute("DELETE FROM AUM WHERE Source = \"E\";")
        with open(aumPath, 'r') as aumFile:
            #[0]	[1]Fund ID	[2]Fund Name	[3]Aug 16	Jul 16	...May 94	Apr 94
            #AUM	33992	168 Growth Fund LP				...	-6.45	4.6
            rows = csv.reader(aumFile, delimiter='\t')
            # Get dates from header row, cells 3 to end = rows[0][3:]
            dates = []
            firstRow = True
            for row in rows:
                if firstRow:
                    for eurekaDate in row[3:]:
                        dates.append(convertFromEurekaDate(eurekaDate))
                    #print(dates)
                    firstRow = False
                    continue
                fundID = row[1]
                if fundID == '':
                    continue
                sql = 'INSERT INTO AUM (Source, SourceFundID'
                for d in dates:
                    sql = sql + ', "' + d + '"'
                sql = sql + ') VALUES ("E", "' + fundID + '"'
                for cell in row[3:]:
                    v = 'NULL' if cell=='' else '"' + cell + '"'
                    sql = sql + ', ' + v
                sql = sql + ');'
                cursor.execute(sql)
            db.commit()
            sql = "SELECT count(*) FROM AUM WHERE Source = \"E\";"
            cursor.execute(sql)
            print ("How many AUM E rows exist?")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
                #...
        # ...
    else: # not doAUM
        sql = "SELECT count(*) FROM AUM WHERE Source = \"E\";"
        cursor.execute(sql)
        print ("How many AUM E rows exist?")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    

except Exception as e:
    db.rollback()
    import sys
    print 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno)
    print 'fundID = ' + fundID
    raise e
finally:
    db.close()