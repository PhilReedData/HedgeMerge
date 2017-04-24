#!/usr/bin/python    
# Step 1.1
# Populate: For each row in SourceCharacteristics, populate MergedCharacteristics.

import ConfigParser
import sqlite3

doPopulate = True

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')
print("Opening database at: " + dbPath)

try:
    db=sqlite3.connect(dbPath)
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
        # LOOP For each row in SourceCh, select ... from SourceCh
        #   Do the merging
        #   Put into MergedCh, insert into MergedCh ...  values ...
        # Commit
        
        # Get all columns and all rows from SourceCharacteristics
        sql = '''SELECT 
            Source, 
            SourceFundID,  
            
            "T_Dead" ,  
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
            "T_CompanyName"
            
            
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
            
            FROM SourceCharacteristics
        LIMIT 1 OFFSET 30000;''' # TEMP LIMIT
        cursor.execute(sql)
        rows = cursor.fetchall()
        names = [description[0] for description in cursor.description]
        print names
        for i, row in enumerate(rows):
            print row
            # Do the merging
            ## Get the data from row
            source = row[0]
            sourceFundID = row[1] # etc
            tass = True if source == 'T' else False
            #### WARNING! The order of the columns requested is not observed!
            #### WRONG  wrong wrong.
            
            ## Do the merging
            ## Use ref file 'mergedFieldsExceptFirst.txt' for row number
            # T_Name 3 or E_FundName 54
            fundName = row[3] if tass else row[54]
            # T_CurrencyCode 5 or E_Currency 98
            currency = row[5] if tass else row[98]
            # T_CompanyName 53 or E_ManagementCompany 123
            companyName = row[53] if tass else row[123]
            # T_CompanyID 52 or row index + 1
            companyID = row[52] if tass else str(i+1)
            # T_ManagementFee 19 or E_ManagementFee_pc 141
            managementFee = row[19] if tass else row[141]
            # T_IncentiveFee 20 or E_PerformanceFee_pc 142
            incentiveFee = row[20] if tass else row[142]
            # T_LockUpPeriod 44 or E_LockUp 138
            lockUp = row[44] if tass else row[138]
            # T_RedemptionNoticePeriod 43 or E_RedemptionNotificationPeriod 137
            notice = row[43] if tass else row[137]
            # T_HighWaterMark 22 or E_HighWaterMark 101
            hwm = row[22] if tass else row[101]
            # T_Leveraged 24 or E_Leverage 108
            leverage = row[24] if tass else row[108]
            # T_MinimumInvestment 18 or E_MinimumInvestmentSize 105
            minimumInvestment = row[18] if tass else row[105]
            # T_RedemptionFrequency 42 or E_RedemptionFrequency 136
            redemptionFrequency = row[42] if tass else row[136]
            # T_SubscriptionFrequency 41 or E_SubscriptionFrequency 134
            subscriptionFrequency = row[41] if tass else row[134]
            # T_PrimaryCategory 4 or E_MainInvestmentStrategy 88
            strategy = row[4] if tass else row[88]
            # T_DomicileCountry 51 or E_Domicile 97
            domicile = row[51] if tass else row[97]
            # T_OpenToPublic 35 (invert) or E_Closed 57
            closed = ''
            if tass:
                t_open = row[35]
                closed = '1' if t_open == '0' else '0'
            else:
                e_closed = row[57]
                closed = '1' if e_closed == 'Yes' else '0'
            # T_Dead 2 or E_Dead 59
            liquidated = ''
            if tass:
                liquidated = row[2]
            else:
                liquidated = '1' if row[59] == 'Yes' else '0'
            
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
                print ('field ' + str(j) + ': ' + field)
                sql = sql + ', "' + field + '"'
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
    
    # Else not doPopulate? Nothing. May move the count statement here.
    
except Exception as e:
    db.rollback()
    import sys
    print 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno)
    raise e
finally:
    db.close()