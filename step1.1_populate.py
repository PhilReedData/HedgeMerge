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
            "T_CompanyName" ,
            
            
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
        WHERE SourceFundID = '42464';''' # TEMP LIMIT
        cursor.execute(sql)
        rows = cursor.fetchall()
        names = [description[0] for description in cursor.description]
        print names
        
        sourceI = names.index('Source')
        sourceFundIDI = names.index('SourceFundID')
        # index of variable in source T or E
        fundNameTi = names.index('T_Name')
        fundNameEi = names.index('E_FundName')
        print ('fundNameEi = ' + str(fundNameEi))
        currencyTi = names.index('T_CurrencyCode')
        currencyEi = names.index('E_Currency')
        companyNameTi = names.index('T_CompanyName')
        companyNameEi = names.index('E_ManagementCompany')
        companyIDTi = names.index('T_CompanyID')
        managementFeeTi = names.index('T_ManagementFee')
        managementFeeEi = names.index('E_ManagementFee_pc')
        incentiveFeeTi = names.index('T_IncentiveFee')
        incentiveFeeEi = names.index('E_PerformanceFee_pc')
        lockUpTi = names.index('T_LockUpPeriod')
        lockUpEi = names.index('E_LockUp')
        noticeTi = names.index('T_RedemptionNoticePeriod')
        noticeEi = names.index('E_RedemptionNotificationPeriod')
        hwmTi = names.index('T_HighWaterMark')
        hwmEi = names.index('E_HighWaterMark')
        leverageTi = names.index('T_Leveraged')
        leverageEi = names.index('E_Leverage')
        minimumInvestmentTi = names.index('T_MinimumInvestment')
        minimumInvestmentEi = names.index('E_MinimumInvestmentSize')
        redemptionFrequencyTi = names.index('T_RedemptionFrequency')
        redemptionFrequencyEi = names.index('E_RedemptionFrequency')
        subscriptionFrequencyTi = names.index('T_SubscriptionFrequency')
        subscriptionFrequencyEi = names.index('E_SubscriptionFrequency')
        strategyTi = names.index('T_PrimaryCategory')
        strategyEi = names.index('E_MainInvestmentStrategy')
        domicileTi = names.index('T_DomicileCountry')
        domicileEi = names.index('E_Domicile')
        openTi = names.index('T_OpenToPublic')
        closedEi = names.index('E_Closed')
        liquidatedTi = names.index('T_Dead')
        liquidatedEi = names.index('E_Dead')
        
        for i, row in enumerate(rows):
            print (row)
            # Do the merging
            ## Get the data from row
            source = row[sourceI]
            sourceFundID = row[sourceFundIDI] # etc
            tass = True if source == 'T' else False
            ## Do the merging
            fundName = row[fundNameTi] if tass else row[fundNameEi]
            print('fundName = ' + str(fundName))
            currency = row[currencyTi] if tass else row[currencyEi]
            companyName = row[companyNameTi] if tass else row[companyNameEi]
            # Eureka has no companyID, use the row index instead.
            companyID = row[companyIDTi] if tass else str(i+1)
            managementFee = row[managementFeeTi] if tass else row[managementFeeEi]
            incentiveFee = row[incentiveFeeTi] if tass else row[incentiveFeeEi]
            lockUp = row[lockUpTi] if tass else row[lockUpEi]
            notice = row[noticeTi] if tass else row[noticeEi]
            hwm = row[hwmTi] if tass else row[hwmEi]
            leverage = row[leverageTi] if tass else row[leverageEi]
            minimumInvestment = row[minimumInvestmentTi] if tass else row[minimumInvestmentEi]
            redemptionFrequency = row[redemptionFrequencyTi] if tass else row[redemptionFrequencyEi]
            subscriptionFrequency = row[subscriptionFrequencyTi] if tass else row[subscriptionFrequencyEi]
            strategy = row[strategyTi] if tass else row[strategyEi]
            domicile = row[domicileTi] if tass else row[domicileEi]
            closed = ''
            if tass:
                t_open = row[openTi]
                # Invert open to closed for TASS
                closed = '1' if t_open == '0' else '0'
            else:
                e_closed = row[closedEi]
                closed = '1' if e_closed == 'Yes' else '0'
            liquidated = ''
            if tass:
                liquidated = row[liquidatedTi]
            else:
                liquidated = '1' if row[liquidatedEi] == 'Yes' else '0'
            
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
                    print ('Empty field ' + str(j) + ' in row ' + str(i))
                sql = sql + ', "' + str(field) + '"'
                print ('field ' + str(j) + ': ' + field)
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