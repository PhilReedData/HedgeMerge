REM export the contents of the database using SQLite localling installed
C:\Work\SQLite\sqlite-tools-win32-x86-3150100\sqlite3.exe -header -separator "	" "S:\A&F\Finance Group\AllianceGroup\Tools\AllHedge\HedgeMerge.db" "SELECT * FROM RateOfReturn;" > export\RateOfReturn.txt
C:\Work\SQLite\sqlite-tools-win32-x86-3150100\sqlite3.exe -header -separator "	" "S:\A&F\Finance Group\AllianceGroup\Tools\AllHedge\HedgeMerge.db" "SELECT * FROM AUM;" > export\AUM.txt
pause
