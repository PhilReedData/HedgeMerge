REM export the contents of the database using SQLite localling installed
C:\Work\SQLite\sqlite-tools-win32-x86-3150100\sqlite3.exe -header -separator "	" "S:\A&F\Finance Group\AllianceGroup\Tools\AllHedge\HedgeMerge1.db" "SELECT * FROM MergedCharacteristics;" > export\MergedCharacteristics.txt
C:\Work\SQLite\sqlite-tools-win32-x86-3150100\sqlite3.exe -header -separator "	" "S:\A&F\Finance Group\AllianceGroup\Tools\AllHedge\HedgeMerge1.db" "SELECT * FROM MergedCharacteristics2;" > export\MergedCharacteristics2.txt
C:\Work\SQLite\sqlite-tools-win32-x86-3150100\sqlite3.exe -header -separator "	" "S:\A&F\Finance Group\AllianceGroup\Tools\AllHedge\HedgeMerge1.db" "SELECT * FROM MergedCharacteristics3;" > export\MergedCharacteristics3.txt
pause
