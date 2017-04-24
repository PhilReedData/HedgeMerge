#!/usr/bin/python    
# Step 0.5
# Load: Create the View of both characteristics tables together.
## NOTE: Although this appears to work, data end up in the wrong columns. DROP THIS STAGE!

import ConfigParser
import sqlite3

doCreateView = True

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')
print("Opening database at: " + dbPath)

try:
    db=sqlite3.connect(dbPath)
    print ("Database created and opened successfully.")
    
    # Fill in tables...
    cursor = db.cursor()
    
    sql = "SELECT count(*) FROM TASSCharacteristics;"
    cursor.execute(sql)
    print ("How many rows in TASSCharacteristics?")
    reply = cursor.fetchone()
    print(reply)
    
    sql = "SELECT count(*) FROM EurekaCharacteristics;"
    cursor.execute(sql)
    print ("How many rows in EurekaCharacteristics?")
    reply = cursor.fetchone()
    print(reply)

    # SourceCharacteristics is a VIEW (outer join), not a pure TABLE. 
    # Create it later once the TASS and Eureka tables are populated.
    # SQLite does not have full outer join, so use union of two left outer joins instead.
    # Join on two-variable primary key Source and SourceFundID
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
    if doCreateView:
        cursor.execute(sql)
        sql = "SELECT count(*) FROM SourceCharacteristics;"
        cursor.execute(sql)
        print ("How many rows in SourceCharacteristics?")
        reply = cursor.fetchone()
        print(reply)
        db.commit()
    else:
        sql = "SELECT * FROM SourceCharacteristics WHERE Source = \"T\" LIMIT 1;"
        cursor.execute(sql)
        print ("First T row in SourceCharacteristics:")
        reply = cursor.fetchone()
        print(reply)
        sql = "SELECT * FROM SourceCharacteristics WHERE Source = \"E\" LIMIT 1;"
        cursor.execute(sql)
        print ("First E row in SourceCharacteristics:")
        reply = cursor.fetchone()
        print(reply)
        db.commit()

except Exception as e:
    db.rollback()
    import sys
    print 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno)
    raise e
finally:
    db.close()