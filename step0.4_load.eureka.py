#!/usr/bin/python    
# Step 0.3
# Load: Read Eureka from text to database

import ConfigParser
import os
import sqlite3

config = ConfigParser.RawConfigParser()
config.read('paths.properties')
dbPath = config.get('DatabaseSection', 'database.dbname')
print("Opening database at: " + dbPath)

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
    
    # ...

except Exception as e:
    db.rollback()
    raise e
finally:
    db.close()