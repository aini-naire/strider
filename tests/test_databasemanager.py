import pytest
import strider
import shutil,os
from datetime import datetime

def testLoad():
    strider.DatabaseManager.new("data/test", "test_tmp")
    assert isinstance(strider.DatabaseManager.load("data/test", "test_tmp"), strider.DatabaseSession)
    shutil.rmtree(os.path.join("data/test", "test_tmp"))
    
def testLoadCorruptedDatabase(database: strider.DatabaseSession):
    database.addKey("testKey", 5)
    data = {datetime(2024, 5, 10, 15, 30, 30): {"testKey": 5.0},
            datetime(2024, 5, 11, 15, 30, 30): {"testKey": 5.0},
            datetime(2024, 5, 12, 15, 30, 30): {"testKey": 5.0},
            datetime(2024, 5, 13, 15, 30, 30): {"testKey": 5.0},}
    database.bulkAdd(data)
    print(database.fileUtil.getDatabaseFilepath())

    with open(database.fileUtil.getDatabaseFilepath(), "rb") as databaseFile:
        databaseFile.seek(0)
        data = databaseFile.read()

    with open(database.fileUtil.getDatabaseFilepath(), "wb") as databaseFile:
        databaseFile.write(data[:int(len(data)/2)])
    
    database:strider.DatabaseSession = strider.DatabaseManager.load("data/test", "test_tmp")
    print(database.databaseHandler.database)
    assert isinstance(database, strider.DatabaseSession)