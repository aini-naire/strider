import pytest
from datetime import datetime
from tests import util
import strider
import struct

def testAddKey(database: strider.DatabaseSession):
    database.addKey("testKey", 5)
    assert database.databaseHandler.getKeys()[0].name == "testKey"

def testAddKeyExisting(database: strider.DatabaseSession):
    database.addKey("testKey", 5)
    with pytest.raises(strider.KeyAlreadyExists):
        database.addKey("testKey", 5)

def testAdd(database: strider.DatabaseSession):
    database.addKey("testKey", 5)

    database.add(datetime(2024, 5, 10, 15, 30, 30), {"testKey": 5.0})
    assert len(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 10, 16, 0))) == 1

def testAddInvalidData(database: strider.DatabaseSession):
    database.addKey("testKey", 5)

    with pytest.raises(struct.error):
        database.add(datetime(2024, 5, 10, 15, 30, 30), {"testKey": None})

    with pytest.raises(ValueError):
        database.add(datetime(2024, 5, 10, 15, 30, 30), {})

    with pytest.raises(struct.error):
        database.add(datetime(2024, 5, 10, 15, 30, 31), {"testKey": "potato"})
    #print(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 10, 16, 0)))
    #assert len(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 10, 16, 0))) == 1

def testAddSequenceViolation(database: strider.DatabaseSession):
    database.addKey("testKey", 5)

    database.add(datetime(2024, 5, 10, 15, 30, 30), {"testKey": 5.0})
    with pytest.raises(strider.SequenceViolation):
        database.add(datetime(2024, 5, 10, 14, 30, 30), {"testKey": 5.0})


def testQuery(database: strider.DatabaseSession):
    database.addKey("testKey", 5)
    database.add(datetime(2024, 5, 10, 15, 30, 30), {"testKey": 5.0})
    assert len(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 10, 16, 0))) == 1

    database.add(datetime(2024, 5, 11, 15, 30, 30), {"testKey": 5.0})
    assert len(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 11, 16, 0))) == 2
    
    database.add(datetime(2024, 5, 11, 23, 59, 30), {"testKey": 5.0})
    database.add(datetime(2024, 5, 12, 0, 1, 30), {"testKey": 5.0})
    assert len(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 11, 16, 0))) == 2
    assert len(database.query(datetime(2024, 5, 11, 22, 0), datetime(2024, 5, 12, 16, 0))) == 2
    

def testQueryMonth(databaseMonth):
    testQuery(databaseMonth)

def testQueryDay(databaseDay):
    testQuery(databaseDay)

def testBulkAdd(database: strider.DatabaseSession):
    database.addKey("testKey", 5)
    data = {datetime(2024, 5, 10, 15, 30, 30): {"testKey": 5.0},
            datetime(2024, 5, 11, 15, 30, 30): {"testKey": 5.0},
            datetime(2024, 5, 12, 15, 30, 30): {"testKey": 5.0},
            datetime(2024, 5, 13, 15, 30, 30): {"testKey": 5.0},}
    database.bulkAdd(data)
    assert len(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 11, 16, 0))) == 2
    assert len(database.query(datetime(2024, 5, 10, 15, 0), datetime(2024, 5, 13, 16, 0))) == 4

def testBulkAddMonth(databaseMonth):
    testBulkAdd(databaseMonth)

def testBulkAddDay(databaseDay):
    testBulkAdd(databaseDay)

def testBulkAddInvalidData(database: strider.DatabaseSession):
    database.addKey("testKey", 5)

    with pytest.raises(struct.error):
        data = {datetime(2024, 5, 10, 15, 30, 30): {"testKey": None},
                datetime(2024, 5, 11, 15, 30, 30): {"testKey": 5.0},
                datetime(2024, 5, 12, 15, 30, 30): {"testKey": 5.0},
                datetime(2024, 5, 13, 15, 30, 30): {"testKey": 5.0},}
        database.bulkAdd(data)

    with pytest.raises(ValueError):
        database.bulkAdd({})

    

    with pytest.raises(AttributeError):
        data = {"date": {"testKey": 5.0},
                datetime(2024, 5, 11, 15, 30, 30): {"testKey": 5.0},
                datetime(2024, 5, 12, 15, 30, 30): {"testKey": 5.0},
                datetime(2024, 5, 13, 15, 30, 30): {"testKey": 5.0},}
        database.bulkAdd(data)