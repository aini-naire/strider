import pytest
import shutil
import os 

from strider import DatabaseManager, datatypes

@pytest.fixture()
def databaseDay():
    database = DatabaseManager.new("data/test", "test_tmp", datatypes.ARCHIVE_RANGE.day)

    yield database

    shutil.rmtree(os.path.join("data/test", "test_tmp"))

@pytest.fixture()
def database():
    database = DatabaseManager.new("data/test", "test_tmp")

    yield database

    shutil.rmtree(os.path.join("data/test", "test_tmp"))

@pytest.fixture()
def databaseMonth():
    database = DatabaseManager.new("data/test", "test_tmp", datatypes.ARCHIVE_RANGE.month)

    yield database

    shutil.rmtree(os.path.join("data/test", "test_tmp"))