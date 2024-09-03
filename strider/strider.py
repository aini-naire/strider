import os
import struct
from enum import Enum
from dataclasses import dataclass, fields, field
from typing import Union
from datetime import datetime
import math

CURRENT_REVISION = 0

from strider.datatypes import StriderStruct, Database, DatabaseArchive, ArchiveFile, ArchiveKey, ArchiveIndex, ARCHIVE_RANGE
from strider.io import StriderFileIO, StriderFileUtil
from strider.database import DatabaseHandler
from strider.exceptions import *


class DatabaseManager():
    """The DatabaseManager is responsible for creating, loading, checking and repariring databases"""
    def load(self, baseDir: str, name: str) -> None:
        """Loads Strider database
        TODO integrity checks and errors"""
        fileUtil = StriderFileUtil(baseDir, name)
        try:
            databaseFile = StriderFileIO(open(fileUtil.getDatabaseFilepath(), "rb"))
        except FileNotFoundError:
            raise DatabaseNotFound

        database: Database = databaseFile.readStruct(Database)
        database.archives = databaseFile.readStructSequence(DatabaseArchive, database.archiveCount)
        database.keys = databaseFile.readStructSequence(ArchiveKey, database.keyCount)

        return DatabaseSession(DatabaseHandler(database, fileUtil), fileUtil)
    

    def new(self, baseDir: str, name: str, range: ARCHIVE_RANGE = ARCHIVE_RANGE.week) -> None:
        """Creates new Strider database"""
        fileUtil = StriderFileUtil(baseDir, name)
        if os.path.isdir(fileUtil.databaseDirectory):
            raise DatabaseExists
        else:
            database = Database("strdrdb", CURRENT_REVISION, name, 0, 0, 3600, range, [], [])
            
            os.mkdir(fileUtil.databaseDirectory)
            databaseHandler = DatabaseHandler(database, fileUtil)
            databaseHandler.save()
            return DatabaseSession(databaseHandler, fileUtil)

        
class DatabaseSession():
    """"""
    databaseHandler: DatabaseHandler
    fileUtil: StriderFileUtil
    loadedArchives: dict = {}

    def __init__(self, handler: DatabaseHandler, fileUtil) -> None:
        self.databaseHandler = handler
        self.fileUtil = fileUtil
        self.loadedArchives = handler.loadArchives()
        
            
    def _getArchiveForDate(self, datetime: datetime):
        timestamp = int(datetime.timestamp())
        archiveKey = self.databaseHandler._getArchiveKey(datetime, timestamp)

        if archiveKey in self.loadedArchives:
            return self.loadedArchives[archiveKey]
        
        if self.databaseHandler.hasArchive(archiveKey):
            archive = self.databaseHandler.loadArchive(archiveKey)
            self.loadedArchives[archiveKey] = archive
            return archive
        
        return None

    def _getOrCreateArchive(self, time):
        archive = self._getArchiveForDate(time)
        if archive is None:
            archive = self.databaseHandler.createArchive(time)
        return archive
    

    def query(self, start: datetime, end: datetime, keys: list[str]):
        results = []
        startTimestamp = int(start.timestamp())
        endTimestamp = int(end.timestamp())
        archivePeriod = self.databaseHandler._getArchivePeriod(start) #IDK if this will work for multiple month queries
        archiveCount = math.ceil((endTimestamp-startTimestamp)/archivePeriod)
        #print(startTimestamp, endTimestamp, archivePeriod, archiveCount)

        for archiveI in range(archiveCount):
            #print("hi")
            archive = self._getArchiveForDate(datetime.fromtimestamp(startTimestamp + (archiveI * archivePeriod)))
            results+=archive.readRecords(startTimestamp, endTimestamp)

        return results


    def add(self, time: datetime, data: dict):
        """Adds an entry to the database. Note this can only add data to the current archive at a timestamp after the last entry"""
        archive = self._getOrCreateArchive(time)
        
        archive.writeRecords([(int(time.timestamp, **data.values()))])


    def bulkAdd(self, ingest: dict):
        """Add data to Database in bulk. 
        This function ingests data as a dictionary `datetime:{key:value}`
        keys already must exist in the database
        TODO transform dictionary to record sequence
        TODO check if archive contains key"""
        time:datetime = next(iter(ingest))
        archive = self._getOrCreateArchive(time)
        timestamp = int(time.timestamp())
        archiveKey = self.databaseHandler._getArchiveKey(time, timestamp)
        recordsQueue = []
        dataDict:dict

        for time, dataDict in ingest.items():
            timestamp = int(time.timestamp())
            iterationArchiveKey = self.databaseHandler._getArchiveKey(time, timestamp)

            if iterationArchiveKey != archiveKey:
                archive.writeRecords(recordsQueue)
                recordsQueue = []
                archive = self._getOrCreateArchive(time)
                archiveKey = iterationArchiveKey
            recordsQueue.append((timestamp, *dataDict.values()))
        
        archive.writeRecords(recordsQueue)
            