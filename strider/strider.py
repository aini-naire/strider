import os
import struct
from enum import Enum
from dataclasses import dataclass, fields, field
from typing import Union
from datetime import datetime
import math

CURRENT_REVISION = 0
from strider.io import StriderFileIO, StriderFileUtil
from strider.database import DatabaseHandler
from strider.archive import ArchiveHandler
from strider.exceptions import *

from strider.datatypes import StriderStruct, Database, DatabaseArchive, ArchiveFile, ArchiveKey, ArchiveIndex, ARCHIVE_RANGE


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

    def __init__(self, handler: DatabaseHandler, fileUtil: StriderFileUtil) -> None:
        self.databaseHandler = handler
        self.fileUtil = fileUtil
        self.loadedArchives = handler.loadArchives()
        
            
    def _getArchiveForDate(self, datetime: datetime) -> Union[None | ArchiveHandler]:
        archiveKey = self.databaseHandler._getArchiveKey(datetime)

        if archiveKey in self.loadedArchives:
            return self.loadedArchives[archiveKey]
        
        if self.databaseHandler.hasArchive(archiveKey):
            archive = self.databaseHandler.loadArchive(archiveKey)
            self.loadedArchives[archiveKey] = archive
            return archive
        
        return None

    def _getOrCreateArchive(self, datetime: datetime) -> ArchiveHandler:
        archive = self._getArchiveForDate(datetime)
        if archive is None:
            archive = self.databaseHandler.createArchive(datetime)
        return archive
    

    def query(self, start: datetime, end: datetime, keys: list[str]) -> list:
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


    def add(self, time: datetime, data: dict) -> None:
        """Adds an entry to the database."""
        archive = self._getOrCreateArchive(time)
        
        archive.writeRecords([(int(time.timestamp, **data.values()))])


    def bulkAdd(self, ingest: dict) -> None:
        """Add data to Database in bulk. 
        This function ingests data as a dictionary `datetime:{key:value}`
        keys already must exist in the database
        TODO transform dictionary to record sequence
        TODO check if archive contains key"""
        time:datetime = next(iter(ingest))
        archive = self._getOrCreateArchive(time)
        archiveKey = self.databaseHandler._getArchiveKey(time)
        recordsQueue = []
        dataDict:dict

        for time, dataDict in ingest.items():
            iterationArchiveKey = self.databaseHandler._getArchiveKey(time)

            if iterationArchiveKey != archiveKey:
                archive.writeRecords(recordsQueue)
                recordsQueue = []
                archive = self._getOrCreateArchive(time)
                archiveKey = iterationArchiveKey
            recordsQueue.append((int(time.timestamp()), *dataDict.values()))
        
        archive.writeRecords(recordsQueue)
            