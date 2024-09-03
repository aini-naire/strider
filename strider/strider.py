import math
import os
from datetime import datetime
from typing import Union

CURRENT_REVISION = 0
from strider.io import StriderFileIO, StriderFileUtil
from strider.database import DatabaseHandler
from strider.archive import ArchiveHandler
from strider.exceptions import *

from strider.datatypes import Database, DatabaseArchive, ArchiveKey, ARCHIVE_RANGE, ARCHIVE_KEY_TYPES

class DatabaseSession:
    """"""
    databaseHandler: DatabaseHandler
    fileUtil: StriderFileUtil
    loadedArchives: dict = {}

    def __init__(self, handler: DatabaseHandler, fileUtil: StriderFileUtil) -> None:
        self.databaseHandler = handler
        self.fileUtil = fileUtil
        self.loadedArchives = handler.loadArchives()

    def _getArchiveForDate(self, date: datetime) -> Union[None | ArchiveHandler]:
        archiveKey = self.databaseHandler.getArchiveKey(date)

        if archiveKey in self.loadedArchives:
            return self.loadedArchives[archiveKey]

        if self.databaseHandler.hasArchive(archiveKey):
            archive = self.databaseHandler.loadArchive(archiveKey)
            self.loadedArchives[archiveKey] = archive
            return archive

        return None

    def _getOrCreateArchive(self, date: datetime) -> ArchiveHandler:
        archive = self._getArchiveForDate(date)
        if archive is None:
            archive = self.databaseHandler.createArchive(date)
        return archive
    
    def _getActiveArchive(self) -> Union[None | ArchiveHandler]:
        date = datetime.now()
        return self._getArchiveForDate(date)

    def query(self, start: datetime, end: datetime, keys: list[str]) -> list:
        results = []
        startTimestamp = int(start.timestamp())
        endTimestamp = int(end.timestamp())
        archivePeriod = self.databaseHandler.getArchivePeriod(start)  # IDK if this will work for multiple month queries
        archiveCount = math.ceil((endTimestamp - startTimestamp) / archivePeriod)

        for archiveI in range(archiveCount):
            archive = self._getArchiveForDate(datetime.fromtimestamp(startTimestamp + (archiveI * archivePeriod)))
            results += archive.readRecords(startTimestamp, endTimestamp)

        return results

    def add(self, time: datetime, data: dict) -> None:
        """Adds an entry to the database."""
        archive = self._getOrCreateArchive(time)

        archive.writeRecords([(int(time.timestamp()), *data.values())])

    def addKey(self, keyName: str, keyType: str) -> None:
        """"Adds keyName to the database keys. This operation only affects current and future archives since the database does not have update operations (yet?)"""
        archiveKey = ArchiveKey(keyName, ARCHIVE_KEY_TYPES(keyType))
        self.databaseHandler.addKey(archiveKey)
        
        activeArchive = self._getActiveArchive()
        if activeArchive:
            activeArchive.addKey(archiveKey)


    def bulkAdd(self, ingest: dict) -> None:
        """Add data to Database in bulk. 
        This function ingests data as a dictionary `datetime:{key:value}`
        keys already must exist in the database
        TODO transform dictionary to record sequence
        TODO check if archive contains key"""
        time: datetime = next(iter(ingest))
        archive = self._getOrCreateArchive(time)
        archiveKey = self.databaseHandler.getArchiveKey(time)
        archivePeriod = self.databaseHandler.getArchivePeriod(time)
        recordsQueue = []
        dataDict: dict

        for time, dataDict in ingest.items():
            timestamp = int(time.timestamp())
            #iterationArchiveKey = self.databaseHandler.getArchiveKey(time)

            if (timestamp > archiveKey+archivePeriod):
                archive.writeRecords(recordsQueue)
                recordsQueue = []
                archive = self._getOrCreateArchive(time)
                archiveKey = self.databaseHandler.getArchiveKey(time)
            recordsQueue.append((timestamp, *dataDict.values()))

        archive.writeRecords(recordsQueue)

class DatabaseManager:
    """The DatabaseManager is responsible for creating, loading, checking and repariring databases"""

    def load(self, baseDir: str, name: str) -> DatabaseSession:
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

    def new(self, baseDir: str, name: str, archiveRange: ARCHIVE_RANGE = ARCHIVE_RANGE.week) -> DatabaseSession:
        """Creates new Strider database"""
        fileUtil = StriderFileUtil(baseDir, name)
        if os.path.isdir(fileUtil.databaseDirectory):
            raise DatabaseExists
        else:
            database = Database("strdrdb", CURRENT_REVISION, name, 0, 0, 3600, archiveRange, [], [])

            os.mkdir(fileUtil.databaseDirectory)
            databaseHandler = DatabaseHandler(database, fileUtil)
            databaseHandler.save()
            return DatabaseSession(databaseHandler, fileUtil)
