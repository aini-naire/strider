import math
import os
from datetime import datetime
from typing import Union
import struct

CURRENT_REVISION = 0
from strider.io import StriderFileIO, StriderFileUtil
from strider.database import DatabaseHandler
from strider.archive import ArchiveHandler
from strider.exceptions import *

from strider.datatypes import Database, DatabaseArchive, ArchiveFile, ArchiveKey, ARCHIVE_RANGE, ARCHIVE_KEY_TYPES


class DatabaseSession:
    """"""
    databaseHandler: DatabaseHandler
    fileUtil: StriderFileUtil
    loadedArchives: dict[int, ArchiveHandler] = {}

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
        archiveHandler = self._getArchiveForDate(date)
        if archiveHandler is None:
            archiveHandler = self.databaseHandler.createArchive(date)
            self.loadedArchives[archiveHandler.archive.minRange] = archiveHandler
        return archiveHandler
    
    def _getActiveArchive(self) -> Union[None | ArchiveHandler]:
        date = datetime.now()
        return self._getArchiveForDate(date)

    def query(self, start: datetime, end: datetime, key: Union[None | str] = None, raw: bool = False, asArrays:bool = False) -> Union[list | dict]:
        """Queries from start to end date. If `key` is set, returns a single key in `{timestamp:keyvalue}` format. If `raw` is set, returns records as tuples"""
        startTimestamp = int(start.timestamp())
        endTimestamp = int(end.timestamp())
        archivePeriod = self.databaseHandler.getArchivePeriod(start)
        startArchive = self.databaseHandler.getArchiveKey(start)
        endArchive = self.databaseHandler.getArchiveKey(end)
        archiveCount = int((endArchive+archivePeriod - startArchive) / archivePeriod)
        
        results = []

        for archiveI in range(archiveCount):
            archive = self._getArchiveForDate(datetime.fromtimestamp(startTimestamp + (archiveI * archivePeriod)))
            if archive:
                results += archive.readRecords(startTimestamp, endTimestamp, key, raw if not asArrays else True)

        if asArrays and len(results):
            keys = ["time", *[key.name for key in (archive.archive.keys if archive else self.databaseHandler.getKeys())]]
            return {keyName: [record[recordIndex] for record in results] for recordIndex, keyName in enumerate(keys)}
        
        return results

    def add(self, time: datetime, data: dict) -> None:
        """Adds an entry to the database."""
        if len(data) == 0:
            raise ValueError("Data is empty")
        
        archive = self._getOrCreateArchive(time)
        databaseKeys = [key.name for key in self.databaseHandler.getKeys()]
        record = [int(time.timestamp())]
        record.extend([data.get(key, 0) for key in databaseKeys])

        archive.writeRecords([record])

    def addKey(self, keyName: str, keyType: str) -> None:
        """"Adds keyName to the database keys. This operation only affects current and future archives since the database does not have update operations (yet?)"""
        archiveKey = ArchiveKey(keyName, 0, ARCHIVE_KEY_TYPES(keyType))
        self.databaseHandler.addKey(archiveKey)
        
        activeArchive = self._getActiveArchive()
        if activeArchive:
            activeArchive.addKey(archiveKey)

    def setIndexInteval(self, inteval: int, full: bool = False) -> None:
        """"Changes database index inteval if ´full´ is False, only re-indexes the current archive"""
        inteval = int(inteval)
        
        if full:
            for archive in self.loadedArchives.values():
                archive.setIndexInteval(inteval)
        else:
            activeArchive = self._getActiveArchive()
            if activeArchive:
                activeArchive.setIndexInteval(inteval)
        self.databaseHandler.setIndexInteval(inteval)
        return True


    def bulkAdd(self, ingest: dict) -> None:
        """Add data to Database in bulk. 
        This function ingests data as a dictionary `datetime:{key:value}`
        keys already must exist in the database
        TODO type safety"""
        if len(ingest) == 0:
            return None
        
        time: datetime = next(iter(ingest))
        archive = self._getOrCreateArchive(time)
        archiveKey = self.databaseHandler.getArchiveKey(time)
        archivePeriod = self.databaseHandler.getArchivePeriod(time)
        databaseKeys = [key.name for key in self.databaseHandler.getKeys()]
        dataDict: dict
        
        recordsQueue = []

        for time, dataDict in ingest.items():
            timestamp = int(time.timestamp())

            if (timestamp > archiveKey+archivePeriod):
                archive.writeRecords(recordsQueue)
                recordsQueue = []
                archive = self._getOrCreateArchive(time)
                archiveKey = self.databaseHandler.getArchiveKey(time)
                archivePeriod = self.databaseHandler.getArchivePeriod(time)

            record = [timestamp]
            record.extend([dataDict.get(key, 0) for key in databaseKeys])
            recordsQueue.append(record)

        archive.writeRecords(recordsQueue)

class DatabaseMultiSession:
    databases: dict = {}
    initialized: bool = False
    directory: str

    def init_app(self, directory, app):
        self.load(directory)
        app.extensions["strider"] = self
    
    def load(self, directory: str) -> None:
        for databaseDirectory in os.listdir(directory):
            self.databases[databaseDirectory] = DatabaseManager.load(directory, databaseDirectory)

        self.initialized = True
        self.directory = directory

    def new(self, name: str) -> DatabaseSession:
        if self.initialized and self.directory:
            database = DatabaseManager.new(self.directory, name)
            self.databases[name] = database
            return database


    def getDatabaseSession(self, databaseName: str) -> DatabaseSession:
        try:
            return self.databases[databaseName]
        except KeyError:
            raise DatabaseNotFound()

class DatabaseManager:
    """The DatabaseManager is responsible for creating, loading, checking and repariring databases"""

    def load(self, baseDir: str, name: str) -> DatabaseSession:
        """Loads Strider database
        TODO integrity checks and errors"""
        fileUtil = StriderFileUtil(baseDir, name)
        try:
            databaseFile = StriderFileIO(open(fileUtil.getDatabaseFilepath(), "rb"))
        except FileNotFoundError:
            raise DatabaseNotFound()

        try:
            database: Database = databaseFile.readStruct(Database)
            database.archives = databaseFile.readStructSequence(DatabaseArchive, database.archiveCount)
            database.keys = databaseFile.readStructSequence(ArchiveKey, database.keyCount)
        except struct.error:
            if os.path.isfile(fileUtil.getDatabaseFilepath()+".old"):
                fileUtil.safeOverwrite(fileUtil.getDatabaseFilepath(), fileUtil.getDatabaseFilepath()+".old")
                return self.load(baseDir, name)
            else:
                database = self.rebuildDatabase(fileUtil)

        return DatabaseSession(DatabaseHandler(database, fileUtil), fileUtil)

    def new(self, baseDir: str, name: str, archiveRange: ARCHIVE_RANGE = ARCHIVE_RANGE.week) -> DatabaseSession:
        """Creates new Strider database"""
        fileUtil = StriderFileUtil(baseDir, name)
        if os.path.isdir(fileUtil.databaseDirectory):
            raise DatabaseExists()
        else:
            database = Database("strdrdb", CURRENT_REVISION, name, 0, 0, 3600, archiveRange, [], [])

            os.mkdir(fileUtil.databaseDirectory)
            databaseHandler = DatabaseHandler(database, fileUtil)
            databaseHandler.save()
            return DatabaseSession(databaseHandler, fileUtil)
        
    def rebuildDatabase(self, fileUtil: StriderFileUtil):
        archives:list[ArchiveFile] = []
        lastArchive = None
        # Read all archives
        for file in os.listdir(fileUtil.databaseDirectory):
            if file.startswith("achv") and file.endswith("strdridx"):
                archive:ArchiveFile = ArchiveHandler._readArchiveIndex(None, StriderFileIO(open(os.path.join(fileUtil.databaseDirectory,file), "rb")))
                if lastArchive is None:
                    lastArchive = archive
                if max(lastArchive.minRange, archive.minRange) == archive.minRange:
                    lastArchive = archive

        # Determine archive range
        archiveRange = (archive.minRange-archive.maxRange)
        match archiveRange:
            case 86400:
                archiveRange = ARCHIVE_RANGE.day
            case 604800:
                archiveRange = ARCHIVE_RANGE.week
            case _:
                archiveRange = ARCHIVE_RANGE.month

        database = Database("strdrdb", CURRENT_REVISION, "rebuilt",
                            len(archives),
                            len(lastArchive.keys),
                            lastArchive.indexInterval,
                            archiveRange,
                            [DatabaseArchive(archive.minRange, archive.maxRange, archive.index, archive.resolution) for archive in archives],
                            lastArchive.keys)
        databaseHandler = DatabaseHandler(database, fileUtil)
        databaseHandler.save()
        return database

DatabaseManager = DatabaseManager()