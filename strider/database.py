from typing import Union
from datetime import datetime
import calendar

from strider.io import StriderFileIO, StriderFileUtil
from strider.archive import ArchiveHandler
from strider.exceptions import *

from strider.datatypes import Database, DatabaseArchive, ArchiveKey, ARCHIVE_RANGE, ARCHIVE_KEY_TYPES


class DatabaseHandler:
    database: Union[None, Database]
    fileUtil: StriderFileUtil

    def __init__(self, database: Database, fileUtil: StriderFileUtil) -> None:
        self.database = database
        self.fileUtil = fileUtil

    def getArchivePeriod(self, date: datetime):
        match self.database.archiveRange:
            case ARCHIVE_RANGE.day:
                return 86400
            case ARCHIVE_RANGE.week:
                return 604800
            case ARCHIVE_RANGE.month:
                monthDays = calendar.monthrange(date.year, date.month)[1]
                return 86400 * monthDays
            case _:
                raise ValueError()

    def getArchiveKey(self, date: datetime) -> int:
        timestamp = int(date.timestamp())
        return timestamp - (timestamp % self.getArchivePeriod(date))

    def save(self) -> None:
        """Saves database"""
        try:
            self.fileUtil.safeOverwrite(self.fileUtil.getDatabaseFilepath()+".old", self.fileUtil.getDatabaseFilepath())
        except FileNotFoundError:
            pass
        with StriderFileIO(open(self.fileUtil.getDatabaseFilepath(), "wb")) as databaseFile:
            databaseFile.writeStruct(self.database)

    def addKey(self, archiveKey: ArchiveKey) -> bool:
        """Correctly adds an key to the database file"""
        for key in self.database.keys:
            if key.name == archiveKey.name:
                raise KeyAlreadyExists()
            
        self.database.keys.append(archiveKey)
        self.database.keyCount = len(self.database.keys)
        self.save()
        return True
    
    def getKeys(self) -> list[ArchiveKey]:
        return self.database.keys
    
    def setIndexInteval(self, inteval) -> None:
        self.database.indexInterval = inteval
        self.save()
        return True

    def hasArchive(self, archiveKey: int) -> bool:
        for archive in self.database.archives:
            if archive.minRange == archiveKey:
                return True
        return False

    def hasArchivePeriod(self, date: datetime) -> bool:
        return self.hasArchive(self.getArchiveKey(date))

    def loadArchive(self, archiveKey: int) -> ArchiveHandler:
        """Loads archive
        TODO check if archive exists etc"""
        for archive in self.database.archives:
            if archive.minRange == archiveKey:
                databaseArchive = archive

        archiveHandler = ArchiveHandler(self.fileUtil).load(databaseArchive)
        return archiveHandler

    def loadArchives(self) -> dict[int, ArchiveHandler]:
        archives = {}
        for archive in self.database.archives:
            archiveHandler = ArchiveHandler(self.fileUtil).load(archive)
            archives[archive.minRange] = archiveHandler
        return archives

    def createArchive(self, date: datetime) -> ArchiveHandler:
        """Depending on the archive range, archives are created starting on the first hour of the day or week and cannot overlap unless the resolution is different
        TODO checks, errors"""
        archiveMin = self.getArchiveKey(date)
        archiveMax = archiveMin + self.getArchivePeriod(date)

        databaseArchive = DatabaseArchive(archiveMin, archiveMax, self.database.archiveCount + 1, 0)

        archiveHandler = ArchiveHandler(self.fileUtil).create(databaseArchive, self.database)

        self.database.archives.append(databaseArchive)
        self.database.archiveCount = len(self.database.archives)
        self.save()

        return archiveHandler
