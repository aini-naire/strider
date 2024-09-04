from typing import Union, Self
from collections import namedtuple

from strider.io import StriderFileIO, StriderFileUtil, StriderArchiveIO
from strider.exceptions import *

from strider.strider import CURRENT_REVISION
from strider.datatypes import Database, DatabaseArchive, ArchiveFile, ArchiveKey, ArchiveIndex, ARCHIVE_KEY_TYPES

class ArchiveHandler:
    archive: ArchiveFile
    archiveRecordFormat: str
    lastEntryTimestamp: int = 0
    lastIndexTimestamp: int = 0
    fileUtil: StriderFileUtil

    def __init__(self, fileUtil: StriderFileUtil) -> None:
        self.fileUtil = fileUtil

    def _buildDataFormat(self):
        _format = "I"
        for key in self.archive.keys:
            _format += ARCHIVE_KEY_TYPES(key.type).name
        self.archiveRecordFormat = _format

    def load(self, archive: DatabaseArchive) -> Self:
        """"""
        try:
            self.archive = self._readArchiveIndex(StriderFileIO(open(self.fileUtil.getArchiveFilePath(archive), "rb")))
            self._buildDataFormat()
            self.lastIndexTimestamp = self.archive.indices[-1].timestamp
        except FileNotFoundError:
            raise ArchiveNotFound()

        return self

    def _readArchiveIndex(self, archiveFile: StriderFileIO) -> ArchiveFile:
        """Read Archive file structs 
        TODO error handling"""
        archive: ArchiveFile = archiveFile.readStruct(ArchiveFile)
        archive.keys = archiveFile.readStructSequence(ArchiveKey, archive.keyCount)
        archive.indices = archiveFile.readStructSequence(ArchiveIndex, archive.indexCount)
        return archive

    def create(self, databaseArchive: DatabaseArchive, database: Database) -> Self:
        self.archive = ArchiveFile("strdridx", CURRENT_REVISION,
                                   databaseArchive.resolution,
                                   databaseArchive.minRange,
                                   databaseArchive.maxRange,
                                   databaseArchive.index,
                                   database.keyCount,
                                   0,
                                   database.indexInterval,
                                   database.keys,
                                   [])
        self.saveArchiveIndex()
        self._buildDataFormat()
        return self

    def saveArchiveIndex(self) -> None:
        """Saves archive
        TODO rename current file to .old"""
        with StriderFileIO(open(self.fileUtil.getArchiveFilePath(self.archive), "wb")) as archiveFile:
            archiveFile.writeStruct(self.archive)

    def getIndex(self, time: int) -> Union[None | ArchiveIndex]:
        index: ArchiveIndex
        last = None
        if time < self.archive.minRange:
            return None
        
        for index in self.archive.indices:
            if index.timestamp > time:
                return last
            last = index

        return last

    def addIndex(self, index: ArchiveIndex) -> None:
        """TODO somthing"""
        self.archive.indices.append(index)
        self.archive.indexCount = len(self.archive.indices)

    def addKey(self, archiveKey: ArchiveKey) -> None:
        with StriderArchiveIO(open(self.fileUtil.getArchiveFilePath(self.archive, True), "rb"), self.archiveRecordFormat) as archiveFile:
            records = archiveFile.readAllRecords()

        with StriderArchiveIO(open(self.fileUtil.getArchiveFilePath(self.archive, True)+".new", "w+b"), self.archiveRecordFormat) as archiveFile:
            archiveFile.setRecordFormat(self.archiveRecordFormat+ARCHIVE_KEY_TYPES(archiveKey.type).name)
            records = [(*item, 0) for item in records]
            archiveFile.writeRecords(records)

        self.fileUtil.replaceArchive(self.archive, True)

        self.archive.keys.append(archiveKey)
        self.archive.keyCount = len(self.archive.keys)
        self.saveArchiveIndex()


    def readRecords(self, start: int, end: int, key: Union[None | str] = None, raw: bool =  False) -> list:
        """
        TODO smarter read strategy"""
        if key:
            records = {}
            for i, archivekey in enumerate(self.archive.keys):
                if archivekey.name == key:
                    keyI = i+1
        else:
            records = []
            #recordObj = construct_slots(["time", *[key.name for key in self.archive.keys]])
            recordTuple = namedtuple('Record', 'timestamp '+' '.join([key.name for key in self.archive.keys]))
        
        index = self.getIndex(start)
        #print(self.archive.index, start, index)
        with StriderArchiveIO(open(self.fileUtil.getArchiveFilePath(self.archive, True), "rb"), self.archiveRecordFormat) as archiveFile:
            if index:
                archiveFile.file.seek(index.offset)

            lookAhead = True
            read = True
            while read:
                if lookAhead:
                    nextRecords = archiveFile.readRecords(50)
                    if nextRecords is None:
                        lookAhead = False

                if nextRecords is None:
                    nextRecords = [archiveFile.readRecord()]
                    if nextRecords[0] is None:
                        break

                for record in nextRecords:
                    if record[0] >= start:
                        if record[0] >= end:
                            read = False
                            break
                        else:
                            if key:
                                records[record[0]] = record[keyI]
                            else:
                                records.append(record)

            if raw or key:
                return records
            else:
                return [tuple.__new__(recordTuple, record) for record in records]

    def writeRecords(self, records: list) -> None:
        with StriderArchiveIO(open(self.fileUtil.getArchiveFilePath(self.archive, True), "a+b"), self.archiveRecordFormat) as archiveFile:
            archiveFile.file.seek(archiveFile.recordSize, 2)
            lastRecord = archiveFile.readRecord()
            self.lastEntryTimestamp = archiveFile.readRecord()[0] if lastRecord else 0

            for i, record in enumerate(records):
                if record[0] < self.lastEntryTimestamp:
                    raise SequenceViolation()

                if (record[0] - self.lastIndexTimestamp) >= self.archive.indexInterval:
                    #print(record[0], self.lastIndexTimestamp, record[0] - self.lastIndexTimestamp, self.archive.indexInterval)
                    index = ArchiveIndex(record[0], archiveFile.file.tell() + (i * archiveFile.recordSize), 1)
                    self.addIndex(index)
                    self.lastIndexTimestamp = index.timestamp

            archiveFile.writeRecords(records)

            self.saveArchiveIndex()
