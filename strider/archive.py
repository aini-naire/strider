from typing import Union
from typing_extensions import Self
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
            raise ArchiveNotFound

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
        for index in self.archive.indices:
            if index.timestamp > time:
                return last
            last = index

    def addIndex(self, index: ArchiveIndex) -> None:
        """TODO somthing"""
        self.archive.indices.append(index)
        self.archive.indexCount = len(self.archive.indices)

    def readRecords(self, start: int, end: int) -> list:
        """
        TODO smarter read strategy"""
        records = []
        index = self.getIndex(start)
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
                        continue
                    for r in nextRecords:
                        if r[0] >= start:
                            if r[0] >= end:
                                read = False
                            else:
                                records.append(r)
                else:
                    record = archiveFile.readRecord()
                    if record is None:
                        break

                    if record[0] >= start:
                        if record[0] >= end:
                            break

                        records.append(record)
        return records

    def writeRecords(self, records: list) -> None:
        with StriderArchiveIO(open(self.fileUtil.getArchiveFilePath(self.archive, True), "a+b"), self.archiveRecordFormat) as archiveFile:
            archiveFile.file.seek(archiveFile.recordSize, 2)
            lastRecord = archiveFile.readRecord()
            self.lastEntryTimestamp = archiveFile.readRecord()[0] if lastRecord else 0

            for record in records:
                if record[0] < self.lastEntryTimestamp:
                    raise SequenceViolation

                if (record[0] - self.lastIndexTimestamp) > self.archive.indexInterval:
                    index = ArchiveIndex(record[0], archiveFile.file.tell(), 1)
                    self.addIndex(index)
                    self.lastIndexTimestamp = index.timestamp

                archiveFile.writeRecord(record)

            self.saveArchiveIndex()
