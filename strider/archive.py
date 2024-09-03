from strider.io import StriderFileIO, StriderFileUtil, StriderArchiveIO


from strider.strider import CURRENT_REVISION
from strider.datatypes import Database, DatabaseArchive, ArchiveFile, ArchiveKey, ArchiveIndex, ARCHIVE_KEY_TYPES

class ArchiveHandler():
    archive: ArchiveFile
    archiveRecordFormat: str
    lastEntryTimestamp: int
    lastIndexTimestamp: int = 0
    fileUtil: StriderFileUtil
    def __init__(self, fileUtil) -> None:
        self.fileUtil = fileUtil

    def _buildDataFormat(self):
        format = "I"
        for key in self.archive.keys:
            format+=ARCHIVE_KEY_TYPES(key.type).name
        self.archiveRecordFormat = format
    
    def load(self, archive: DatabaseArchive) -> None:
        """TODO set lastIndexTimestamp and lastEntryTimestamp"""
        try:
            self.archive = self._readArchiveIndex(open(self.fileUtil.getArchiveFilePath(archive), "rb"))
            self._buildDataFormat()
            self.lastIndexTimestamp = self.archive.indices[-1].timestamp
        except FileNotFoundError:
            print("archive not found")
        return self

    def _readArchiveIndex(self, archiveFile) -> ArchiveFile:
        """Read Archive file structs 
        TODO error handling"""
        archiveFile = StriderFileIO(archiveFile)
        archive: ArchiveFile = archiveFile.readStruct(ArchiveFile)
        archive.keys = archiveFile.readStructSequence(ArchiveKey, archive.keyCount)
        archive.indices = archiveFile.readStructSequence(ArchiveIndex, archive.indexCount)
        return archive
    
    def create(self, databaseArchive: DatabaseArchive, database):
        self.archive = ArchiveFile("strdridx", CURRENT_REVISION,
                                    databaseArchive.resolution,
                                    databaseArchive.minRange,
                                    databaseArchive.maxRange,
                                    databaseArchive.index,
                                    database.keyCount,
                                    0,
                                    database.indexInteval,
                                    database.keys,
                                    [])
        self.saveArchiveIndex()
        self._buildDataFormat()
        self.lastEntryTimestamp = 0
        return self

    def saveArchiveIndex(self) -> None:
        """Saves archive
        TODO rename current file to .old"""
        with StriderFileIO(open(self.fileUtil.getArchiveFilePath(self.archive), "wb")) as archiveFile:
            archiveFile.writeStruct(self.archive)

    def getIndex(self, time):
        index:ArchiveIndex
        for index in self.archive.indices:
            if index.timestamp < time:
                return index
    
    def addIndex(self, index: ArchiveIndex):
        """TODO somthing"""
        self.archive.indices.append(index)
        self.archive.indexCount=len(self.archive.indices)

    def readRecords(self, start, end):
        """
        TODO smarter read strategy"""
        records = []
        index = self.getIndex(start)
        with StriderArchiveIO(open(self.fileUtil.getArchiveFilePath(self.archive, True), "rb"), self.archiveRecordFormat) as archiveFile:
            archiveFile.file.seek(index.offset)
            #note that reading one at a time is way slower especially calling a wrapper function
            while True:
                record = archiveFile.readRecord()
                if record is None:
                    break

                if record[0] >= start:
                    if record[0] >= end:
                        break

                    records.append(record)
        return records

    def writeRecords(self, records):
        with StriderArchiveIO(open(self.fileUtil.getArchiveFilePath(self.archive, True), "a+b"), self.archiveRecordFormat) as archiveFile:
            archiveFile.file.seek(archiveFile.recordSize, 2)
            lastRecord = archiveFile.readRecord()
            self.lastEntryTimestamp = archiveFile.readRecord()[0] if lastRecord else 0
            for record in records:
                if record[0] < self.lastEntryTimestamp:
                    print("tried to add a record before the last")
                    break

                if record[0] - self.lastIndexTimestamp > self.archive.indexInteval:
                    index = ArchiveIndex(record[0], archiveFile.file.tell(), 1)
                    self.addIndex(index)
                    self.lastIndexTimestamp = record[0]

                archiveFile.writeRecord(record)
            self.saveArchiveIndex() 