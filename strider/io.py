from io import BufferedIOBase
import struct
import dataclasses
import os
import shutil
from typing import Union, Type

from strider.datatypes import StriderStruct, ArchiveFile, DatabaseArchive


class StriderFileIO:
    """Low-level binary reader/writer
    TODO check magic"""

    def __init__(self, file) -> None:
        self.file: BufferedIOBase = file

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.file.close()

    def readFormat(self, _format: tuple) -> list:
        data = []
        for _type in _format:
            if _type == str:
                data.append(self.readString())
            else:
                data.append(struct.unpack(_type, self.file.read(struct.calcsize(_type)))[0])
        return data

    def readString(self) -> str:
        length = struct.unpack('B', self.file.read(1))[0]
        if length == 0:
            return ''

        string = self.file.read(length)
        string = string.decode('utf8')
        return string

    def writeString(self, string: str) -> None:
        self.file.write(struct.pack("B", len(string)))
        self.file.write(string.encode())

    def readStruct(self, striderStruct: type[StriderStruct]) -> type[StriderStruct]:
        return striderStruct(*self.readFormat(striderStruct.format))

    def writeStruct(self, striderStruct: StriderStruct) -> None:
        for i, field in enumerate(dataclasses.fields(striderStruct)):
            if i < len(striderStruct.format):
                value = getattr(striderStruct, field.name)

                match field.type.__name__:
                    case "str":
                        self.writeString(value)
                        continue
                    case "Enum":
                        value = value.value

                self.file.write(struct.pack(striderStruct.format[i], value))
            else:
                for item in getattr(striderStruct, field.name):
                    self.writeStruct(item)

    def readStructSequence(self, striderStruct: Type[StriderStruct], count: int) -> list[Type[StriderStruct]]:
        seq = []
        for _ in range(count):
            seq.append(self.readStruct(striderStruct))
        return seq


class StriderArchiveIO(StriderFileIO):
    def __init__(self, file, recordFormat) -> None:
        super().__init__(file)
        self.file: BufferedIOBase = file
        self.setRecordFormat(recordFormat)
    
    def setRecordFormat(self, newFormat: str) -> None:
        self.recordFormat = newFormat
        self.recordSize = struct.calcsize(newFormat)

    def readRecord(self) -> tuple:
        recordBytes = self.file.read(self.recordSize)
        if recordBytes:
            return struct.unpack(self.recordFormat, recordBytes)

    def readRecords(self, count: int) -> Union[None | tuple]:
        orig = self.file.tell()
        recordBytes = self.file.read(self.recordSize * count)
        if recordBytes:
            return struct.iter_unpack(self.recordFormat, recordBytes)
        self.file.seek(orig, 0)

    def readAllRecords(self) -> Union[None | tuple]:
        recordBytes = self.file.read()
        if recordBytes:
            return tuple(struct.iter_unpack(self.recordFormat, recordBytes))

    def writeRecord(self, record: tuple) -> None:
        self.file.write(struct.pack(self.recordFormat, *record))

        
    def writeRecords(self, records: tuple) -> None:
        self.file.write(struct.pack(self.recordFormat*len(records), *[recordItem for record in records for recordItem in record]))


class StriderFileUtil:
    def __init__(self, baseDir: str, databaseName: str) -> None:
        self.databaseDirectory = os.path.join(baseDir, databaseName)

    def getArchiveFilePath(self, archive: Union[DatabaseArchive | ArchiveFile], data=False) -> str:
        return os.path.join(self.databaseDirectory, f"achv_i{archive.index}_r{archive.resolution}.strdr{'data' if data else 'idx'}")

    def getDatabaseFilepath(self) -> str:
        return os.path.join(self.databaseDirectory, "db.strdr")
    
    def safeOverwrite(self, old: str, new: str) -> None:
        if os.path.exists(old):
            os.remove(old)
        shutil.copy2(new, old)
        os.remove(new)
    
    def replaceArchive(self, archive: ArchiveFile, data: bool = False) -> None:
        old = self.getArchiveFilePath(archive, data)
        new = self.getArchiveFilePath(archive, data)+".new"
        self.safeOverwrite(old, new)
