from io import BufferedIOBase
import struct
from strider.datatypes import StriderStruct
import dataclasses
import os

class StriderFileIO():
    """Low-level binary reader/writer
    TODO check magic"""
    def __init__(self, file) -> None:
        self.file:BufferedIOBase = file
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.file.close()

    def readFormat(self, format: tuple) -> list:
        data = []
        for type in format:
            if type == str:
                data.append(self.readString())
            else:
                data.append(struct.unpack(type, self.file.read(struct.calcsize(type)))[0])
        return data
    
    def readString(self) -> str:
        length = struct.unpack('B', self.file.read(1))[0]
        if length == 0:
            return ''
        
        string = self.file.read(length)
        string = string.decode('utf8')
        return string
    
    def writeString(self, string: str):
        self.file.write(struct.pack("B", len(string)))
        self.file.write(string.encode())

    def readStruct(self, struct: StriderStruct) -> StriderStruct:
        return struct(*self.readFormat(struct._format))
    
    def writeStruct(self, striderStruct: StriderStruct) -> None:
        for i, field in enumerate(dataclasses.fields(striderStruct)):
            if i < len(striderStruct._format):
                value = getattr(striderStruct, field.name)
                match field.type.__name__:
                    case "str":
                        self.writeString(value)
                        continue
                    case "Enum":
                        value = value.value
                
                #print(i, field.name, field.type, value, striderStruct._format[i])
                self.file.write(struct.pack(striderStruct._format[i], value))
            else:
                for item in getattr(striderStruct, field.name):
                    self.writeStruct(item)
    
    def readStructSequence(self, struct: StriderStruct, count: int) -> list[StriderStruct]:
        seq = []
        for _ in range(count):
            seq.append(self.readStruct(struct))
        return seq
    
class StriderArchiveIO(StriderFileIO):
    def __init__(self, file, recordFormat) -> None:
        super().__init__(file)
        self.file:BufferedIOBase = file
        self.recordFormat = recordFormat
        self.recordSize = struct.calcsize(recordFormat)
    
    def readRecord(self):
        recordBytes = self.file.read(self.recordSize)
        if recordBytes:
            return struct.unpack(self.recordFormat, recordBytes)
        
    def writeRecord(self, record):
        self.file.write(struct.pack(self.recordFormat, *record))
    
class StriderFileUtil():
    def __init__(self, baseDir, databaseName) -> None:
        self.databaseDirectory = os.path.join(baseDir, databaseName)


    def getArchiveFilePath(self, archive, data=False):
        return os.path.join(self.databaseDirectory, f"achv_i{archive.index}_r{archive.resolution}.strdr{'data' if data else 'idx'}")
    

    def getDatabaseFilepath(self):
        return os.path.join(self.databaseDirectory, "db.strdr")
    