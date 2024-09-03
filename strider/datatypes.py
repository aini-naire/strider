from dataclasses import dataclass, fields, field

from enum import Enum


class StriderStruct:
    format = tuple()

    def __post_init__(self):
        for field in fields(self):
            if field.type == Enum:
                setattr(self, field.name, field.default_factory(getattr(self, field.name)))


ARCHIVE_RANGE = Enum("Range", "day week month")


@dataclass
class DatabaseArchive(StriderStruct):
    format = ("I", "I", "H", "B")
    minRange: int
    maxRange: int
    index: int
    resolution: int


ARCHIVE_KEY_TYPES = Enum("KeyType", "? h i I f")  # bool, short, uint, int, float


@dataclass
class ArchiveKey(StriderStruct):
    format = (str, "H")
    name: str
    type: Enum = field(default_factory=ARCHIVE_KEY_TYPES)


@dataclass
class Database(StriderStruct):
    format = (str, "I", str, "H", "H", "H", "I")
    magic: str
    revision: int
    databaseName: str
    archiveCount: int
    keyCount: int
    indexInterval: int
    archiveRange: Enum = field(default_factory=ARCHIVE_RANGE)
    # should keys and stride be global?
    # todo archive ranges
    archives: list[DatabaseArchive] = field(default_factory=list)
    keys: list[ArchiveKey] = field(default_factory=list)


ARCHIVE_INDEX_TYPES = Enum("IndexType", "default start end")


@dataclass
class ArchiveIndex(StriderStruct):
    format = ("I", "I", "H")
    timestamp: int
    offset: int
    type: Enum = field(default_factory=ARCHIVE_INDEX_TYPES)


@dataclass
class ArchiveFile(StriderStruct):
    format = (str, "I", "B", "I", "I", "H", "H", "H", "I")
    magic: str
    revision: int
    resolution: int
    minRange: int
    maxRange: int
    index: int
    keyCount: int
    indexCount: int
    indexInterval: int
    keys: list[ArchiveKey] = field(default_factory=list)
    indices: list[ArchiveIndex] = field(default_factory=list)
