# Database
class DatabaseNotFound(Exception):
    pass


class DatabaseExists(Exception):
    pass


class DatabaseCorrupt(Exception):
    pass


# Archive
class ArchiveNotFound(Exception):
    pass


# Operations
class SequenceViolation(Exception):
    pass


class KeyAlreadyExists(Exception):
    pass
