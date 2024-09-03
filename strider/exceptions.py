class DatabaseNotFound(Exception):
    pass

class DatabaseExists(Exception):
    pass

class DatabaseCorrupt(Exception):
    pass

class SequenceViolation(Exception):
    pass