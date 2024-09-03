# Strider
Strider is a simple timeseries database written in Python.
# TODO
 - [x] exceptions
 - [x] styling/typing consistency
 - [ ] tests
 - [ ] database integrity checks
 - [ ] database self repair
 - [ ] set archive index inteval/rebuild
 - [x] add database key with an existing archive/full rebuild (full rebuild only necessary if backfilling past records is necessary)
 - [ ] strides querying
 - [ ] downsampling
 - [ ] compression(?)
 - [ ] memory residency mode (all archives, current, none)
 - [x] query return formats
 - [ ] database creation options
# Usage

    from strider.strider import DatabaseManager, DatabaseSession
    from datetime import datetime
    
    # Create new Database
    databaseSession:DatabaseSession  =  DatabaseManager().new("test", "test")
    # Register a new key
    databaseSession.databaseHandler.addKey("cpu_load", 5) #from 1 to 5, types are bool, short, uint, int, float
    # Add a record
    databaseSession.add(datetime.now(), {"cpu_load": 1.0})
    

# Design (WIP)
Strider is designed with storage and retrieval efficiency in mind. For those reasons, it is a **sequential append-only** database. A database is comprised of a group of archives that store a week, day, or month. Each archive can have a different resolution.