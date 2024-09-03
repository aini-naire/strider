# Strider
Strider is a simple timeseries database written in Python.
# Usage

    from strider.strider import DatabaseManager, DatabaseSession
    from datetime import datetime
    
    # Create new Database
    databaseSession:DatabaseSession  =  DatabaseManager().new("test", "test")
    # Register a new key
    databaseSession.databaseHandler.addKey("cpu_load", 5) #from 1 to 5, types are bool, short, uint, int, float
    # Add a record
    databaseSession.add(datetime.now(), {"cpu_load": 1.0})
    

# Design
Strider is designed with storage and retrieval efficiency in mind. A database is comprised of a master file with the .strdr extension 
