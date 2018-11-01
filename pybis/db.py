class Db:

import os
from pymongo import MongoClient

"""Tools to connect to BIS databases.

This class contains tools to connect to BIS databases.

These OS environment variables must be set if they are something other than
the defaults:

MONGODB_DATABASE
MONGODB_USERNAME
MONGODB_SERVER
MONGODB_PASSWORD
"""

    def __init__(self):
        self.description = "Set of functions for connecting to database infrastructure in various environments"

    def connect_mongodb(db_name):
        mongo_uri = "mongodb://" + os.environ["MONGODB_USERNAME"] + ":" + os.environ["MONGODB_PASSWORD"] + "@" + os.environ["MONGODB_SERVER"] + "/" + os.environ["MONGODB_DATABASE"]
        client = MongoClient(mongo_uri)
        client_db = client.get_database(os.environ["MONGODB_DATABASE"])
        return client_db[db_name]
