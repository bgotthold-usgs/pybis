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

    def set_env_variables(config_file):
        try:
            env_vars_set = []
            with open(config_file) as f:
                for line in f:
                    if 'export' not in line:
                        continue
                    if line.startswith('#'):
                        continue
                    key, value = line.replace('export ', '', 1).strip().split('=', 1)
                    os.environ[key] = value
                    env_vars_set.append(key)
            f.close()
            return env_vars_set
        except Exception as e:
            return e

    def connect_mongodb(db_name):
        mongo_uri = "mongodb://" + os.environ["MONGODB_USERNAME"] + ":" + os.environ["MONGODB_PASSWORD"] + "@" + os.environ["MONGODB_SERVER"] + "/" + os.environ["MONGODB_DATABASE"]
        client = MongoClient(mongo_uri)
        client_db = client.get_database(os.environ["MONGODB_DATABASE"])
        return client_db[db_name]
