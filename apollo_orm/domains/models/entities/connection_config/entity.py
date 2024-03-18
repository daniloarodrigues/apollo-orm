from typing import List

from apollo_orm.domains.models.entities.credentials.entity import Credentials


class ConnectionConfig:
    def __init__(self, credentials: Credentials, tables: List[str]):
        if len(tables) == 0:
            raise ValueError("Tables cannot be empty")
        self.credential: Credentials = credentials
        self.tables: List[str] = tables

    def __eq__(self, other):
        if isinstance(other, ConnectionConfig):
            return self.credential == other.credential and self.tables == other.tables
        return False