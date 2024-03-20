from typing import List

from apollo_orm.domains.models.entities.credentials.entity import Credentials


class ConnectionConfig:
    def __init__(self, credentials: Credentials, tables: List[str]):
        if not tables:
            raise ValueError("Tables cannot be empty")
        self.credential: Credentials = credentials
        self.tables: List[str] = tables

    def __eq__(self, other):
        return isinstance(other, ConnectionConfig) and all(
            getattr(self, attr) == getattr(other, attr)
            for attr in ['credential', 'tables']
        )
