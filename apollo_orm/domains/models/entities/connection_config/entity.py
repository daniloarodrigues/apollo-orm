from typing import List

from apollo_orm.domains.models.entities.credentials.entity import Credentials


class ConnectionConfig:
    def __init__(self, credentials: Credentials, tables: List[str]):
        self.credential: Credentials = credentials
        self.tables: List[str] = tables
