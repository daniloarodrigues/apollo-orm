from typing import List

from apollo_orm.domains.models.entities.column.entity import Column


class TableConfig:
    def __init__(self, keyspace_name: str, table_name: str, columns: List[Column]):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        self.columns = columns
