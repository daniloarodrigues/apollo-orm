from typing import Any


class Column:
    def __init__(self, hash_id: str, name: str, kind: str, type_name: str, value: Any = None):
        self.hash_id = hash_id
        self.name = name
        self.value = value
        self.kind = kind
        self.type = type_name

    def __str__(self):
        return f"Column: {self.name} - {self.value} - {self.type} - {self.kind}"
