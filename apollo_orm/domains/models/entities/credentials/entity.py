from typing import List


class Credentials:
    def __init__(self, hosts: List, port: int, user: str, password: str, keyspace_name: str):
        self.hosts: List[str] = hosts
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.keyspace_name: str = keyspace_name

