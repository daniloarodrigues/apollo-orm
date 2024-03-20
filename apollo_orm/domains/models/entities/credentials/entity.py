from typing import List, Optional


class Credentials:
    def __init__(self, hosts: List, port: int, user: str, password: str, keyspace_name: str,
                 datacenter: Optional[str] = None):
        self.hosts: List[str] = hosts
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.keyspace_name: str = keyspace_name
        self.datacenter: str = datacenter

    def __eq__(self, other):
        if isinstance(other, Credentials):
            return self.hosts == other.hosts and self.port == other.port and self.user == other.user and self.password == other.password and self.keyspace_name == other.keyspace_name and self.datacenter == other.datacenter
        return False
