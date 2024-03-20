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
        return isinstance(other, Credentials) and all(
            getattr(self, attr) == getattr(other, attr)
            for attr in ['hosts', 'port', 'user', 'password', 'keyspace_name', 'datacenter']
        )
