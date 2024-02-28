from typing import List

from apollo_orm.domains.models.entities.credentials.entity import Credentials
from apollo_orm.orm.abstracts.icredential import ICredential


class CredentialService(ICredential):

    def __init__(self, hosts: List, port: int, user: str, password: str, keyspace_name: str):
        self.hosts: List[str] = hosts
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.keyspace_name: str = keyspace_name

    def credential(self) -> Credentials:
        return self._convert_to_credential()

    def _convert_to_credential(self) -> Credentials:
        return Credentials(self.hosts, self.port, self.user, self.password, self.keyspace_name)