import json

from apollo_orm.domains.models.entities.credentials.entity import Credentials
from apollo_orm.orm.abstracts.icredential import ICredential


class JsonCredentialService(ICredential):
    """
    Credential class for JSON credentials
    """

    def __init__(self, credentials: str):
        self.credentials = json.loads(credentials)

    def credential(self) -> Credentials:
        return self._convert_to_credential()

    def _convert_to_credential(self) -> Credentials:
        self.credentials["hosts"] = self.credentials["hosts"].split(",")
        return Credentials(**self.credentials)
