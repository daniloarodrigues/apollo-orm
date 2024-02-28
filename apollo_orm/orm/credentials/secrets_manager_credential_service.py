from json import loads
from typing import Optional

from boto3 import client as aws_client

from apollo_orm.domains.models.entities.credentials.entity import Credentials
from apollo_orm.orm.abstracts.icredential import ICredential


class SecretsManagerCredentialService(ICredential):

    def __init__(self, endpoint_url: Optional[str] = None, **kwargs):
        self.endpoint_url = endpoint_url

        self.sm = (aws_client("secretsmanager") if endpoint_url is None
                   else aws_client("secretsmanager", endpoint_url=endpoint_url))

    def credential(self, **kwargs) -> Credentials:
        return self._convert_to_credential(**kwargs)

    def _convert_to_credential(self, **kwargs) -> Credentials:
        credentials = {}

        for key, value in kwargs.items():
            secret_value = self.sm.get_secret_value(SecretId=value)
            credentials[key] = loads(secret_value["SecretString"])

        return Credentials(**credentials)
