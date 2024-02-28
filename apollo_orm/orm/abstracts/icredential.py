from abc import ABC

from apollo_orm.domains.models.entities.credentials.entity import Credentials


class ICredential(ABC):
    """
    Interface for Credential
    """

    def _convert_to_credential(self) -> Credentials:
        """
        Convert data type to Credential
        :return: ConnectionConfig
        """

    def credential(self) -> Credentials:
        """
        Get a Credential Instance
        :return: ConnectionConfig
        """
