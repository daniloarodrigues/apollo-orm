from abc import ABC
from typing import Optional, List, Dict, Any

from apollo.domains.models.entities.column.entity import Column
from apollo.domains.models.entities.table_config.entity import TableConfig
from apollo.utils.exceptions.CommonBaseException import CommonBaseException
from apollo.utils.logger.logger import Logger


class DatabaseException(CommonBaseException):
    """Custom database exception."""

    log = Logger("DatabaseException")

    def __init__(self, message=None, log=None, *args, **kwargs):
        super(DatabaseException, self).__init__(message, log, *args, **kwargs)


class IDatabaseService(ABC):
    """
    Interface for Database Service
    """

    def close(self):
        """"""

    def connect(self) -> None:
        """"""

    def reconnect(self) -> None:
        """"""

    def select(self, dictionary_input: Dict[str, Any], table_name: str) -> List[Dict]:
        """"""

    def insert(self, dictionary_input: Dict[str, Any], table_name: str) -> None:
        """"""

    def delete(self, dictionary_input: Dict[str, Any], table_name: str) -> None:
        """"""

    def select_from_json(self, json_input: str, table_name: str) -> List[Dict]:
        """"""

    def _reload_prepared_statements(self) -> None:
        """"""

    def _prepare_dynamic_statement(self, columns: Dict[str, Column], table_name: str,
                                   query_type: str):
        """"""

    def _filter_columns(self, parameters: Any, table_name: str, type_process: str) -> Dict[str, Column]:
        """"""

    def _add_to_table_config(self, table_config: TableConfig) -> None:
        """"""

    def _scan_tables(self) -> None:
        """"""

    def _reload_prepared_statements(self) -> None:
        """"""

    def _prepare_read(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str) -> None:
        """"""

    def _prepare_insert(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str,
                        dict_columns: Optional[Dict[str, Column]] = None) -> None:
        """"""

    def _prepare_delete(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str,
                        dict_columns: Optional[Dict[str, Column]] = None) -> None:
        """"""
