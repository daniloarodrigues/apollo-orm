import hashlib
import json
import re
import uuid
from datetime import datetime

from typing import Dict, Optional, List, Any

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.connection import ConnectionException
from cassandra.cqltypes import Int32Type, UTF8Type, BooleanType, UUIDType, TimestampType, FloatType, DoubleType, \
    SetType, ListType, MapType
from cassandra.query import PreparedStatement

from apollo.domains.models.entities.column.entity import Column
from apollo.domains.models.entities.connection_config.entity import ConnectionConfig
from apollo.domains.models.entities.table_config.entity import TableConfig
from apollo.orm.abstracts.idatabase import IDatabaseService, DatabaseException
from apollo.utils.logger.logger import Logger


class ScyllaException(DatabaseException):
    logger = Logger("ScyllaException")

    def __init__(self, message, *args, **kwargs):
        super().__init__(message, log="error", *args, **kwargs)


def _generate_pre_statement_labels(columns: List[Column]) -> Dict[str, str]:
    hashed_statement = {}
    for column in columns:
        if column.kind == "partition_key" or column.kind == "clustering":
            hashed_statement[column.hash_id] = f"{column.name} = ?"
    return hashed_statement


def _text_to_hash(text: str) -> str:
    return hashlib.md5(re.sub(r'[^a-zA-Z0-9]+', '', text).lower().encode()).hexdigest()


def _column_name_to_hash(parameters: Any) -> Dict[str, Any]:
    hashed_names = {}
    for key, value in parameters.items():
        hash_id = _text_to_hash(key)
        hashed_names[hash_id] = value
    return hashed_names


def _parse_to_cassandra_type(value: Any, cassandra_type: str) -> Any:
    if cassandra_type == "uuid":
        return uuid.UUID(value)
    elif cassandra_type == "timeuuid":
        return uuid.UUID(value)
    elif cassandra_type == "boolean":
        return value
    elif cassandra_type == "text":
        return value
    elif cassandra_type == "timestamp":
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    elif cassandra_type == "int" or cassandra_type == "bigint":
        return value
    elif cassandra_type == "float" or cassandra_type == "double":
        return float(value)
    else:
        raise ScyllaException(f"Type {cassandra_type} not supported")


def _type_validate(column: Column, hashed_columns: Dict[str, Any], type_process: str) -> Column:
    if type_process == "insert":
        return Column(column.hash_id, column.name, column.kind, column.type,
                      _parse_to_cassandra_type(hashed_columns[column.hash_id], column.type))
    elif type_process == "select" or type_process == "delete":
        if column.kind == "partition_key" or column.kind == "clustering":
            return Column(column.hash_id, column.name, column.kind, column.type,
                          _parse_to_cassandra_type(hashed_columns[column.hash_id], column.type))


class ScyllaService(IDatabaseService):
    log = Logger("ScyllaService")

    def __init__(self,
                 connection_config: ConnectionConfig,
                 attempts: int = 5
                 ):
        self._connection_config = connection_config
        self._attempts = attempts
        self._table_config: Optional[List[TableConfig]] = None
        self._prepared_statements: Dict[str, PreparedStatement] = {}
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None

        self.connect()

    def connect(self):
        if self._connection_config is None:
            raise ScyllaException("Connection config is not set")
        error_message = ""
        for _ in range(self._attempts):
            try:
                auth_provider = PlainTextAuthProvider(
                    username=self._connection_config.credential.user,
                    password=self._connection_config.credential.password
                )
                self.cluster = Cluster(
                    contact_points=self._connection_config.credential.hosts,
                    port=self._connection_config.credential.port,
                    auth_provider=auth_provider,
                    protocol_version=4
                )
                self.session = self.cluster.connect()
                self._scan_tables()
                return
            except Exception as e:
                error_message = str(e) if str(e) else "Unknown error"
        raise ScyllaException(f"Failed to connect after {self._attempts} attempts - {error_message}")

    def close(self):
        if self.session is not None:
            self.session.shutdown()
        if self.cluster is not None:
            self.cluster.shutdown()

    def _reload_prepared_statements(self) -> None:
        reloaded_statements = {}
        for name, statement in self._prepared_statements.items():
            reloaded_statements[name] = self.session.prepare(statement.query_string)
        self._prepared_statements = reloaded_statements

    def reconnect(self) -> None:
        self.close()
        self.connect()
        self._reload_prepared_statements()

    def _scan_tables(self) -> None:
        system_schema = "system_schema.columns"
        columns_statement = "column_name, kind, type"
        where_statement = "keyspace_name = ? and table_name = ?"
        statement = self.session.prepare(
            f"""
                    select {columns_statement}
                    from {system_schema}
                    where {where_statement}
                    """
        )
        for table in self._connection_config.tables:
            values = [self._connection_config.credential.keyspace_name, table]
            config_rows = self.session.execute(statement.bind(values))
            columns_list = []
            for config_row in config_rows:
                columns_list.append(Column(
                    _text_to_hash(config_row.column_name),
                    config_row.column_name,
                    config_row.kind,
                    config_row.type))
                self._add_to_table_config(TableConfig(self._connection_config.credential.keyspace_name,
                                                      table,
                                                      columns_list))

    def _add_to_table_config(self, table_config: TableConfig) -> None:
        if self._table_config is None:
            self._table_config = []
        self._table_config.append(table_config)

    def _filter_columns(self, parameters: Any, table_name: str, type_process: str) -> Dict[str, Column]:
        hashed_columns = _column_name_to_hash(parameters)
        filtered_columns = {}
        for column in self._table_config[self._connection_config.tables.index(table_name)].columns:
            if column.hash_id in hashed_columns:
                filtered_column = _type_validate(column, hashed_columns, type_process)
                if filtered_column is not None:
                    filtered_columns[column.hash_id] = _type_validate(column, hashed_columns, type_process)
        self._check_partition_key_columns(filtered_columns, table_name)
        if type_process != "select":
            self._check_clustering_columns(filtered_columns, table_name)
        return filtered_columns

    def _filter_kind(self, columns: List[Column], kind: str) -> list[Column]:
        return [column for column in columns if column.kind == kind]

    def _check_partition_key_columns(self, columns: Dict[str, Column], table_name: str) -> None:
        non_regular_columns = self._table_config[self._connection_config.tables.index(table_name)].columns
        filtered = self._filter_kind(non_regular_columns, "partition_key")
        pendent_columns = []
        for column in filtered:
            if column.hash_id not in columns and column.kind == "partition_key":
                pendent_columns.append(column.__str__())
        if pendent_columns:
            raise ScyllaException(
                f"""Column {pendent_columns} is not in the filtered columns.
                    All partition keys columns must be passed as parameter""")

    def _check_clustering_columns(self, columns: Dict[str, Column], table_name: str) -> None:
        non_regular_columns = self._table_config[self._connection_config.tables.index(table_name)].columns
        filtered = self._filter_kind(non_regular_columns, "clustering")
        pendent_columns = []
        for column in filtered:
            if column.hash_id not in columns and column.kind == "clustering":
                pendent_columns.append(column.__str__())
        if pendent_columns:
            raise ScyllaException(
                f"""Column {pendent_columns} is not in the filtered columns.
                    All clustering columns must be passed as parameter""")

    def select_from_json(self, json_input: str, table_name: str) -> List[Dict]:
        return self.select(json.loads(json_input), table_name)

    def select(self, dictionary_input: Dict[str, Any], table_name: str) -> List[Dict]:
        filtered_columns = self._filter_columns(dictionary_input, table_name, "select")
        partition_and_clustering = [column for column in sorted(filtered_columns.values(), key=lambda x: x.name) if
                                    column.kind == "partition_key" or column.kind == "clustering"]
        prepared_statement = self._prepare_dynamic_statement(filtered_columns, table_name, "select")
        values = [dictionary_input[column.name] for column in partition_and_clustering]
        try:
            return self.session.execute(prepared_statement.bind(values)).all()
        except ConnectionException as e:
            self.log.error(f"Connection error: {e}")
            self.reconnect()
            return self.session.execute(prepared_statement.bind(values)).all()

    def insert(self, dictionary_input: Dict[str, Any], table_name: str) -> None:
        filtered_columns = self._filter_columns(dictionary_input, table_name, "insert")
        prepared_statement = self._prepare_dynamic_statement(filtered_columns, table_name, "insert")
        self._bind_delete_or_insert(filtered_columns, prepared_statement)

    def delete(self, dictionary_input: Dict[str, Any], table_name: str) -> None:
        filtered_columns = self._filter_columns(dictionary_input, table_name, "delete")
        prepared_statement = self._prepare_dynamic_statement(filtered_columns, table_name, "delete")
        self._bind_delete_or_insert(filtered_columns, prepared_statement)

    def _bind_delete_or_insert(self, filtered_columns: Dict[str, Column],
                               prepared_statement: PreparedStatement) -> None:
        values = [filtered_columns[column.hash_id].value for column in
                  sorted(filtered_columns.values(), key=lambda x: x.name)]
        try:
            self.session.execute(prepared_statement.bind(values)).all()
        except ConnectionException as e:
            self.log.error(f"Connection error: {e}")
            self.reconnect()
            self.session.execute(prepared_statement.bind(values)).all()

    def _prepare_dynamic_statement(self, columns: Dict[str, Column], table_name: str,
                                   query_type: str) -> PreparedStatement:
        keyspace = self._connection_config.credential.keyspace_name

        ordered_columns = sorted(columns.values(), key=lambda x: x.name)
        hashed_name = _text_to_hash(f"{query_type}".join([column.name for column in ordered_columns]))

        if hashed_name not in self._prepared_statements:
            if query_type == "select":
                self._prepare_read(hashed_name, ordered_columns, keyspace, table_name)
            elif query_type == "insert":
                self._prepare_insert(hashed_name, ordered_columns, keyspace, table_name, columns)
            elif query_type == "delete":
                self._prepare_delete(hashed_name, ordered_columns, keyspace, table_name, columns)

        return self._prepared_statements[hashed_name]

    def _prepare_read(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str) -> None:
        values = _generate_pre_statement_labels(columns)
        statement = f"select * from {keyspace}.{table_name} where {' and '.join(values.values())}"
        self._prepared_statements[hashed_name] = self.session.prepare(statement)

    def _prepare_insert(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str,
                        dict_columns: Optional[Dict[str, Column]] = None) -> None:
        self._check_clustering_columns(dict_columns, table_name)
        hashed_statement = {}
        keys = [column.name for column in columns]
        for column in columns:
            hashed_statement[column.hash_id] = "?"
        statement = f"""insert into
        {keyspace}.{table_name}
        ({', '.join(keys)})
        values ({', '.join(hashed_statement.values())})"""
        self._prepared_statements[hashed_name] = self.session.prepare(statement)

    def _prepare_delete(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str,
                        dict_columns: Optional[Dict[str, Column]] = None) -> None:
        self._check_clustering_columns(dict_columns, table_name)
        values = _generate_pre_statement_labels(columns)
        statement = f"delete from {keyspace}.{table_name} where {' and '.join(values.values())}"
        self._prepared_statements[hashed_name] = self.session.prepare(statement)
