import hashlib
import json
import re
import uuid
from datetime import datetime, date

from typing import Dict, Optional, List, Any

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session, NoHostAvailable, ExecutionProfile, ResultSet, ResponseFuture, \
    EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.connection import ConnectionException
from cassandra.policies import RoundRobinPolicy, DCAwareRoundRobinPolicy, TokenAwarePolicy, RetryPolicy, \
    ExponentialReconnectionPolicy, ConstantSpeculativeExecutionPolicy
from cassandra.query import PreparedStatement
from apollo_orm.domains.models.entities.column.entity import Column
from apollo_orm.domains.models.entities.concurrent.pre_processed_insert.entity import PreProcessedInsertData
from apollo_orm.domains.models.entities.concurrent.result_list.entity import ResultList
from apollo_orm.domains.models.entities.concurrent.result_process.entity import ResultProcess
from apollo_orm.domains.models.entities.connection_config.entity import ConnectionConfig
from apollo_orm.domains.models.entities.table_config.entity import TableConfig
from apollo_orm.orm.abstracts.idatabase import IDatabaseService, DatabaseException
from apollo_orm.utils.logger.logger import Logger


class ApolloORMException(DatabaseException):
    logger = Logger("ApolloORMException")

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
    if cassandra_type in ["uuid", "timeuuid"]:
        return uuid.UUID(value)
    elif cassandra_type in ["boolean", "text", "int", "bigint", "decimal"]:
        return value
    elif cassandra_type == "timestamp":
        return _timestamp_validate(value)
    elif cassandra_type == "date":
        return _date_validate(value)
    elif cassandra_type == "time" and isinstance(value, datetime):
        return value
    elif cassandra_type in ["float", "double"]:
        return float(value)
    else:
        raise ApolloORMException(f"Type {cassandra_type} not supported. Value {value}")


def _timestamp_validate(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    elif isinstance(value, str):
        formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f",
                   "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f"]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    raise ValueError("No valid date format found")


def _date_validate(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    elif isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("No valid date format found")


def _type_validate(column: Column, hashed_columns: Dict[str, Any], type_process: str) -> Column:
    if type_process == "insert":
        return Column(column.hash_id, column.name, column.kind, column.type,
                      _parse_to_cassandra_type(hashed_columns[column.hash_id], column.type))
    elif type_process == "select" or type_process == "delete":
        if column.kind == "partition_key" or column.kind == "clustering":
            return Column(column.hash_id, column.name, column.kind, column.type,
                          _parse_to_cassandra_type(hashed_columns[column.hash_id], column.type))


def _filter_kind(columns: List[Column], kind: str) -> list[Column]:
    return [column for column in columns if column.kind == kind]


class ORMRetryPolicy(RetryPolicy):
    def __init__(self, max_retry_attempts: int = 5):
        self.MAX_RETRY_ATTEMPTS = max_retry_attempts

    def on_read_timeout(self, query, consistency, required_responses, received_responses, data_retrieved, retry_num):
        if retry_num < self.MAX_RETRY_ATTEMPTS and received_responses >= required_responses and not data_retrieved:
            return self.RETRY, consistency
        return self.RETHROW, None

    def on_write_timeout(self, query, consistency, write_type, required_responses, received_responses, retry_num):
        if retry_num < self.MAX_RETRY_ATTEMPTS and (
                (write_type == "BATCH_LOG" and received_responses >= required_responses) or (
                write_type != "BATCH_LOG" and received_responses > 0)):
            return self.RETRY, consistency
        return self.RETHROW, None

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        return (self.RETRY_NEXT_HOST, None) if retry_num == 0 else (self.RETHROW, None)

    def on_request_error(self, query, consistency, error, retry_num):
        return self.RETRY_NEXT_HOST, None


def get_consistency_level(consistency: str) -> int:
    ConsistencyLevel.name_to_value = {
        'ANY': ConsistencyLevel.ANY,
        'ONE': ConsistencyLevel.ONE,
        'TWO': ConsistencyLevel.TWO,
        'THREE': ConsistencyLevel.THREE,
        'QUORUM': ConsistencyLevel.QUORUM,
        'ALL': ConsistencyLevel.ALL,
        'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
        'EACH_QUORUM': ConsistencyLevel.EACH_QUORUM,
        'SERIAL': ConsistencyLevel.SERIAL,
        'LOCAL_SERIAL': ConsistencyLevel.LOCAL_SERIAL,
        'LOCAL_ONE': ConsistencyLevel.LOCAL_ONE
    }
    return ConsistencyLevel.name_to_value[consistency]


class ORMInstance(IDatabaseService):
    log = Logger("ORMInstance")

    def __init__(self,
                 connection_config: ConnectionConfig,
                 attempts: int = 5,
                 consistency_level: str = "LOCAL_ONE",
                 client_timeout: int = 20.0
                 ):
        self._attempts = attempts
        self._speculative_execution_policy = ConstantSpeculativeExecutionPolicy(
            delay=0.1, max_attempts=attempts)
        self._policy = DCAwareRoundRobinPolicy(
            connection_config.credential.datacenter) if connection_config.credential.datacenter else RoundRobinPolicy()
        self._load_balancing_policy = TokenAwarePolicy(self._policy)
        self._execution_profile = ExecutionProfile(load_balancing_policy=self._load_balancing_policy,
                                                   request_timeout=client_timeout,
                                                   consistency_level=get_consistency_level(consistency_level),
                                                   retry_policy=ORMRetryPolicy(attempts),
                                                   speculative_execution_policy=self._speculative_execution_policy
                                                   )
        self._connection_config = connection_config
        self._table_config: Optional[List[TableConfig]] = None
        self._prepared_statements: Dict[str, PreparedStatement] = {}
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self.connect()

    def connect(self,
                protocol_version: int = 4) -> None:
        if self._connection_config is None:
            raise ApolloORMException("Connection config is not set")
        try:
            auth_provider = PlainTextAuthProvider(
                username=self._connection_config.credential.user,
                password=self._connection_config.credential.password
            )
            self.cluster = Cluster(
                contact_points=self._connection_config.credential.hosts,
                port=self._connection_config.credential.port,
                auth_provider=auth_provider,
                protocol_version=protocol_version,
                execution_profiles={EXEC_PROFILE_DEFAULT: self._execution_profile},
                reconnection_policy=ExponentialReconnectionPolicy(base_delay=1.0, max_delay=10.0)
            )
            self.session = self.cluster.connect()
            self._scan_tables()
            self.log.info(f"Connected to {self._connection_config.credential.hosts}")
            return
        except Exception as e:
            error_message = str(e) if str(e) else "Unknown error"
            raise ApolloORMException(f"Failed to connect after {self._attempts} attempts - {error_message}")

    def close(self):
        if self.session is not None:
            self.session.shutdown()
        if self.cluster is not None:
            self.cluster.shutdown()

    def _reload_prepared_statements(self) -> None:
        self._prepared_statements = {name: self.session.prepare(statement.query_string) for name, statement in
                                     self._prepared_statements.items()}

    def reconnect(self) -> None:
        self.close()
        self.connect()
        self._reload_prepared_statements()

    def _scan_tables(self) -> None:
        system_schema = "system_schema.columns"
        columns_statement = "column_name, kind, type"
        where_statement = "keyspace_name = ? and table_name = ?"
        statement = self.session.prepare(f"select {columns_statement} from {system_schema} where {where_statement}")
        for table in self._connection_config.tables:
            values = [self._connection_config.credential.keyspace_name, table]
            config_rows = self.session.execute(statement, values)
            if not config_rows:
                raise ApolloORMException(
                    f"Not found keyspace {self._connection_config.credential.keyspace_name} or table {table}")
            columns_list = [
                Column(_text_to_hash(config_row.column_name), config_row.column_name, config_row.kind, config_row.type)
                for config_row in config_rows]
            self._add_to_table_config(
                TableConfig(self._connection_config.credential.keyspace_name, table, columns_list))

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

    def _check_partition_key_columns(self, columns: Dict[str, Column], table_name: str) -> None:
        non_regular_columns = self._table_config[self._connection_config.tables.index(table_name)].columns
        filtered = _filter_kind(non_regular_columns, "partition_key")
        pendent_columns = [column.__str__() for column in filtered if
                           column.hash_id not in columns and column.kind == "partition_key"]
        if pendent_columns:
            raise ApolloORMException(
                f"""Column {pendent_columns} is not in the filtered columns.
                    All partition keys columns must be passed as parameter""".rstrip())

    def _check_clustering_columns(self, columns: Dict[str, Column], table_name: str) -> None:
        non_regular_columns = self._table_config[self._connection_config.tables.index(table_name)].columns
        filtered = _filter_kind(non_regular_columns, "clustering")
        pendent_columns = [column.__str__() for column in filtered if
                           column.hash_id not in columns and column.kind == "clustering"]
        if pendent_columns:
            raise ApolloORMException(
                f"""Column {pendent_columns} is not in the filtered columns.
                    All clustering columns must be passed as parameter""".rstrip())

    def select_from_json(self, json_input: str, table_name: str) -> ResultSet:
        return self.select(json.loads(json_input), table_name)

    def select(self, dictionary_input: Dict[str, Any], table_name: str) -> ResultSet:
        try:
            filtered_columns = self._filter_columns(dictionary_input, table_name, "select")
            partition_and_clustering = [column for column in sorted(filtered_columns.values(), key=lambda x: x.name) if
                                        column.kind == "partition_key" or column.kind == "clustering"]
            prepared_statement = self._prepare_dynamic_statement(filtered_columns, table_name, "select")
            values = [dictionary_input[column.name] for column in partition_and_clustering]
            return self._execute_query(prepared_statement, values)
        except Exception as e:
            self.log.error(f"Failed to select data: {e}, in table {table_name}")
            raise ApolloORMException(f"Failed to select data: {dictionary_input} in table {table_name} - {e}")

    def insert(self, dictionary_input: Dict[str, Any], table_name: str, exec_async: bool = False) \
            -> Optional[ResponseFuture]:
        try:
            filtered_columns = self._filter_columns(dictionary_input, table_name, "insert")
            prepared_statement = self._prepare_dynamic_statement(filtered_columns, table_name, "insert")
            if exec_async:
                return self._bind_delete_or_insert(filtered_columns, prepared_statement, exec_async)
            self._bind_delete_or_insert(filtered_columns, prepared_statement)
        except Exception as e:
            self.log.error(f"Failed to insert data: {dictionary_input}, in table {table_name}")
            raise ApolloORMException(f"Failed to insert data: {dictionary_input} in table {table_name} - {e}")

    def delete(self, dictionary_input: Dict[str, Any], table_name: str, exec_async: bool = False) \
            -> Optional[ResponseFuture]:
        try:
            filtered_columns = self._filter_columns(dictionary_input, table_name, "delete")
            prepared_statement = self._prepare_dynamic_statement(filtered_columns, table_name, "delete")
            if exec_async:
                return self._bind_delete_or_insert(filtered_columns, prepared_statement, exec_async)
            self._bind_delete_or_insert(filtered_columns, prepared_statement)
        except Exception as e:
            self.log.error(f"Failed to delete data: {e}, in table {table_name}")
            raise ApolloORMException(f"Failed to delete data: {dictionary_input} in table {table_name} - {e}")

    def insert_concurrent(self, pre_processed_insert: PreProcessedInsertData, workers: int = 10) -> ResultList:
        errors: List[ResultProcess] = []
        successful: List[ResultProcess] = []
        if pre_processed_insert.errors:
            errors.extend(pre_processed_insert.errors)
        for statement, values in pre_processed_insert.statements_and_params.items():
            try:
                result = execute_concurrent_with_args(self.session, statement, values, concurrency=workers)
                for success, failed in result:
                    if failed:
                        errors.append(ResultProcess(str(failed), statement.query_string, values))
                        self.log.error(f"Failed to insert concurrent data: {failed}")
                    else:
                        successful.append(
                            ResultProcess("Data inserted successfully", statement.query_string, values))
            except Exception as e:
                self.log.error("Connection error: {e}")
                raise ApolloORMException(f"Failed to insert data: {statement} - {values} - Error Message: {e}")
        return ResultList(successful, errors)

    def _bind_delete_or_insert(self, filtered_columns: Dict[str, Column],
                               prepared_statement: PreparedStatement, exec_async: bool = False) \
            -> Optional[ResponseFuture]:
        values = [filtered_columns[column.hash_id].value for column in
                  sorted(filtered_columns.values(), key=lambda x: x.name)]
        if exec_async:
            return self._execute_async_query(prepared_statement, values)
        self._execute_query(prepared_statement, values)

    def _execute_async_query(self, statement: PreparedStatement, values: List[Any]) -> ResponseFuture:
        statement.is_idempotent = True
        self.log.info(f"Executing query: {statement.query_string} with values: {values}")
        try:
            return self.session.execute_async(statement, values)
        except (NoHostAvailable, ConnectionException) as e:
            self.log.error(f"Connection error: {e}. Reconnecting...")
            self.reconnect()
            return self.session.execute_async(statement, values)
        except Exception as e:
            self.log.error(f"Failed to execute query: {statement.query_string, values}")
            raise ApolloORMException(f"Failed to execute query: {statement.query_string, values} - {e}")

    def _execute_query(self, statement: PreparedStatement, values: List[Any]) -> ResultSet:
        self.log.info(f"Executing query: {statement.query_string} with values: {values}")
        try:
            return self.session.execute(statement, values)
        except Exception as e:
            self.log.error(f"Connection error: {e}")
            raise ApolloORMException(f"Failed to execute query: {statement.query_string, values} - {e}")

    def _prepare_dynamic_statement(self, columns: Dict[str, Column], table_name: str,
                                   query_type: str) -> PreparedStatement:
        keyspace = self._connection_config.credential.keyspace_name
        ordered_columns = sorted(columns.values(), key=lambda x: x.name)
        hashed_name = _text_to_hash(f"{query_type}-{table_name}".join([column.name for column in ordered_columns]))
        if hashed_name not in self._prepared_statements:
            try:
                if query_type == "select":
                    self._prepare_read(hashed_name, ordered_columns, keyspace, table_name)
                elif query_type == "insert":
                    self._prepare_insert(hashed_name, ordered_columns, keyspace, table_name, columns)
                elif query_type == "delete":
                    self._prepare_delete(hashed_name, ordered_columns, keyspace, table_name, columns)
            except Exception as e:
                self.log.error(f"Failed to prepare statement: {e}")
                raise ApolloORMException(f"Failed to prepare statement: {columns} - {e}")
        return self._prepared_statements[hashed_name]

    def _prepare_read(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str) -> None:
        try:
            values = _generate_pre_statement_labels(columns)
            statement = f"select * from {keyspace}.{table_name} where {' and '.join(values.values())}"
            self._prepare_statement(hashed_name, statement)
        except Exception as e:
            self.log.error(f"Failed to prepare read statement: {e}")
            raise ApolloORMException(f"Failed to prepare read statement: {columns} - {e}")

    def _prepare_insert(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str,
                        dict_columns: Optional[Dict[str, Column]] = None) -> None:
        hashed_statement = {}
        try:
            keys = [column.name for column in columns]
            for column in columns:
                hashed_statement[column.hash_id] = "?"
            statement = f"""insert into
                    {keyspace}.{table_name}
                    ({', '.join(keys)})
                    values ({', '.join(hashed_statement.values())})"""
            self._prepare_statement(hashed_name, statement)
        except Exception as e:
            self.log.error(f"Failed to prepare insert statement: {e}")
            raise ApolloORMException(f"Failed to prepare insert statement: {columns} - {e}")

    def _prepare_delete(self, hashed_name: str, columns: List[Column], keyspace: str, table_name: str,
                        dict_columns: Optional[Dict[str, Column]] = None) -> None:
        try:
            values = _generate_pre_statement_labels(columns)
            statement = f"delete from {keyspace}.{table_name} where {' and '.join(values.values())}"
            self._prepare_statement(hashed_name, statement)
        except Exception as e:
            self.log.error(f"Failed to prepare delete statement: {e}")
            raise ApolloORMException(f"Failed to prepare delete statement: {columns} - {e}")

    def _prepare_statement(self, hashed_name: str, statement: str) -> None:
        self._prepared_statements[hashed_name] = self.session.prepare(statement)
        self._prepared_statements[hashed_name].is_idempotent = True
