import datetime
import hashlib
import re
import unittest
from unittest.mock import patch, MagicMock
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.connection import ConnectionException

from apollo_orm.domains.models.entities.column.entity import Column
from apollo_orm.domains.models.entities.table_config.entity import TableConfig
from apollo_orm.orm.core import ORMInstance, ApolloORMException, _generate_pre_statement_labels, _text_to_hash, \
    _column_name_to_hash, _type_validate, _timestamp_validate
from apollo_orm.domains.models.entities.connection_config.entity import ConnectionConfig
from apollo_orm.domains.models.entities.credentials.entity import Credentials


class TestORMInstance(unittest.TestCase):

    @patch.object(Cluster, 'connect')
    @patch.object(PlainTextAuthProvider, '__init__', return_value=None)
    def test_connect(self, auth_init_mock, connect_mock):
        # Arrange
        credential = Credentials(['localhost'], 9042, 'user', 'password', 'keyspace')
        connection_config = ConnectionConfig(credential, ['table1', 'table2'])

        # Act
        ORMInstance(connection_config)

        # Assert
        connect_mock.assert_called_once()
        auth_init_mock.assert_called_with(username='user', password='password')

    @patch.object(Cluster, 'connect')
    def test_connect_exception(self, connect_mock):
        # Arrange
        credential = Credentials(['localhost'], 9042, 'user', 'password', 'keyspace')
        connection_config = ConnectionConfig(credential, ['table1', 'table2'])
        orm_instance = ORMInstance(connection_config)
        connect_mock.side_effect = Exception('Connection error')

        # Act & Assert
        with self.assertRaises(ApolloORMException):
            orm_instance.connect()

    @patch.object(Cluster, 'connect')
    def test_close(self, connect_mock):
        # Arrange
        credential = Credentials(['localhost'], 9042, 'user', 'password', 'keyspace')
        connection_config = ConnectionConfig(credential, ['table1', 'table2'])
        orm_instance = ORMInstance(connection_config)

        # Act
        orm_instance.close()

        # Assert
        connect_mock.return_value.shutdown.assert_called_once()

    @patch.object(Cluster, 'connect')
    def test_reconnect(self, connect_mock):
        # Arrange
        credential = Credentials(['localhost'], 9042, 'user', 'password', 'keyspace')
        connection_config = ConnectionConfig(credential, ['table1', 'table2'])
        orm_instance = ORMInstance(connection_config)

        # Act
        orm_instance.reconnect()

        # Assert
        connect_mock.return_value.shutdown.assert_called_once()
        connect_mock.assert_called()

    def test_generate_pre_statement_labels(self):
        # Arrange
        columns = [
            Column('hash1', 'name1', 'partition_key', 'type1'),
            Column('hash2', 'name2', 'clustering', 'type2'),
            Column('hash3', 'name3', 'other', 'type3')
        ]

        expected_result = {
            'hash1': 'name1 = ?',
            'hash2': 'name2 = ?'
        }

        # Act
        result = _generate_pre_statement_labels(columns)

        # Assert
        self.assertEqual(result, expected_result)

    def test_text_to_hash(self):
        # Arrange
        text = "Test String 123"
        expected_hash = hashlib.md5(re.sub(r'[^a-zA-Z0-9]+', '', text).lower().encode()).hexdigest()

        # Act
        result = _text_to_hash(text)

        # Assert
        self.assertEqual(result, expected_hash)

    def test_column_name_to_hash(self):
        # Arrange
        parameters = {"TestKey1": "value1", "TestKey2": "value2"}
        expected_hashed_names = {_text_to_hash(key): value for key, value in parameters.items()}

        # Act
        result = _column_name_to_hash(parameters)

        # Assert
        self.assertEqual(result, expected_hashed_names)

    def test_type_validate_select(self):
        # Arrange
        column = Column('hash1', 'name1', 'partition_key', 'type1')
        hashed_columns = {'hash1': 'value1'}
        type_process = 'select'

        # Act
        result = _type_validate(column, hashed_columns, type_process)

        # Assert
        self.assertEqual(result.value, 'value1')

    def test_type_validate_non_select(self):
        # Arrange
        column = Column('hash1', 'name1', 'partition_key', 'type1')
        hashed_columns = {'hash1': 'value1'}
        type_process = 'insert'

        # Act
        result = _type_validate(column, hashed_columns, type_process)

        # Assert
        self.assertEqual(result.value, 'value1')

    def test_type_validate_non_select_non_key(self):
        # Arrange
        column = Column('hash1', 'name1', 'other', 'type1')
        hashed_columns = {'hash1': 'value1'}
        type_process = 'insert'

        # Act
        result = _type_validate(column, hashed_columns, type_process)

        # Assert
        self.assertIsNone(result)

    @patch.object(ORMInstance, '_scan_tables')
    @patch('cassandra.cluster.Cluster.connect')
    def test_scan_tables_called_on_init(self, connect_mock, scan_tables_mock):
        # Arrange
        credential = Credentials(['localhost'], 9042, 'user', 'password', 'keyspace')
        connection_config = ConnectionConfig(credential, ['table1', 'table2'])

        # Act
        ORMInstance(connection_config)

        # Assert
        scan_tables_mock.assert_called_once()

    @patch.object(ORMInstance, '__init__', return_value=None)
    def test_connect_no_config(self, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance._connection_config = None

        # Act & Assert
        with self.assertRaises(ApolloORMException) as context:
            orm_instance.connect()

        self.assertIn("Connection config is not set", str(context.exception))

    @patch.object(ORMInstance, '__init__', return_value=None)
    def test_reload_prepared_statements(self, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance._prepared_statements = {'statement1': MagicMock(query_string='query1'),
                                               'statement2': MagicMock(query_string='query2')}
        orm_instance.session = MagicMock()

        # Act
        orm_instance._reload_prepared_statements()

        # Assert
        orm_instance.session.prepare.assert_any_call('query1')
        orm_instance.session.prepare.assert_any_call('query2')

    @patch.object(ORMInstance, '__init__', return_value=None)
    def test_add_to_table_config(self, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        table_config = TableConfig('keyspace', 'table', [])

        # Act
        orm_instance._add_to_table_config(table_config)

        # Assert
        self.assertEqual(orm_instance._table_config, [table_config])

    @patch.object(ORMInstance, '__init__', return_value=None)
    def test_scan_tables(self, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance._connection_config = MagicMock(tables=['table1', 'table2'])
        orm_instance.session = MagicMock()
        mock_config_row = MagicMock(column_name='column1', kind='kind1', type='type1')
        orm_instance.session.execute.return_value = [mock_config_row]

        # Act
        orm_instance._scan_tables()

        # Assert
        self.assertEqual(len(orm_instance._table_config), 2)
        for table_config in orm_instance._table_config:
            self.assertEqual(len(table_config.columns), 1)
            column = table_config.columns[0]
            self.assertEqual(column.name, 'column1')
            self.assertEqual(column.kind, 'kind1')
            self.assertEqual(column.type, 'type1')

    @patch.object(ORMInstance, '__init__', return_value=None)
    def test_check_partition_key_columns(self, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance._connection_config = ConnectionConfig(MagicMock(), ['table1'])
        orm_instance._table_config = [TableConfig('keyspace', 'table1', [
            Column('hash1', 'name1', 'partition_key', 'type1'),
            Column('hash2', 'name2', 'partition_key', 'type2'),
            Column('hash3', 'name3', 'other', 'type3')
        ])]

        # Act & Assert
        # Case when all partition key columns are present
        columns = {'hash1': Column('hash1', 'name1', 'partition_key', 'type1'),
                   'hash2': Column('hash2', 'name2', 'partition_key', 'type2')}
        orm_instance._check_partition_key_columns(columns, 'table1')

        # Case when some partition key columns are missing
        columns = {'hash1': Column('hash1', 'name1', 'partition_key', 'type1')}
        with self.assertRaises(ApolloORMException) as context:
            orm_instance._check_partition_key_columns(columns, 'table1')
        self.assertIn("All partition keys columns must be passed as parameter", str(context.exception))

    @patch.object(ORMInstance, '__init__', return_value=None)
    def test_check_clustering_columns(self, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance._connection_config = ConnectionConfig(MagicMock(), ['table1'])
        orm_instance._table_config = [TableConfig('keyspace', 'table1', [
            Column('hash1', 'name1', 'clustering', 'type1'),
            Column('hash2', 'name2', 'clustering', 'type2'),
            Column('hash3', 'name3', 'other', 'type3')
        ])]

        # Act & Assert
        # Case when all clustering columns are present
        columns = {'hash1': Column('hash1', 'name1', 'clustering', 'type1'),
                   'hash2': Column('hash2', 'name2', 'clustering', 'type2')}
        orm_instance._check_clustering_columns(columns, 'table1')

        # Case when some clustering columns are missing
        columns = {'hash1': Column('hash1', 'name1', 'clustering', 'type1')}
        with self.assertRaises(ApolloORMException) as context:
            orm_instance._check_clustering_columns(columns, 'table1')
        self.assertIn("All clustering columns must be passed as parameter", str(context.exception))

    @patch.object(ORMInstance, '__init__', return_value=None)
    @patch.object(ORMInstance, 'select')
    def test_select_from_json(self, select_mock, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        json_input = '{"key1": "value1", "key2": "value2"}'
        table_name = 'table1'
        expected_dict = {"key1": "value1", "key2": "value2"}

        # Act
        orm_instance.select_from_json(json_input, table_name)

        # Assert
        select_mock.assert_called_once_with(expected_dict, table_name)

    @patch.object(ORMInstance, '__init__', return_value=None)
    @patch.object(ORMInstance, '_filter_columns')
    def test_select(self, filter_columns_mock, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance.session = MagicMock()
        orm_instance.log = MagicMock()
        orm_instance.reconnect = MagicMock()
        dictionary_input = {"key1": "value1", "key2": "value2"}
        table_name = 'table1'
        filtered_columns = {
            'hash1': Column('hash1', 'name1', 'partition_key', 'type1'),
            'hash2': Column('hash2', 'name2', 'clustering', 'type2')
        }
        filter_columns_mock.return_value = filtered_columns
        prepared_statement_mock = MagicMock()
        orm_instance._prepare_dynamic_statement = MagicMock(return_value=prepared_statement_mock)

        # Act
        orm_instance.select(dictionary_input, table_name)

        # Assert
        orm_instance.session.execute.assert_called()
        orm_instance._prepare_dynamic_statement.assert_called_once_with(filtered_columns, table_name, "select")
        prepared_statement_mock.bind.assert_called_once_with(['value1', 'value2'])

    @patch.object(ORMInstance, '__init__', return_value=None)
    @patch.object(ORMInstance, '_filter_columns')
    def test_select_connection_exception(self, filter_columns_mock, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance.session = MagicMock()
        orm_instance.log = MagicMock()
        orm_instance.reconnect = MagicMock()
        dictionary_input = {"key1": "value1", "key2": "value2"}
        table_name = 'table1'
        filtered_columns = {
            'hash1': Column('hash1', 'name1', 'partition_key', 'type1'),
            'hash2': Column('hash2', 'name2', 'clustering', 'type2')
        }
        filter_columns_mock.return_value = filtered_columns
        prepared_statement_mock = MagicMock()
        orm_instance._prepare_dynamic_statement = MagicMock(return_value=prepared_statement_mock)
        orm_instance.session.execute.side_effect = [ConnectionException('Connection error'), None]

        # Act
        orm_instance.select(dictionary_input, table_name)

        # Assert
        orm_instance.reconnect.assert_called_once()
        orm_instance.log.error.assert_called_once_with('Connection error: Connection error')

    @patch.object(ORMInstance, '__init__', return_value=None)
    @patch.object(ORMInstance, '_check_clustering_columns')
    def test_prepare_delete(self, check_clustering_columns_mock, init_mock):
        # Arrange
        orm_instance = ORMInstance()
        orm_instance.session = MagicMock()
        hashed_name = 'hashed_name'
        columns = [Column('hash1', 'name1', 'partition_key', 'type1'),
                   Column('hash2', 'name2', 'clustering', 'type2')]
        keyspace = 'keyspace'
        table_name = 'table1'
        dict_columns = {'hash1': Column('hash1', 'name1', 'partition_key', 'type1')}

        # Act
        orm_instance._prepare_delete(hashed_name, columns, keyspace, table_name, dict_columns)

        # Assert
        check_clustering_columns_mock.assert_called_once_with(dict_columns, table_name)
        orm_instance.session.prepare.assert_called_once()

    def test_timestamp_validate(self):
        date_test_one = _timestamp_validate('2024-02-28T23:48:52.232Z')
        date_test_two = _timestamp_validate('2024-02-29T23:48:52.232Z')
        date_test_three = _timestamp_validate('2024-02-28T23:48:52.232')
        date_test_four = _timestamp_validate('2024-02-28T23:48:52')
        date_test_five = _timestamp_validate('2024-02-28')
        date_test_six = _timestamp_validate('2024-02-29T00:00:13.895Z')

        self.assertEqual(date_test_one, datetime.datetime(2024, 2, 28, 23, 48, 52, 232000))
        self.assertEqual(date_test_two, datetime.datetime(2024, 2, 29, 23, 48, 52, 232000))
        self.assertEqual(date_test_three, datetime.datetime(2024, 2, 28, 23, 48, 52, 232000))
        self.assertEqual(date_test_four, datetime.datetime(2024, 2, 28, 23, 48, 52))
        self.assertEqual(date_test_five, datetime.datetime(2024, 2, 28))
        self.assertEqual(date_test_six, datetime.datetime(2024, 2, 29, 0, 0, 13, 895000))


if __name__ == '__main__':
    unittest.main()
