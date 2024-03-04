import json
import os

from apollo_orm.domains.models.entities.connection_config.entity import ConnectionConfig
from apollo_orm.domains.models.entities.credentials.entity import Credentials
from apollo_orm.orm.scylla import ScyllaService


def test_work_flow():
    json_data = json.loads(os.environ['JSON_DATA'])
    json_credentials = json.loads(os.environ['CREDENTIALS'])
    credentials = Credentials(**json_credentials)
    tables = os.environ['TABLES'].replace('"', '').replace('[', '').replace(']', '').split(',')
    connection_config = ConnectionConfig(credentials, tables)
    connection = ScyllaService(connection_config)
    connection.insert(json_data, tables[0])
    select_table = connection.select(json_data, tables[0])
    connection.delete(json_data, tables[0])
    select_deleted_table = connection.select(json_data, tables[0])

    assert len(select_table.current_rows) == 1
    assert len(select_deleted_table.current_rows) == 0
