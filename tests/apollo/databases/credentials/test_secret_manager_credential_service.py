from boto3 import client
from moto import mock_aws
from apollo.orm.credentials.secrets_manager_credential_service import SecretsManagerCredentialService


@mock_aws()
def test_secret_manager_credentials_service():
    # Arrange
    client_secrets_manager = client('secretsmanager', region_name='sa-east-1')
    client_secrets_manager.create_secret(Name='user', SecretString='test_username')
    client_secrets_manager.create_secret(Name='password', SecretString='test_password')
    client_secrets_manager.create_secret(Name='hosts', SecretString='localhost1,localhost2')
    client_secrets_manager.create_secret(Name='port', SecretString='9042')
    client_secrets_manager.create_secret(Name='keyspace', SecretString='keyspace_name')
    service = SecretsManagerCredentialService()
    service.sm = client
    # Act
    credential = service.credential(username='username_secret', password='password_secret', hosts='hosts_secret',
                                    port='port_secret', keyspace='keyspace_secret')

    # Assert
    assert credential.user == 'test_username'
    assert credential.password == 'test_password'
    assert credential.hosts == ['localhost1', 'localhost2']
    assert credential.port == 9042
    assert credential.keyspace_name == 'keyspace_name'
