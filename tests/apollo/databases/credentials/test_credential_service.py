from apollo_orm.orm.credentials.credential_service import CredentialService


def test_credential_service():
    service = CredentialService(
        hosts=["localhost1", "localhost2"],
        port=9042,
        user="user",
        password="password",
        keyspace_name="keyspace_name"
    )

    credential = service.credential()

    assert credential.hosts == ["localhost1", "localhost2"]
    assert credential.port == 9042
    assert credential.user == "user"
    assert credential.password == "password"
    assert credential.keyspace_name == "keyspace_name"
