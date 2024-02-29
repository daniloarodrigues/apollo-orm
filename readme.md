# Apollo ORM for ScyllaDB

This is the Apollo ORM library for ScyllaDB, a Python library that provides an Object-Relational Mapping (ORM) interface to ScyllaDB.

## Installation

You can install the Apollo ORM library via pip:

```bash
pip install apollo-orm
```

## Usage

Here's a basic example of how to use the Apollo ORM library:

```python
from apollo_orm.orm.scylla import ScyllaService
from apollo_orm.domains.models.entities.connection_config.entity import ConnectionConfig
from apollo_orm.orm.credentials.credential_service import CredentialService

data = {
    "id": "1",
    "name": "John Doe",
    "age": 30
}

# Initialize the Credentials
credentials = CredentialService(
    ["localhost1", "localhost2", "localhost3"],
    9042,
    "user",
    "password",
    "keyspace_name"
).credential()

# Initialize the ConnectionConfig
connection_config = ConnectionConfig(
    credentials,
    ["table_name1", "table_name2"])

# Initialize the ScyllaService
scylla_service = ScyllaService(connection_config)

# Insert data into a table
scylla_service.insert(data, "table_name1")

# Select data from a table
results = scylla_service.select(data, "table_name1")

# Print the results
for result in results:
    print(result)
```

## Contributing

Contributions are welcome! Please open an issue if you encounter any problems, or a pull request if you have improvements to suggest.

## License

This project is licensed under the MIT License.
```
MIT License

Copyright (c) 2023 Danilo Rodrigues

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.