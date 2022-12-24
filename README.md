# sqlalchemy-vertica-dialect

[![Build and Test](https://github.com/vishalkSimplify/sqlalchemy-vertica-dialect/actions/workflows/dialecttest.yml/badge.svg)](https://github.com/vishalkSimplify/sqlalchemy-vertica-dialect/actions/workflows/dialecttest.yml)
[![PyPi](https://img.shields.io/pypi/v/sqlalchemy-vertica-dialect.svg)](https://pypi.python.org/pypi/sqlalchemy-vertica-dialect/)
[![License Apache-2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

Vertica dialect for SQLAlchemy uses the pure-Python DB-API driver vertica-python, to connect a Vertica database and SQLAlchemy applications.

## Prerequisites

### Vertica Python

[!vertica-python](https://github.com/vertica/vertica-python) is needed to use the SQLAlchemy-Vertica-Dialect however, the connector does not need to be installed as the dialect installation takes care of it.


## Installing Vertica SQLAlchemy Dialect

The Vertica SQLAlchemy package can be installed from the public PyPI repository using `pip`:

```shell
pip install --upgrade vertica-sqlalchemy-dialect
```

`pip` automatically installs all required modules, including vertica-python.

## Verifying Your Installation

1. Create a file (e.g. `validate.py`) that contains the following Python sample code,
   which connects to Vertica and displays the Vertica version:

    ```python
    from sqlalchemy import create_engine

    engine = create_engine(
        'vertica+vertica_python://{user}:{password}@{host}:{port}/{db}'.format(
            user='<your_user_login_name>',
            password='<your_password>',
            host='<your_Host_IP>',
            port='<your_db_port>',
            db='your_db_name'
        )
    )
    try:
        connection = engine.connect()
        results = connection.execute('select version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
    ```

2. Replace the credentials with the values for Vertica DB and user.

    For more details, see [Connection Parameters](#connection-parameters).

3. Execute the sample code. For example, if you created a file named `validate.py`:

    ```shell
    python validate.py
    ```

    The Vertica version (e.g. `v12.0.1`) should be displayed.

## Parameters and Behavior

As much as possible, Vertica SQLAlchemy provides compatible functionality for SQLAlchemy applications. For information on using SQLAlchemy, see the [SQLAlchemy documentation](http://docs.sqlalchemy.org/en/latest/).

However, Vertica SQLAlchemy also provides specific parameters and behavior, which are described in the following sections.

### Connection Parameters

Vertica SQLAlchemy Dialect uses the following syntax for the connection string used to connect to Vertica and initiate a session:

```python
'vertica+vertica_python://<user>:<password>@<host_name>/<database_name>'
```

Where:

- `<user>` is the login name for your Vertica user.
- `<password>` is the password for your Vertica user.
- `<host_name>` is the IP/FQDN of your Vertica Host.
- `<database_name>` is the name of your Vertica Database.


You can optionally specify the initial database and schema for the Vertica session by including them at the end of the connection string, separated by `/`. You can also specify the initial warehouse and role for the session as a parameter string at the end of the connection string:

```python
'vertica+vertica_python://<user>:<password>@<host_name>/<database_name>'
```

<!-- #### Escaping Special Characters such as `%, @` signs in Passwords

As pointed out in [SQLAlchemy](https://docs.sqlalchemy.org/en/14/core/engines.html#escaping-special-characters-such-as-signs-in-passwords), URLs
containing special characters need to be URL encoded to be parsed correctly. This includes the `%, @` signs. Unescaped password containing special
characters could lead to authentication failure.

The encoding for the password can be generated using `urllib.parse`:
```python
import urllib.parse
urllib.parse.quote("kx@% jj5/g")
'kx%40%25%20jj5/g'
```

**Note**: `urllib.parse.quote_plus` may also be used if there is no space in the string, as `urllib.parse.quote_plus` will replace space with `+`.

To create an engine with the proper encodings, either manually constructing the url string by formatting
or taking advantage of the `snowflake.sqlalchemy.URL` helper method:
```python
import urllib.parse
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

quoted_password = urllib.parse.quote("kx@% jj5/g") -->

<!-- # 1. manually constructing an url string
url = f'snowflake://testuser1:{quoted_password}@abc123/testdb/public?warehouse=testwh&role=myrole'
engine = create_engine(url)

# 2. using the snowflake.sqlalchemy.URL helper method
engine = create_engine(URL(
    account = 'abc123',
    user = 'testuser1',
    password = quoted_password,
    database = 'testdb',
    schema = 'public',
    warehouse = 'testwh',
    role='myrole',
))
```

**Note**:
After login, the initial database, schema, warehouse and role specified in the connection string can always be changed for the session.

The following example calls the `create_engine` method with the user name `testuser1`, password `0123456`, account name `abc123`, database `testdb`, schema `public`, warehouse `testwh`, and role `myrole`:

```python
from sqlalchemy import create_engine
engine = create_engine(
    'snowflake://testuser1:0123456@abc123/testdb/public?warehouse=testwh&role=myrole'
)
```

Other parameters, such as `timezone`, can also be specified as a URI parameter or in `connect_args` parameters. For example:

```python
from sqlalchemy import create_engine
engine = create_engine(
    'snowflake://testuser1:0123456@abc123/testdb/public?warehouse=testwh&role=myrole',
    connect_args={
        'timezone': 'America/Los_Angeles',
    }
)
```

For convenience, you can use the `snowflake.sqlalchemy.URL` method to construct the connection string and connect to the database. The following example constructs the same connection string from the previous example:

```python
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

engine = create_engine(URL(
    account = 'abc123',
    user = 'testuser1',
    password = '0123456',
    database = 'testdb',
    schema = 'public',
    warehouse = 'testwh',
    role='myrole',
    timezone = 'America/Los_Angeles',
))
```

#### Using a proxy server

Use the supported environment variables, `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` to configure a proxy server. -->

### Opening and Closing Connection

Open a connection by executing `engine.connect()`; avoid using `engine.execute()`. Make certain to close the connection by executing `connection.close()` before
`engine.dispose()`; otherwise, the Python Garbage collector removes the resources required to communicate with Vertica, preventing the Python connector from closing the session properly.

```python
# Example
engine = create_engine(...)
connection = engine.connect()
try:
    connection.execute(<SQL>)
finally:
    connection.close()
    engine.dispose()
```
