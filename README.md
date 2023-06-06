# Vertica SQLAlchemy Dialect

[![Build and Test](https://github.com/vertica/vertica-sqlalchemy-dialect/actions/workflows/dialecttest.yml/badge.svg)](https://github.com/vertica/vertica-sqlalchemy-dialect/actions/workflows/dialecttest.yml)
[![PyPi](https://img.shields.io/pypi/v/vertica-sqlalchemy-dialect.svg)](https://pypi.python.org/pypi/vertica-sqlalchemy-dialect/)
[![License Apache-2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

Vertica dialect for SQLAlchemy uses the pure-Python DB-API driver vertica-python, to connect a Vertica database and SQLAlchemy applications.

This repo was derived from a series of chained forks as outlined in the [Acknowledgement](https://github.com/vertica/vertica-sqlalchemy-dialect/blob/main/ACKNOWLEDGMENTS).

> **Warning**
> This dialect currently doesn't not have state-of-the-art features and support maybe limited based on Vertica developer availability. However, we encourage you to raise a PR to add new features that may help your SQLAlchemy application.

## Prerequisites

You will need the following softwares to run, build and test the dialect. Everything apart from Python and pip can be installed via pip itself.

1. Python 3.x or higher
2. pip 22 or higher
3. sqlalchemy>=1.3.24,<=1.4
4. vertica-python 1.1.1 or higher

### Vertica-Python

[vertica-python](https://github.com/vertica/vertica-python) is needed to use the Vertica-SQLAlchemy-Dialect. The connector does not need to be installed as the dialect installation takes care of it.

Note: We recommend using the vertica-python connector. However, the dialect also allows connecting using [pyodbc](https://pypi.org/project/pyodbc/). More instructions are at the end of this README.


## Installing Vertica SQLAlchemy Dialect

The Vertica SQLAlchemy package can be installed from the public PyPI repository using `pip`:

```shell
pip install --upgrade vertica-sqlalchemy-dialect
```

`pip` automatically installs all required modules, including vertica-python.

## Verifying Your Installation

1. Create a file (e.g. `validate.py`) that contains the following Python sample code,
   which connects to Vertica and displays the Vertica version:

    Using vertica-python
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

    For more details, see [Connection Parameters](https://github.com/vertica/vertica-sqlalchemy-dialect#connection-parameters).

3. Execute the sample code. For example, if you created a file named `validate.py`:

    ```shell
    python validate.py
    ```

    The Vertica version (e.g. `v12.0.1`) should be displayed.

## Parameters and Behavior

As much as possible, Vertica SQLAlchemy provides compatible functionality for SQLAlchemy applications. For information on using SQLAlchemy, see the [SQLAlchemy documentation](http://docs.sqlalchemy.org/en/latest/).

Note: Current state of the dialect only supports metadata functions. It is still under development. 

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


You can optionally specify the initial database and schema for the Vertica session by including them at the end of the connection string, separated by `/`. You can also specify other supported parameters by vertica-python at the end of the connection string:

```python
'vertica+vertica_python://<user>:<password>@<host_name>/<database_name>?session_label=sqlalchemy&connection_load_balance=1'
```
For more information, check out the connection [options](https://github.com/vertica/vertica-python#set-properties-with-connection-string) of vertica-python.

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

## Using pyodbc instead of vertica-python

You may use pyodbc instead of vertica-python for the connection.

### Create a Vertica DSN 


You will need to have a Vertica ODBC driver installed from [Vertica-Client-Drivers](https://www.vertica.com/download/vertica/client-drivers/). For steps to install ODBC for Vertica, follow official [Vertica Docs](https://www.vertica.com/docs/12.0.x/HTML/Content/Authoring/ConnectingToVertica/ClientODBC/InstallingODBC.htm).

For example, you will need to configure these files with your credentials:

`/etc/vertica.ini`
```
[Driver]
ErrorMessagesPath = /opt/vertica/lib64/
ODBCInstLib = /usr/lib/x86_64-linux-gnu/libodbcinst.so
DriverManagerEncoding=UTF-16
```

`~/.odbc.ini`
```
[ODBC Data Sources]
vertica = "My Database"

[verticadsn]
Description = My Database
Driver = /opt/vertica/lib64/libverticaodbc.so
Database = docker
Servername = 127.0.0.1
UID = dbadmin
PWD =
```


Then use the Vertica DSN in a file like so:
```python
from sqlalchemy import create_engine

engine = sa.create_engine('vertica+pyodbc://@verticadsn')
try:
    res = engine.connect().scalar('select version();')
    print(res)
finally:
    connection.close()
    engine.dispose()

```

This should display the Vertica version info: "Vertica Analytic Database v12.0.0-0".
