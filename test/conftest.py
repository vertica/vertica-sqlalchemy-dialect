import os
import sys
import time
import uuid
from functools import partial
from logging import getLogger

import pytest
from sqlalchemy import create_engine

import vertica_python as vpython
from vertica_sqlalchemy_dialect.dialect_vertica_python import VerticaDialect as dialect
from .util import _url as URL

TEST_SCHEMA = f"sqlalchemy_tests_{str(uuid.uuid4()).replace('-', '_')}"

create_engine_with_future_flag = create_engine

# For future not yet supported
def pytest_addoption(parser):
    parser.addoption(
        "--run_v20_sqlalchemy",
        help="Use only 2.0 SQLAlchemy APIs, any legacy features (< 2.0) will not be supported."
        "Turning on this option will set future flag to True on Engine and Session objects according to"
        "the migration guide: https://docs.sqlalchemy.org/en/14/changelog/migration_20.html",
        action="store_true",
    )

def help():
    print(
        """Connection parameter must be specified in parameters.py,
    for example:
CONNECTION_PARAMETERS = {
    'account': 'testaccount',
    'user': 'user1',
    'password': 'test',
    'database': 'testdb',
    'schema': 'public',
}"""
    )

logger = getLogger(__name__)

DEFAULT_PARAMETERS = {
    "user": "dbadmin",
    "password": "abc123",
    "database": "vmart",
    "host": "localhost",
    "port": "5433",
}

@pytest.fixture(scope="session")
def db_parameters():
    return get_db_parameters()

def get_db_parameters():
    """
    Sets the db connection parameters
    """
    ret = {}
    os.environ["TZ"] = "UTC"

    # for k, v in CONNECTION_PARAMETERS.items():
    #     ret[k] = v

    for k, v in DEFAULT_PARAMETERS.items():
        if k not in ret:
            ret[k] = v


    # a unique table name
    ret["name"] = ("sqlalchemy_tests_" + str(uuid.uuid4())).replace("-", "_")
    ret["schema"] = TEST_SCHEMA

    # This reduces a chance to exposing password in test output.
    ret["a00"] = "dummy parameter"
    ret["a01"] = "dummy parameter"
    ret["a02"] = "dummy parameter"
    ret["a03"] = "dummy parameter"
    ret["a04"] = "dummy parameter"
    ret["a05"] = "dummy parameter"
    ret["a06"] = "dummy parameter"
    ret["a07"] = "dummy parameter"
    ret["a08"] = "dummy parameter"
    ret["a09"] = "dummy parameter"
    ret["a10"] = "dummy parameter"
    ret["a11"] = "dummy parameter"
    ret["a12"] = "dummy parameter"
    ret["a13"] = "dummy parameter"
    ret["a14"] = "dummy parameter"
    ret["a15"] = "dummy parameter"
    ret["a16"] = "dummy parameter"

    return ret

def get_engine(user=None, password=None, host=None, port=None, database=None):
    """
    Creates a connection using the parameters defined in connect string
    """
    ret = get_db_parameters()

    if user is not None:
        ret["user"] = user
    if password is not None:
        ret["password"] = password
    if host is not None:
        ret["host"] = host
    if database is not None:
        ret["database"] = database
    

    from sqlalchemy.pool import NullPool
    from sqlalchemy import create_engine

    engine = create_engine(url=
    URL(
        user=ret["user"],
        password=ret["password"],
        host=ret["host"],
        port=ret["port"],
        database=ret["database"],
    ),
    poolclass=NullPool, echo=True)

    return engine, ret


@pytest.fixture
def engine_test(request):
    engine, _ = get_engine()
    
    def fin():
        engine.dispose()

    request.addfinalizer(fin)
    return engine

@pytest.fixture(scope="session", autouse=True)
def init_test_schema(request, db_parameters):
    ret = db_parameters
    conn_info = {
        'user': ret["user"],
        'password': ret["password"],
        'host': ret["host"],
        'port': ret["port"],
        'database': ret["database"]
    }
    with vpython.connect(**conn_info) as con:
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")

    def fin():
        with vpython.connect(**conn_info) as con1:
            con1.cursor().execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA}")

    request.addfinalizer(fin)

@pytest.fixture(scope="session")
def sql_compiler():
    return lambda sql_command: str(
        sql_command.compile(
            dialect=dialect(),
            compile_kwargs={"literal_binds": True, "deterministic": True},
        )
    ).replace("\n", "")

@pytest.fixture(scope="session")
def run_v20_sqlalchemy(pytestconfig):
    return pytestconfig.option.run_v20_sqlalchemy

def pytest_runtest_setup(item) -> None:
    """Ran before calling each test, used to decide whether a test should be skipped."""
    pass