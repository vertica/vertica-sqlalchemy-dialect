import decimal
import json
import os
import re
import string
import textwrap
import time
from datetime import date, datetime
from unittest.mock import patch
from sqlalchemy import sql

import pytest
# import pytz
from sqlalchemy import (
    REAL,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    Sequence,
    String,
    Table,
    UniqueConstraint,
    dialects,
    inspect,
    text,
)
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import and_, not_, or_, select

from vertica_python import Error, ProgrammingError, connect

# from vertica_sqlalchemy_dialect._constants import (
#     APPLICATION_NAME,
#     SNOWFLAKE_SQLALCHEMY_VERSION,
# )
from vertica_sqlalchemy_dialect.dialect_vertica_python import VerticaDialect as dialect

from .conftest import create_engine_with_future_flag as create_engine
from .conftest import get_engine
from .conftest import DEFAULT_PARAMETERS
from .conftest import TEST_SCHEMA
from .util import ischema_names_baseline, random_string, _url as URL

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

PST_TZ = "America/New_York"
JST_TZ = "Asia/Tokyo"


def _create_users_addresses_tables(
    engine_test, metadata, fk=None, pk=None, uq=None
):
    users = Table(
        "users",
        metadata,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )

    addresses = Table(
        "addresses",
        metadata,
        Column("id", Integer, Sequence("address_id_seq")),
        Column("user_id", None, ForeignKey("users.id", name=fk)),
        Column("email_address", String, nullable=False),
        PrimaryKeyConstraint("id", name=pk),
        UniqueConstraint("email_address", name=uq),
    )
    metadata.create_all(engine_test)
    return users, addresses

def _create_users_addresses_tables_without_sequence(engine_test, metadata):
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("fullname", String),
        schema=TEST_SCHEMA,
    )

    addresses = Table(
        "addresses",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", None, ForeignKey("%(schema)s.users.id" % {'schema': TEST_SCHEMA})),
        Column("email_address", String, nullable=False),
        schema=TEST_SCHEMA,
    )
    metadata.create_all(engine_test)
    return users, addresses

def test_verify_engine_connection(engine_test):
    with engine_test.connect() as conn:
        results = conn.execute(sql.text("select version()")).fetchone()
        assert results[0] == 'Vertica Analytic Database v12.0.2-0'

def test_simple_sql(engine_test):
    """
    Simple SQL by SQLAlchemy
    """
    with engine_test.connect() as conn:
        result = conn.scalar(sql.text("SELECT DBNAME();"))
    assert len(result) >= 0
    assert result.lower() == DEFAULT_PARAMETERS["database"].lower()

def test_create_drop_tables(engine_test):
    """
    Creates and Drops tables
    """
    metadata = MetaData(engine_test)
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_test, metadata
    )

    try:
        # validate the tables exists
        with engine_test.connect() as conn:
            chckuser = conn.execute(sql.text("select * from tables where table_schema ilike '%(schema)s' and table_name ilike '%(table)s';" % {'schema': TEST_SCHEMA, 'table': 'users'}))
            assert len([row for row in chckuser]) > 0, "users table doesn't exist"

            # validate the tables exists
            chckaddr = conn.execute(sql.text("select * from tables where table_schema ilike '%(schema)s' and table_name ilike '%(table)s';" % {'schema': TEST_SCHEMA, 'table': 'addresses'}))
            assert len([row for row in chckaddr]) > 0, "addresses table doesn't exist"
    finally:
        # drop tables
        addresses.drop(engine_test)
        users.drop(engine_test)

# def test_insert_tables(engine_test):
#     """
#     Inserts data into tables
#     """
#     metadata = MetaData(schema=TEST_SCHEMA)
#     users, addresses = _create_users_addresses_tables(engine_test, metadata)

#     with engine_test.connect() as conn:
#         try:
#             with conn.begin():
#                 # inserts data with an implicitly generated id
#                 results = conn.execute(
#                     users.insert().values(name="jack", fullname="Jack Jones")
#                 )
#                 # Note: SQLAlchemy 1.4 changed what ``inserted_primary_key`` returns
#                 #  a cast is here to make sure the test works with both older and newer
#                 #  versions
#                 # assert list(results.inserted_primary_key) == [1], "sequence value"
#                 results.close()

#                 # inserts data with the given id
#                 conn.execute(
#                     users.insert(),
#                     {"id": 2, "name": "wendy", "fullname": "Wendy Williams"},
#                 )

#                 # verify the results
#                 results = conn.execute(select(users))
#                 assert (
#                     len([row for row in results]) == 2
#                 ), "number of rows from users table"
#                 results.close()

#                 # fetchone
#                 results = conn.execute(select(users).order_by("id"))
#                 row = results.fetchone()
#                 results.close()
#                 assert row._mapping._data[2] == "Jack Jones", "user name"
#                 assert row._mapping["fullname"] == "Jack Jones", "user name by dict"
#                 assert (
#                     row._mapping[users.c.fullname] == "Jack Jones"
#                 ), "user name by Column object"

#                 conn.execute(
#                     addresses.insert(),
#                     [
#                         {"user_id": 1, "email_address": "jack@yahoo.com"},
#                         {"user_id": 1, "email_address": "jack@msn.com"},
#                         {"user_id": 2, "email_address": "www@www.org"},
#                         {"user_id": 2, "email_address": "wendy@aol.com"},
#                     ],
#                 )

#                 # more records
#                 results = conn.execute(select(addresses))
#                 assert (
#                     len([row for row in results]) == 4
#                 ), "number of rows from addresses table"
#                 results.close()

#                 # select specified column names
#                 results = conn.execute(
#                     select(users.c.name, users.c.fullname).order_by("name")
#                 )
#                 results.fetchone()
#                 row = results.fetchone()
#                 assert row._mapping["name"] == "wendy", "name"

#                 # join
#                 results = conn.execute(
#                     select(users, addresses).where(users.c.id == addresses.c.user_id)
#                 )
#                 results.fetchone()
#                 results.fetchone()
#                 results.fetchone()
#                 row = results.fetchone()
#                 assert row._mapping["email_address"] == "wendy@aol.com", "email address"

#         finally:
#             # drop tables
#             addresses.drop(engine_test)
#             users.drop(engine_test)