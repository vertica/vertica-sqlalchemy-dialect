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
    )

    addresses = Table(
        "addresses",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", None, ForeignKey("users.id")),
        Column("email_address", String, nullable=False),
    )
    metadata.create_all()
    return users, addresses

def test_verify_engine_connection(engine_test):
    with engine_test.connect() as conn:
        results = conn.execute(sql.text("select version()")).fetchone()
        assert results[0] == 'Vertica Analytic Database v12.0.0-0'

def test_simple_sql(engine_test):
    """
    Simple SQL by SQLAlchemy
    """
    with engine_test.connect() as conn:
        result = conn.scalar(sql.text("SELECT DBNAME();"))
    assert len(result) >= 0
    assert result.lower() == DEFAULT_PARAMETERS["database"].lower()

# @TODO Fix the test  as create table fails
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
            results = conn.execute(sql.text("select * from %(schema)s.users;" % {'schema': TEST_SCHEMA.lower()}))
            assert len([row for row in results]) > 0, "users table doesn't exist"

            # # validate the tables exists
            # results = conn.execute(sql.text("desc table addresses"))
            # assert len([row for row in results]) > 0, "addresses table doesn't exist"
    finally:
        # drop tables
        addresses.drop(engine_test)
        users.drop(engine_test)