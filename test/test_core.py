# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
# Copyright (c) 2017 StartApp Inc.
# Copyright (c) 2015 Locus Energy
# Copyright (c) 2013 James Casbon
# Copyright (c) 2010 Bo Shi

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
#     VERTICA_SQLALCHEMY_VERSION,
# )
from vertica_sqlalchemy_dialect.dialect_vertica_python import VerticaDialect as dialect

from .conftest import create_engine_with_future_flag as create_engine
from .conftest import get_engine
from .conftest import DEFAULT_PARAMETERS
from .conftest import TEST_SCHEMA
from .util import _url as URL

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

PST_TZ = "America/New_York"


def _create_employee_emails_tables(
    engine_test, metadata, fk=None, pk=None, uq=None
):
    employee = Table(
        "employee",
        metadata,
        Column("id", Integer, Sequence("emp_id_seq"), primary_key=True),
        Column("name", String),
        Column("full_name", String),
    )

    emails = Table(
        "emails",
        metadata,
        Column("id", Integer, Sequence("email_id_seq")),
        Column("employee_id", None, ForeignKey("employee.id", name=fk)),
        Column("email_address", String, nullable=False),
        PrimaryKeyConstraint("id", name=pk),
        UniqueConstraint("email_address", name=uq),
    )
    metadata.create_all(engine_test)
    return employee, emails

def _create_employee_emails_tables_without_sequence(engine_test, metadata):
    employee = Table(
        "employee",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("full_name", String),
        schema=TEST_SCHEMA,
    )

    emails = Table(
        "emails",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("employee_id", None, ForeignKey("%(schema)s.employee.id" % {'schema': TEST_SCHEMA})),
        Column("email_address", String, nullable=False),
        schema=TEST_SCHEMA,
    )
    metadata.create_all(engine_test)
    return employee, emails

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
    employee, emails = _create_employee_emails_tables_without_sequence(
        engine_test, metadata
    )

    try:
        # validate the tables exists
        with engine_test.connect() as conn:
            chckuser = conn.execute(sql.text("select * from tables where table_schema ilike '%(schema)s' and table_name ilike '%(table)s';" % {'schema': TEST_SCHEMA, 'table': 'employee'}))
            assert len([row for row in chckuser]) > 0, "employee table doesn't exist"

            # validate the tables exists
            chckaddr = conn.execute(sql.text("select * from tables where table_schema ilike '%(schema)s' and table_name ilike '%(table)s';" % {'schema': TEST_SCHEMA, 'table': 'emails'}))
            assert len([row for row in chckaddr]) > 0, "emails table doesn't exist"
    finally:
        # drop tables
        emails.drop(engine_test)
        employee.drop(engine_test)

def test_insert_tables(engine_test):
    """
    Inserts data into tables
    """
    metadata = MetaData(schema=TEST_SCHEMA)
    employee, emails = _create_employee_emails_tables(engine_test, metadata)

    with engine_test.connect() as conn:
        try:
            with conn.begin():
                # inserts data with an implicitly generated id
                results = conn.execute(
                    employee.insert().values(name="John", full_name="John Doe")
                )
                conn.execute(
                     employee.insert().values(name="Jaden", full_name="Jaden Smith")
                )
                # verify the results
                results = conn.execute(select(employee))
                assert (
                    len([row for row in results]) == 2
                ), "number of rows from employee table"
                results.close()

                # fetchone
                results = conn.execute(select(employee).order_by("id"))
                row = results.fetchone()
                results.close()
                assert row._mapping._data[2] == "John Doe", "Employee name"
                assert row._mapping["full_name"] == "John Doe", "Employee name by dict"
                assert (
                    row._mapping[employee.c.full_name] == "John Doe"
                ), "employee name by Column object"

                conn.execute(
                    emails.insert(),
                    [
                        {"employee_id": 1, "email_address": "john@gmail.com"},
                        {"employee_id": 1, "email_address": "jaden@hotmail.com"},
                        {"employee_id": 2, "email_address": "www@www.org"},
                        {"employee_id": 2, "email_address": "jaden@abc.com"},
                    ],
                )

                # more records
                results = conn.execute(select(emails))
                assert (
                    len([row for row in results]) == 4
                ), "number of rows from emails table"
                results.close()

                # select specified column names
                results = conn.execute(
                    select(employee.c.name, employee.c.full_name).order_by("name")
                )
                results.fetchone()
                row = results.fetchone()
                assert row._mapping["name"] == "John", "name"

                # join
                results = conn.execute(
                    select(employee, emails).where(employee.c.id == emails.c.employee_id)
                )
                results.fetchone()
                results.fetchone()
                results.fetchone()
                row = results.fetchone()
                assert row._mapping["email_address"] == "jaden@abc.com", "email address"

        finally:
            # drop tables
            emails.drop(engine_test)
            employee.drop(engine_test)