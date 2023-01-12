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

import sqlalchemy as sa
vpyengine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart?session_label=sqlalchemy&tlsmode=server')   
vpyconn = vpyengine.connect()
vpyres = vpyengine.dialect._get_server_version_info(vpyconn)
print(vpyres)

vpyodbcengine = sa.create_engine('vertica+pyodbc://@verticadsn')
vpyodbcconn= vpyodbcengine.connect()
vpyodbcres = vpyodbcengine.dialect._get_server_version_info(vpyodbcconn)
print(vpyodbcres)

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Sequence, ForeignKey, PrimaryKeyConstraint, UniqueConstraint, sql
engine = create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart?session_label=sqlalchemy', echo = True)
meta = MetaData()

# students = Table(
#    'students', meta, 
#    Column('id', Integer, primary_key = True), 
#    Column('name', String), 
#    Column('lastname', String),
# )
users = Table(
        "users",
        meta,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )
addresses = Table(
        "addresses",
        meta,
        Column("id", Integer, Sequence("address_id_seq")),
        Column("user_id", None, ForeignKey("users.id")),
        Column("email_address", String, nullable=False),
        PrimaryKeyConstraint("id"),
        UniqueConstraint("email_address"),
    )

meta.create_all(engine)
with engine.connect() as conn:
   try:
      with conn.begin():
         results = conn.execute(
                           users.insert().values(name="jack", fullname="Jack Jones")
                        )
         print(results.rowcount)
   finally:
      conn.close()
         
meta.drop_all(engine)