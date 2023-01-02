import sqlalchemy as sa
# vpyengine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart?session_label=sqlalchemy&tlsmode=server')   
# vpyconn = vpyengine.connect()
# vpyres = vpyengine.dialect._get_server_version_info(vpyconn)
# print(vpyres)

# vpyodbcengine = sa.create_engine('vertica+pyodbc://@verticadsn')
# vpyodbcconn= vpyodbcengine.connect()
# vpyodbcres = vpyodbcengine.dialect._get_server_version_info(vpyodbcconn)
# print(vpyodbcres)


from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Sequence, ForeignKey, PrimaryKeyConstraint, UniqueConstraint
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
meta.drop_all(engine)