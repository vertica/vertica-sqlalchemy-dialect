import sqlalchemy as sa
engine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart?session_label=sqlalchemy&tlsmode=server')   
conn = engine.connect()
res = engine.dialect._get_server_version_info(conn)
print(res)

# import sqlalchemy as sa

# engine = sa.create_engine('vertica+pyodbc://@verticadsn')
# res = engine.connect().scalar('select version();')
# print(res)