# import sqlalchemy as sa
# engine = sa.create_engine('vertica+pyodbc://dbadmin:abc123@localhost:5433/VMart')   
# conn = engine.connect()
# res = engine.dialect._get_server_version_info(conn)
# print(res)

import sqlalchemy as sa

engine = sa.create_engine('vertica+pyodbc://@verticadsn')
res = engine.connect().scalar('select version();')
print(res)