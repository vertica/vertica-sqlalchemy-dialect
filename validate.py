import sqlalchemy as sa
# vpyengine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart?session_label=sqlalchemy&tlsmode=server')   
# vpyconn = vpyengine.connect()
# vpyres = vpyengine.dialect._get_server_version_info(vpyconn)
# print(vpyres)

# vpyodbcengine = sa.create_engine('vertica+pyodbc://@verticadsn')
# vpyodbcconn= vpyodbcengine.connect()
# vpyodbcres = vpyodbcengine.dialect._get_server_version_info(vpyodbcconn)
# print(vpyodbcres)


#engine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart?session_label=sqlalchemy)   

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
engine = create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart?session_label=sqlalchemy', echo = True)
meta = MetaData()

students = Table(
   'students', meta, 
   Column('id', Integer, primary_key = True), 
   Column('name', String), 
   Column('lastname', String),
)
meta.create_all(engine)