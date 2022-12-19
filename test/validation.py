import sqlalchemy as sa
#@TODO Sample test, extend
engine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/vmart')
try:
    conn = engine.connect()
    #results = engine.dialect.get_table_names(connection=conn)
    res2 = engine.dialect._get_properties_keys(conn, db_name='vmart', schema='public', level='schema')
    #results = connection.execute('select version();').fetchone()
    print(res2)
finally:
    conn.close()
    engine.dispose()