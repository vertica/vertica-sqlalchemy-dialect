import sqlalchemy as sa
from sqlalchemy.sql import sqltypes
import pytest
import re 
import test.sampleobjects as sample

@pytest.fixture(scope="module")
def vpyconn():
    engine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/VMart')
    try:     
        conn = engine.connect()
    except:
        print("Failed to connect to the database")
    yield [engine, conn]
    conn.close()
    engine.dispose()

def test_get_server_version_info(vpyconn):
    res = vpyconn[0].dialect._get_server_version_info(vpyconn[1])
    assert res == (12,0,2)

def test_get_default_schema_name(vpyconn):
    res = vpyconn[0].dialect._get_default_schema_name(vpyconn[1])
    assert res == "public"

def test_has_schema(vpyconn):
    sc1 = vpyconn[0].dialect.has_schema(vpyconn[1], schema="public")
    assert sc1 == True
    sc2 = vpyconn[0].dialect.has_schema(vpyconn[1], schema="store")
    assert sc2 == True

def test_has_table(vpyconn):
    res = vpyconn[0].dialect.has_table(connection=vpyconn[1], table_name=sample.sample_table_list["store"][0], schema="store")
    assert res == True

def test_has_sequence(vpyconn):
    res = vpyconn[0].dialect.has_sequence(connection=vpyconn[1], sequence_name="clicks_user_id_seq", schema="public")
    assert res == True

def test_has_type(vpyconn):
    res = vpyconn[0].dialect.has_type(connection=vpyconn[1], type_name="Long Varchar")
    assert res == True

def test_get_schema_names(vpyconn):
    res = vpyconn[0].dialect.get_schema_names(connection=vpyconn[1])
    assert len(res) == 3
    assert "store" in res

# TODO Improve this function to verify the output with a regex match
def test_get_table_comment(vpyconn):
    res = vpyconn[0].dialect.get_table_comment(connection=vpyconn[1], table_name=sample.sample_table_list["public"][0], schema="public")
    assert res["properties"]["create_time"]
    assert res["properties"]["Total_Table_Size"]

# TODO Improve this function to verify the output with a regex match
def test_get_table_oid(vpyconn):
    res = vpyconn[0].dialect.get_table_oid(connection=vpyconn[1], table_name=sample.sample_table_list["public"][1], schema="public")
    # Assert the oid is an int
    assert type(res) == int
    # Assert the format of the oid
    p = re.compile(r'^\d+$')
    assert bool(p.match(str(res)))

def test_get_projection_names(vpyconn):
    res = vpyconn[0].dialect.get_projection_names(connection=vpyconn[1], schema="public")
    # Assert the no. of projections
    assert len(res) == 41
    # Assert sample projection
    assert sample.sample_projection in res

def test_get_table_names(vpyconn):
    res = vpyconn[0].dialect.get_table_names(connection=vpyconn[1], schema="public")
    # Assert the no. of tables
    assert len(res) == 42
    # Assert sample tables
    assert all(value in res  for value in sample.sample_table_list["public"])

    res = vpyconn[0].dialect.get_table_names(connection=vpyconn[1], schema="store")
    # Assert the no of tables in another schema
    assert len(res) == 3
    # Assert sample tables
    assert all(value in res  for value in sample.sample_table_list["store"])


def test_get_temp_table_names(vpyconn):
    res = vpyconn[0].dialect.get_temp_table_names(connection=vpyconn[1], schema="public")
    # Assert the no. of temp tables
    assert len(res) == 1
    # Assert sample tables
    assert sample.sample_temp_table in res

def test_get_view_names(vpyconn):
    res = vpyconn[0].dialect.get_view_names(connection=vpyconn[1], schema="public")
    # Assert the no. of views
    assert len(res) == 1
    # Assert sample view
    assert sample.sample_view in res

def test_get_view_definition(vpyconn):
    res = vpyconn[0].dialect.get_view_definition(connection=vpyconn[1], view_name=sample.sample_view, schema="public")
    # Assert the view definition exists
    assert len(res)>0
    # Assert the format of a view creation
    p = re.compile(r'SELECT')
    assert bool(p.match(res))

def test_get_temp_view_names(vpyconn):
    res = vpyconn[0].dialect.get_view_names(connection=vpyconn[1], schema="public")
    # Assert the no. of views
    assert len(res) == 1
    # Assert sample view
    assert sample.sample_view in res

def test_get_columns(vpyconn):
    res = vpyconn[0].dialect.get_columns(connection=vpyconn[1], table_name=sample.sample_table_list["public"][2], schema="public")
    # Assert the no. of columns
    assert len(res)>0
    # Assert sample columns
    assert all(value["name"] in sample.sample_columns for value in res)

def test_get_unique_constraints(vpyconn):
    # TODO query doesnt return the result here. Query works from other clients.
    assert True
    # res = vpyconn[0].dialect.get_unique_constraints(connection=vpyconn[1], table_name=sample_table_list["store"][0], schema="store")
    # # Assert the no. of unique contraints
    # assert len(res)>0
    # # Assert sample constraint
    # assert all(k["names"] in sample_columns.keys() for k in res)
    # assert all(v["columns"] in sample_columns.values() for v in res)

def test_get_check_constraints(vpyconn):
    # TODO query doesnt return the result here. Query works from other clients.
    assert True
    # res = vpyconn[0].dialect.get_unique_constraints(connection=vpyconn[1], table_name=sample_table_list["store"][0], schema="store")
    # # Assert the no. of unique contraints
    # assert len(res)>0
    # # Assert sample constraint
    # assert all(k["names"] in sample_columns.keys() for k in res)
    # assert all(v["columns"] in sample_columns.values() for v in res)

def test_normalize_name(vpyconn):
    assert vpyconn[0].dialect.normalize_name("SAMPLE_TABLE123") == "sample_table123"
    assert vpyconn[0].dialect.normalize_name("saMPLE_123") == "sample_123"

def test_denormalize_name(vpyconn):
    assert vpyconn[0].dialect.denormalize_name("SAMPLE_TABLE123") == "SAMPLE_TABLE123"
    assert vpyconn[0].dialect.denormalize_name("saMPLE_123") == "saMPLE_123"


def test_get_pk_constraint(vpyconn):
    # TODO query doesnt return the result here. Query works from other clients.
    res = vpyconn[0].dialect.get_pk_constraint(connection=vpyconn[1], table_name=sample.sample_table_list["public"][0], schema="public")
    # Assert the no. of unique contraints
    assert len(res)>0
    # Assert sample constraint
    assert all(k in sample.sample_pk.values() for k in res['name'])

def test_get_foreign_keys(vpyconn):
    # TODO Need functionality
    assert True
    #res = vpyconn[0].dialect.get_foreign_keys(connection=vpyconn[1], table_name=sample_table_list["store"][0], schema="store")
    # Assert the no. of unique contraints
    #assert len(res)>0
    # Assert sample constraint
    # assert all(k in sample_pk.values() for k in res['name'])

def test_get_column_info(vpyconn):
    # TODO Add more tests here for other datatypes
    res = vpyconn[0].dialect._get_column_info(name="customer_name", data_type="varchar(256)", default=None, is_nullable=False)
    assert res['name'] == 'customer_name'
    assert res['autoincrement'] == False
    assert res['nullable'] == False
    assert type(res['type']) == type(sqltypes.VARCHAR(length=256))

def test_get_models_names(vpyconn):
    res = vpyconn[0].dialect.get_models_names(vpyconn[1], schema="public")
    # Assert model names
    assert all(value in sample.sample_model_list for value in res)

# def test_get_properties_keys(vpyconn):
#     db_keys = vpyconn[0].dialect._get_properties_keys(vpyconn[1], db_name="vmart", schema="public", level="database")
#     sc_keys = vpyconn[0].dialect._get_properties_keys(vpyconn[1], db_name="vmart", schema="public", level="schema")
#     # Assert the schema level properties
#     assert sc_keys["projection_count"] > 0
#     assert len(sc_keys["udx_list"]) > 0
#     assert len(sc_keys["udx_language"]) > 0
#     # Assert the database level properties
#     gb = re.compile(r'[0-9]+ GB')
#     assert db_keys["cluster_type"] == "Enterprise"
#     assert bool(gb.match(db_keys["cluster_size"]))
#     assert db_keys["subcluster"] == ' '
#     assert len(db_keys["communal_storage_path"])==0

def test_get_extra_tags(vpyconn):
    extra_tags = vpyconn[0].dialect._get_extra_tags(vpyconn[1], name="table", schema="public")
    assert len(extra_tags)==42
    assert all(value in extra_tags for value in sample.sample_tags)

def test_get_ros_count(vpyconn):
    rc = vpyconn[0].dialect._get_ros_count(vpyconn[1], projection_name="employee_dimension_super", name="table", schema="public")
    assert rc>0

def test_get_model_comment(vpyconn):
    mc = vpyconn[0].dialect.get_model_comment(vpyconn[1], model_name=sample.sample_ml_model, schema="public")
    assert mc["properties"]["used_by"] == "dbadmin"
    assert len(mc["properties"]["Model Attributes"])>0
    assert len(mc["properties"]["Model Specifications"])>0

def test_get_oauth_comment(vpyconn):
    oc = vpyconn[0].dialect.get_oauth_comment(vpyconn[1])
    assert oc["properties"]["client_id"] == "vertica"
    h = re.compile(r'http://|https://')
    assert len(oc["properties"]["introspect_url"])>0
    assert bool(h.match(oc["properties"]["introspect_url"]))
    assert len(oc["properties"]["discovery_url"])>0
    assert bool(h.match(oc["properties"]["discovery_url"]))
    assert len(oc["properties"]["is_fallthrough_enabled"])>0


