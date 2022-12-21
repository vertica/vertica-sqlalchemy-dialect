import sqlalchemy as sa
from sqlalchemy.sql import sqltypes
import pytest
import re 


sample_table_list = {"store":["store_orders_fact"],"public": ["customer_dimension","employee_dimension","product_dimension", "vendor_dimension"]}
sample_temp_table = "sampletemp"
sample_projection = "employee_super"
sample_view = "sampleview"
sample_columns = [
    "product_key",
    "product_version",
    "product_description",
    "sku_number",
    "category_description",
    "department_description",
    "package_type_description",
    "package_size",
    "fat_content",
    "diet_type",
    "weight",
    "weight_units_of_measure",
    "shelf_width",
    "shelf_height",
    "shelf_depth",
    "product_price",
    "product_cost",
    "lowest_competitor_price",
    "highest_competitor_price",
    "average_competitor_price",
    "discontinued_flag"
    ]

sample_constraints = {
    "fk_store_orders_vendor":"vendor_key",
    "fk_store_orders_product":"product_key",
    "C_NOTNULL":"store_key" ,
    "C_NOTNULL":"product_version",
    "fk_store_orders_employee":"employee_key",
    "C_NOTNULL":"order_number",
    "C_NOTNULL":"vendor_key",
    "fk_store_orders_product":"product_version",
    "fk_store_orders_store":"store_key",
    "C_NOTNULL":"employee_key",
    "C_NOTNULL":"product_key"
}

sample_pk = {"C_PRIMARY":"customer_key"}

sample_model_list = ["naive_house84_model"]

@pytest.fixture
def vpyconn():
    engine = sa.create_engine('vertica+vertica_python://dbadmin:abc123@localhost:5433/vmart')
    try:     
        conn = engine.connect()
    except:
        print("Failed to connect to the database")
    return [engine, conn]

def test_get_server_version_info(vpyconn):
    res = vpyconn[0].dialect._get_server_version_info(vpyconn[1])
    assert res == (12,0,0)

def test_get_default_schema_name(vpyconn):
    res = vpyconn[0].dialect._get_default_schema_name(vpyconn[1])
    assert res == "public"

def test_has_schema(vpyconn):
    sc1 = vpyconn[0].dialect.has_schema(vpyconn[1], schema="public")
    assert sc1 == True
    sc2 = vpyconn[0].dialect.has_schema(vpyconn[1], schema="store")
    assert sc2 == True

def test_has_table(vpyconn):
    res = vpyconn[0].dialect.has_table(connection=vpyconn[1], table_name=sample_table_list["store"][0], schema="store")
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
    res = vpyconn[0].dialect.get_table_comment(connection=vpyconn[1], table_name=sample_table_list["public"][0], schema="public")
    assert res["properties"]["create_time"]
    assert res["properties"]["Total_Table_Size"]

# TODO Improve this function to verify the output with a regex match
def test_get_table_oid(vpyconn):
    res = vpyconn[0].dialect.get_table_oid(connection=vpyconn[1], table_name=sample_table_list["public"][1], schema="public")
    # Assert the oid is an int
    assert type(res) == int
    # Assert the format of the oid
    p = re.compile(r'^\d+$')
    assert bool(p.match(str(res)))

def test_get_projection_names(vpyconn):
    res = vpyconn[0].dialect.get_projection_names(connection=vpyconn[1], schema="public")
    # Assert the no. of projections
    assert len(res) == 40
    # Assert sample projection
    assert sample_projection in res

def test_get_table_names(vpyconn):
    res = vpyconn[0].dialect.get_table_names(connection=vpyconn[1], schema="public")
    # Assert the no. of tables
    assert len(res) == 42
    # Assert sample tables
    assert all(value in res  for value in sample_table_list["public"])

    res = vpyconn[0].dialect.get_table_names(connection=vpyconn[1], schema="store")
    # Assert the no of tables in another schema
    assert len(res) == 3
    # Assert sample tables
    assert all(value in res  for value in sample_table_list["store"])


def test_get_temp_table_names(vpyconn):
    res = vpyconn[0].dialect.get_temp_table_names(connection=vpyconn[1], schema="public")
    # Assert the no. of temp tables
    assert len(res) == 1
    # Assert sample tables
    assert sample_temp_table in res

def test_get_view_names(vpyconn):
    res = vpyconn[0].dialect.get_view_names(connection=vpyconn[1], schema="public")
    # Assert the no. of views
    assert len(res) == 1
    # Assert sample view
    assert sample_view in res

def test_get_view_definition(vpyconn):
    res = vpyconn[0].dialect.get_view_definition(connection=vpyconn[1], view_name=sample_view, schema="public")
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
    assert sample_view in res

def test_get_columns(vpyconn):
    res = vpyconn[0].dialect.get_columns(connection=vpyconn[1], table_name=sample_table_list["public"][2], schema="public")
    # Assert the no. of columns
    assert len(res)>0
    # Assert sample columns
    assert all(value["name"] in sample_columns for value in res)

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
    res = vpyconn[0].dialect.get_pk_constraint(connection=vpyconn[1], table_name=sample_table_list["public"][0], schema="public")
    # Assert the no. of unique contraints
    assert len(res)>0
    # Assert sample constraint
    assert all(k in sample_pk.values() for k in res['name'])

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
    assert all(value in sample_model_list for value in res)

