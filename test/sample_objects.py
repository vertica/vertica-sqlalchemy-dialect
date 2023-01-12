sample_table_list = {"store":["store_orders_fact"],"public": ["customer_dimension","employee_dimension","product_dimension", "vendor_dimension"]}
sample_temp_table = "sampletemp"
sample_projections = ["employee_super", "store_orders_fact_super", "ytd_orders"]
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

sample_tags = {"sampletemp": "dbadmin", "employee_dimension": "dbadmin", "clicks": "dbadmin"}

sample_ml_model = "naive_house84_model"

sample_oauth_name = "v_oauth"