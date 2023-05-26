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

sample_view_columns = [
    'SUM',
    'customer_state'
    
]


sample_projection_columns = [
    "date_key",
    "product_key",
    "product_version",
    "warehouse_key",
    "qty_in_stock",
    "inventory_date"
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
sample_projection_columns = {'ROS_Count': 264, 'projection_name': 'store_orders_fact_super', 'Projection_Type': 'is_super_projection', 'is_segmented': 'True', 'Segmentation_key': 'hash(store_orders_fact.product_key, store_orders_fact.product_version, store_orders_fact.store_key, store_orders_fact.vendor_key, store_orders_fact.employee_key, store_orders_fact.order_number, store_orders_fact.date_ordered, store_orders_fact.date_shipped)', 'Partition_Key': '2014-06-04', 'projection_size': '7553 KB'}