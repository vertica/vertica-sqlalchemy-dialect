python -m venv venv
source /opt/sqlalchemy-vertica-dialect/venv/bin/activate

/usr/bin/python -m pip install -U pytest
pip install sqlalchemy vertica_python
pip install pyodbc 

# Pull the latest Vertica CE image
sudo docker pull vertica/vertica-ce

# Run a Docker container
docker run -d -p 5433:5433 -p 5444:5444 -itd --network=vertica --name vertica_docker vertica/vertica-ce

# Install git
docker exec -it vertica_docker sh -c "sudo yum install git"


# Create a sample projection
cat << EOF >> ddl.sql
\set AUTOCOMMIT on
-- Create a Top-k projection
CREATE TABLE readings (meter_id INT, reading_date TIMESTAMP, reading_value FLOAT);
CREATE PROJECTION readings_topk (meter_id, recent_date, recent_value) AS SELECT meter_id, reading_date, reading_value FROM readings LIMIT 5 OVER (PARTITION BY meter_id ORDER BY reading_date DESC);

-- Create a live agg projs
CREATE TABLE clicks(user_id IDENTITY(1,1), page_id INTEGER, click_time TIMESTAMP NOT NULL);
CREATE PROJECTION clicks_agg AS SELECT page_id, click_time::DATE click_date, COUNT(*) num_clicks FROM clicks GROUP BY page_id, click_time::DATE 
EOF

# Create a Oauth config

cat << EOF >> ddl.sql

-- Create a Oauth config
CREATE AUTHENTICATION v_oauth METHOD 'oauth' HOST '0.0.0.0/0';
ALTER AUTHENTICATION v_oauth SET client_id = 'vertica';
ALTER AUTHENTICATION v_oauth SET client_secret = 'avdhqh1234139uhbicabqwsxiudb12uew1o2nn1i2j';
ALTER AUTHENTICATION v_oauth SET discovery_url = 'https://203.0.113.1:8443/realms/myrealm/.well-known/openid-configuration';
ALTER AUTHENTICATION v_oauth SET introspect_url = 'https://203.0.113.1:8443/realms/myrealm/protocol/openid-connect/token/introspect';
CREATE USER oauth_user;
GRANT AUTHENTICATION v_oauth TO oauth_user;
GRANT ALL ON SCHEMA PUBLIC TO oauth_user;
EOF

# Create a View
cat << EOF >> ddl.sql

-- Create a VIEW
CREATE VIEW sampleview AS SELECT SUM(annual_income), customer_state
FROM public.customer_dimension
WHERE customer_key IN (SELECT customer_key FROM store.store_sales_fact)
GROUP BY customer_state ORDER BY customer_state ASC;
EOF
# Create a UDX
cat << EOF >> ddl.sql

-- Step 1: Create library
\set libfile '\''`pwd`'/python/TransformFunctions.py\''
CREATE LIBRARY TransformFunctions AS '/opt/vertica/sdk/examples/python/TransformFunctions.py' LANGUAGE 'Python';


-- Step 2: Create functions
CREATE TRANSFORM FUNCTION tokenize AS NAME 'StringTokenizerFactory' LIBRARY TransformFunctions;
CREATE TRANSFORM FUNCTION topk AS NAME 'TopKPerPartitionFactory' LIBRARY TransformFunctions;

CREATE TABLE phrases (phrase VARCHAR(128));
COPY phrases FROM STDIN;
Word
The quick brown fox jumped over the lazy dog
\.

SELECT tokenize(phrase) OVER () FROM phrases;
EOF

# Create a temp table
# Create a UDX
cat << EOF >> ddl.sql

-- Create a temp table

CREATE TEMPORARY TABLE sampletemp (a int, b int) ON COMMIT PRESERVE ROWS;
INSERT INTO sampletemp VALUES(1,2);

EOF

# Copy the file to the container
docker cp ddl.sql vertica_docker:/home/dbadmin/

# Run the ddl
docker exec -it vertica_docker sh -c "/opt/vertica/bin/vsql -w 'abc123' -f /home/dbadmin/ddl.sql"

## ML Model build
docker exec -it vertica_docker sh -c "sudo git clone https://github.com/vertica/Machine-Learning-Examples"

## Get perms right
docker exec -it vertica_docker sh -c "sudo chmod a+rwx -R Machine-Learning-Examples"
docker exec -it vertica_docker sh -c "sudo chown dbadmin:verticadba -R Machine-Learning-Examples"


docker exec -it vertica_docker sh -c "/opt/vertica/bin/vsql -f /home/dbadmin/Machine-Learning-Examples/data/load_ml_data.sql"
docker exec -it vertica_docker sh -c "vsql -f /home/dbadmin/Machine-Learning-Examples/naive_bayes/naive_bayes_data_preparation.sql"
docker exec -it vertica_docker sh -c "vsql -f /home/dbadmin/Machine-Learning-Examples/naive_bayes/naivebayes_examples.sql"


