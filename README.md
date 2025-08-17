# snowflake_proj2_stages_and_transformations

TASK 1:- 

Loading CSV Data from S3 into Snowflake Using Stages

Use the demo warehouse for executing all SQL commands.
Create a database named sales_stage_db.
Create a table named sales_data_stage with the following columns:
order_id (Integer)
customer_id (Integer)
customer_name (String, 100 characters)
order_date (Date)
product (String, 100 characters)
quantity (Integer)
price (Numeric)
complete_address (String, 255 characters)

Create a stage s3_stage and point it to the S3 bucket containing the CSV file.

S3 Path: s3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv
AWS Access Key: <8888888888888888>
AWS Secret Key: <8888888888888888>

Load the data from the stage into the sales_data_stage table.
Query the sales_data_stage table to verify that all data is loaded correctly.
Drop the sales_data_stage table and sales_stage_db database once you have verified the data.

TASK 2:-

Loading Data from S3 Using a Stage with Data Transformation

Use the demo warehouse for executing all SQL commands.
Create a database named sales_transformation_db.
Create a table named sales_data_transformation with the following columns:
order_id (Integer)
customer_id (Integer)
customer_name (String, 100 characters)
order_date (Date)
product (String, 100 characters)
quantity (Integer)
price (Numeric)
complete_address (String, 255 characters)

Create a named stage s3_stage and point it to the S3 bucket containing the CSV file:

S3 Path: s3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv
AWS Access Key: 8888888888888888
AWS Secret Key: 8888888888888888

Transformation: Load the data from the stage into the sales_data_transformation table, transforming the complete_address column to store only the first 5 characters of the address.
Query the sales_data_transformation table to verify the data has been transformed and loaded correctly.
Clean Up: Drop the created stage, table, and database to ensure no residual objects are left.

TASK 3:-

Load a Subset of Columns from S3 into Snowflake

Ensure you are using the demo warehouse created in earlier assignments.

Create Database and Table: Create a new database named sales_db_subset. Inside this database, create a table called orders_subset with the following structure:
order_id INTEGER
customer_name STRING
Create a Named Stage:
Create a named stage s3_stage pointing to the S3 bucket containing the CSV file.
S3 Path: s3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv
AWS Access Key: 888888888888888
AWS Secret Key: 888888888888888

Load Subset of Columns into Table:
Use the COPY INTO command to load a subset of columns (specifically order_id and customer_name) from the S3 file into the orders_subset table.

COPY INTO sales_db_subset.public.orders_subset (order_id, customer_name)
FROM (
SELECT
t.$1 AS order_id,
t.$3 AS customer_name
FROM @s3_stage t
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);


Verify Data: Query the orders_subset table to ensure only the order_id and customer_name columns have been loaded correctly.

Clean Up: Drop the stage s3_stage, database sales_db_subset, table orders_subset after verifying that everything is correctly loaded and transformed.


