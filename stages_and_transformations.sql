--    TASK 1:- Loading CSV Data from S3 into Snowflake Using Stages
-- Set the warehouse to use
USE WAREHOUSE demo;

-- Create the database
CREATE DATABASE sales_stage_db;


-- Create the sales_data_stage table
CREATE OR REPLACE TABLE public.sales_data_stage (
  order_id INTEGER,
  customer_id INTEGER,
  customer_name STRING(100),
  order_date DATE,
  product STRING(100),
  quantity INTEGER,
  price NUMERIC,
  complete_address STRING(255)
);

-- Create a named stage
CREATE OR REPLACE STAGE s3_stage
URL = 's3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv'
CREDENTIALS = ( 
  AWS_KEY_ID = 888888888888888
  AWS_SECRET_KEY = 8888888888888888

)
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 COMPRESSION = 'AUTO');


-- Load data from the stage into Snowflake table
COPY INTO public.sales_data_stage
FROM @s3_stage;


-- Query the table to verify data insertion
SELECT * FROM public.sales_data_stage LIMIT 10;
-- Drop the table
DROP TABLE IF EXISTS public.sales_data_stage;
-- Drop the database
DROP DATABASE IF EXISTS sales_stage_db;


--    TASK 2:- Loading Data from S3 Using a Stage with Data Transformation
-- Set the warehouse to use
USE WAREHOUSE demo;

-- Create the database
CREATE DATABASE sales_transformation_db;

-- Create the sales_data_transformation table
CREATE OR REPLACE TABLE public.sales_data_transformation (
  order_id INTEGER,
  customer_id INTEGER,
  customer_name STRING(100),
  order_date DATE,
  product STRING(100),
  quantity INTEGER,
  price NUMERIC,
  complete_address STRING(255)
);

-- Create a named stage to reference the S3 path
CREATE OR REPLACE STAGE s3_stage
URL = 's3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv'
CREDENTIALS = ( 
  AWS_KEY_ID = 888888888888888888
  AWS_SECRET_KEY = 8888888888888888
)
FILE_FORMAT = ( 
  TYPE = 'CSV', 
  FIELD_DELIMITER = ',', 
  SKIP_HEADER = 1, 
  COMPRESSION = 'AUTO'
);

-- Load data from the stage with transformation (first 5 characters of complete_address)
COPY INTO public.sales_data_transformation
FROM (
  SELECT
    $1 AS order_id,
    $2 AS customer_id,
    $3 AS customer_name,
    $4 AS order_date,
    $5 AS product,
    $6 AS quantity,
    $7 AS price,
    LEFT($8, 5) AS complete_address
  FROM @s3_stage
);


-- Query the table to verify data insertion and transformation
SELECT * FROM public.sales_data_transformation LIMIT 10;

-- Drop the stage
DROP STAGE IF EXISTS s3_stage;

-- Drop the table
DROP TABLE IF EXISTS public.sales_data_transformation;

-- Drop the database
DROP DATABASE IF EXISTS sales_transformation_db;



--    TASK 3:- Load a Subset of Columns from S3 into Snowflake
-- Use the 'demo' warehouse for running queries.
USE WAREHOUSE demo;

-- Create a new database named 'sales_db_subset'.
CREATE OR REPLACE DATABASE sales_db_subset;

-- Create the 'orders_subset' table with columns for 'order_id' and 'customer_name'.
CREATE OR REPLACE TABLE sales_db_subset.public.orders_subset (
    order_id INTEGER,     
customer_name STRING);

-- Create a named stage 's3_stage' that points to the specified S3 path with the necessary credentials.
CREATE OR REPLACE STAGE s3_stage
  URL = 's3://snowflake-hands-on-data/sample_data_basic/sales_sample_data.csv' -- Path to the S3 bucket
  CREDENTIALS = (
    AWS_KEY_ID = 888888888888
    AWS_SECRET_KEY = 88888888888888
    )
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

  -- Load data into the 'orders_subset' table, selecting only the 'order_id' (first column) and 'customer_name' (third column).
COPY INTO sales_db_subset.public.orders_subset (order_id, customer_name) FROM (
  SELECT 
    t.$1 AS order_id,   
    t.$3 AS customer_name
  FROM @s3_stage t
  )
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

-- Verify the data in the 'orders_subset' table to ensure the correct columns are loaded.
SELECT * FROM sales_db_subset.public.orders_subset;

-- Drop the 's3_stage' stage after completing the data load.
DROP STAGE IF EXISTS s3_stage;

-- Drop the 'orders_subset' table once the data has been verified.
DROP TABLE IF EXISTS sales_db_subset.public.orders_subset;

-- Drop the entire 'sales_db_subset' database if no longer needed.
DROP DATABASE IF EXISTS sales_db_subset;

