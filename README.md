# snowflake_proj2_stages_and_transformations
This project demonstrates the use of Snowflake stages to load data from Amazon S3 and apply basic transformations during the load process. The exercises cover:
Loading full datasets from S3 into Snowflake
Applying simple transformations while loading
Loading subsets of columns
Managing database objects efficiently

The goal is to learn how Snowflake stages simplify data ingestion and support transformations without modifying the source files.

Prerequisites:-
Active Snowflake account
Access to Snowflake Web UI or SnowSQL
AWS credentials with permission to access the specified S3 bucket
Demo warehouse created in Project 1

Tasks Performed:-
Task 1: Loading CSV Data from S3 Using Stages
Created a database and table for sales data
Created a named stage pointing to the S3 bucket containing the CSV file
Loaded the full CSV data into the table using the stage
Verified that all rows were ingested successfully
Dropped the database and table after verification to maintain a clean environment

Task 2: Loading Data with Transformations
Created a database and table for transformed sales data
Created a named stage pointing to the S3 bucket
Applied a transformation while loading: stored only the first 5 characters of the complete_address column
Verified that data was loaded and transformed correctly
Cleaned up by dropping the database, table, and stage

Task 3: Loading a Subset of Columns from S3
Created a database and table with a subset of columns (order_id and customer_name)
Created a named stage pointing to the S3 bucket
Loaded only the required columns into the table
Verified that the subset of data was loaded correctly
Cleaned up by dropping the database, table, and stage

Real-World Relevance:
Stages simplify the ingestion of large datasets from cloud storage into Snowflake without moving files manually
Transformations during load reduce preprocessing steps and help maintain clean, standardized data
Selective column loading optimizes storage and query performance by ingesting only the necessary data



