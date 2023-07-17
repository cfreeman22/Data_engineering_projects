README

## Introduction
This project focuses on assisting Sparkify, a music streaming startup, in migrating their users and song database processes to the cloud. The objective is to build an ETL pipeline that extracts data from Udacity AWS S3 (data storage), stages tables on AWS Redshift (a columnar storage data warehouse), and executes SQL statements to create analytics tables from the staging tables.

## Datasets
The project utilizes datasets available in two public S3 buckets. One bucket contains information about songs and artists, while the other bucket consists of user action data, such as song plays. Both buckets contain JSON files.

## TOOLS

<p align="left"> <a href="https://aws.amazon.com" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/amazonwebservices/amazonwebservices-original-wordmark.svg" alt="aws" width="40" height="40"/> </a> <a href="https://www.postgresql.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> </p>



## Data Warehouse Configurations and Setup
To set up the data warehouse, follow these steps:

1. Create a new IAM user in your AWS account.
2. Assign the IAM user the "AdministratorAccess" permission and attach relevant policies.
3. Use the access and secret keys of the IAM user to create clients for EC2, S3, IAM, and Redshift.
4. Create an IAM Role that grants Redshift read-only access to the S3 bucket.
5. Create a RedShift Cluster and record the DWH_ENDPOINT (host address) and DWH_ROLE_ARN. Update the config file with these details.

## ETL Pipeline
The ETL pipeline involves the following steps:

1. Creation of tables to store data from the S3 buckets.
2. Loading of data from the S3 buckets into staging tables within the Redshift Cluster.
3. Insertion of data into fact and dimension tables from the staging tables.

## Project Structure
- `create_cluster.py`: This script creates a Redshift cluster and provides the endpoint and IAM Role ARN.
- `create_tables.py`: This script drops existing tables (if present) and recreates them.
- `etl.py`: This script executes queries to extract JSON data from the S3 bucket and load it into Redshift.
- `sql_queries.py`: This file contains SQL statements stored as variables, categorized by CREATE, DROP, COPY, and INSERT statements.
- `dwh.cfg`: Configuration file that includes Redshift, IAM, and S3 information.
- `README.md`: Detailed information about the project and the ETL pipeline.
- `test_queries.py`: Includes sample queries that can be run on the database or used as a reference.

## How to Run
To execute the project, follow these steps:

1. Run `create_cluster.py` to create the Redshift Cluster.
2. Verify the accessibility of the database.
3. Execute `create_tables.py` to create the necessary tables.
4. Run `etl.py` to execute the ETL process.
5. Create custom queries to run on the database or use the queries provided in `test_queries.py`.