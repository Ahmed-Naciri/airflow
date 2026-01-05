Build ETL Data Pipelines with BashOperator using Apache Airflow
Project Overview

This project demonstrates how to build an ETL (Extract, Transform, Load) data pipeline using Apache Airflow and the BashOperator.
The pipeline automates the extraction of data from multiple file formats, applies transformations using Linux shell commands, and loads the processed data into a staging area.

The workflow is implemented as an Airflow DAG scheduled to run daily.

Objectives

The main objectives of this assignment are to:

Extract data from a CSV file

Extract data from a TSV file

Extract data from a fixed-width text file

Consolidate data from multiple sources

Transform the data according to business rules

Load the transformed data into a staging file

Technologies Used

Apache Airflow

Python

BashOperator

Linux shell utilities (tar, cut, paste, tr)

CSV / TSV / Fixed-width files

DAG Information

DAG ID: ETL_toll_data

Schedule: Daily (@daily)

Operator Used: BashOperator

Owner: Ahmed Naciri

Retry Policy: 1 retry with a 5-minute delay

Failure Notifications: Email alerts enabled

Pipeline Workflow

The ETL pipeline follows the sequence below:

Unzip Data

Extracts data files from tolldata.tgz

Extract CSV Data

Extracts selected columns from vehicle-data.csv

Extract TSV Data

Extracts selected columns from tollplaza-data.tsv

Extract Fixed-Width Data

Extracts specific character ranges from payment-data.txt

Consolidate Data

Merges extracted datasets into a single file

Transform Data

Converts text to uppercase

Reorders and recombines columns

Produces the final transformed dataset

DAG Task Dependency Graph
unzip_data
     ↓
extract_data_from_csv
     ↓
extract_data_from_tsv
     ↓
extract_data_from_fixed_width
     ↓
consolidate_data
     ↓
transform_data

Input Files

The pipeline expects the following files inside the compressed archive:

vehicle-data.csv

tollplaza-data.tsv

payment-data.txt

Output Files

csv_data.csv

tsv_data.csv

fixed_width_data.csv

extracted_data.csv

transformed_data.csv ✅ (final output)

Project Directory Structure
airflow_project/
│
├── dags/
│   └── finalassignment/
│       ├── etl_toll_data.py
│       ├── tolldata.tgz
│       └── transformed_data.csv
│
└── README.md



