# AWS Glue ETL Script: RestAPI data download and transform

This project demonstrates an AWS Glue ETL script that:
- Reads data from an rest api link (https://www.anapioficeandfire.com/api/characters)
- The url is part of an API of Ice and Fire, which is an open REST API providing data from the Game of Thrones / A Song of Ice and Fire universe.
- Downloading data in json format into a pandas list with selected columns
- Loops through a set of 50 rows per page one-by-one and appends the rows into a separate list
- Final list created after appending all the rows is then converted into pandas dataframe
- Pandas dataframe is further converted into spark dataframe
- Spark dataframe is written to an S3 bucket in parquet format
- Files generated in parquet are merged to create a single csv file to read through the data

## Technologies
- AWS Glue (PySpark)
- AWS S3
- Python (Boto3)

## Input Example
Rest api link to the request object

## Output Example
A sample csv file is created with selected columns after downloading data from rest api.

## How to Run
This script is designed to run inside AWS Glue, but can be adapted to run locally with a Spark environment.

## Author
Shailesh Mistry – [LinkedIn](https://www.linkedin.com/in/shailesh-mistry-a346659)
