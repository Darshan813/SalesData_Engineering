import os
import sys
from resources.dev import config
from src.main.download.aws_file_download import *
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import *


access_key = config.aws_access_key
secret_key = config.aws_secret_key

# Creating an object s3_client_provider from file s3_client_object.py
s3_client_provider = S3ClientProvider(access_key, secret_key)
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

#logger.info(response["Buckets"])

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]

connection = get_mysql_connection()
cursor = connection.cursor()

# If this is false, it means that the process was successful:
if csv_files:
    statement = f'''SELECT distinct file_name FROM {config.database_name}.{config.product_staging_table} 
                WHERE file_name IN ({str(csv_files)[1:-1]}) and status = "I"'''
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Last run Failed")
    else:
        logger.info("No record match")
else:
    logger.info("Last run Success")

# To get the files_name from s3 folder
try:
    s3_reader = S3Reader()
    foder_path = "sales_data/"
    bucket_name = config.bucket_name
    s3_file_path = s3_reader.list_files(s3_client, bucket_name, foder_path) #return a list
    logger.info("Absolute file path: %s", s3_file_path)
    if not s3_file_path:
        logger.info(f"No file found at {foder_path}")
        raise Exception("No Data available to process")
except Exception as e:
    logger.error("Exited with error - %s", e)

prefix = f"s3://{bucket_name}/"
local_directory = config.local_directory

file_paths = [url[len(prefix):] for url in s3_file_path]
print(file_paths)

# To download the files from s3
try:
    s3_downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    s3_downloader.download_files(file_paths)

except Exception as e:
    logger.error("File download error - %s", e)
    sys.exit()

# to check all the files in the local_dir, if they are csv or not
all_files = os.listdir(local_directory)
logger.info("Total files: %s", all_files)

if all_files:
    csv_files = []
    error_files = [] #not csv
    for files in all_files:
        if files.endswith("csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files))) #will go in the error_dir

    if not csv_files: #if all the files are other than csv files, like json
        logger.error("No CSV files")
        raise Exception("No CSV files")
else:
    logger.error("No data to process")
    raise Exception("No data to process")

logger.info(f"CSV files: {csv_files}")

#Schema Validation

logger.info("Checking Schema for transformations")

#if a csv file, doesn't have the proper schema, then the csv file will go in the error_files
correct_files = []
spark = spark_session()
for data in csv_files:
    data_schema = spark.read.format("csv").option("header", "true").load(data).columns
    logger.info(f"Schema of the data: {data_schema}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns: {missing_columns}")
    if missing_columns:
        error_files.append(data)
    else:
        logger.info("No missing columns for the data")
        correct_files.append(data)

