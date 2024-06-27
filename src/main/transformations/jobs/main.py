import os
from resources.dev import config
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *

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

