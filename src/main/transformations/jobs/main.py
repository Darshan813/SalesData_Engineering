import os
import shutil
import sys
import datetime

from resources.dev import config
from src.main.download.aws_file_download import *
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import *
from src.main.transformations.jobs.dimension_tables_join import *
from src.main.write.parquet_writer import ParquetWriter

access_key = config.aws_access_key
secret_key = config.aws_secret_key

# Creating an object s3_client_provider from file s3_client_object.py
s3_client_provider = S3ClientProvider(access_key, secret_key)
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

# logger.info(response["Buckets"])

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]

connection = get_mysql_connection()
cursor = connection.cursor()

# If this is false, it means that the process was successful:
if csv_files:
    statement = f'''SELECT distinct file_name FROM {config.database_name}.{config.product_staging_table} 
                WHERE file_name IN ({str(csv_files)[1:-1]}) and status = "A"'''
    cursor.execute(statement)
    data = cursor.fetchall()
    print("Printing Data...........")
    print(data)  # [('file_name.csv',), ('file1_name.csv',)]
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
    s3_file_path = s3_reader.list_files(s3_client, bucket_name, foder_path)  # return a list
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
    error_files = []  # not csv
    for files in all_files:
        if files.endswith("csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))  # will go in the error_dir

    if not csv_files:  # if all the files are other than csv files, like json
        logger.error("No CSV files")
        raise Exception("No CSV files")
else:
    logger.error("No data to process")
    raise Exception("No data to process")

logger.info(f"CSV files: {csv_files}")

# Schema Validation

logger.info("Checking Schema for transformations")

# if a csv file, doesn't have the proper schema (if they have less columns then the original schema),
# then the csv file will go in the error_files

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

error_files_local_dir = config.error_folder_path_local
print(error_files)

# To move error files to error_files_dir (local and s3)
if error_files:
    for file_path in error_files:
        # Moved to local error_file_dir
        file_name = os.path.basename(file_path)
        destination_path = os.path.join(error_files_local_dir, file_name)
        shutil.move(file_path, destination_path)
        logger.info(f"Moved {file_name} from {file_path} to {destination_path}")

        # Moved to s3_file_dir
        source_prefix = "sales_data/"
        destination_prefix = config.s3_error_directory

        message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
        logger.info(f" File move to s3 error dir: {message}")


else:
    logger.info("Error Files does not exists")

# Adding the files into the product_staging table or updating the product_staging_table, Adding files which also
# have extra columns, that can be handled later.

logger.info("******Updating the product staging table*****")

insert_statements = []
db_name = config.database_name
table_name = config.product_staging_table
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        print(file_name)
        print(file)
        statements = f'''
            INSERT INTO {db_name}.{config.product_staging_table} 
            (`file_name`, `file_location`, `created_date`, `status`) VALUES 
            ('{file_name}', '{file}', '{formatted_date}', 'A') 
        '''
        insert_statements.append(statements)
    logger.info(f"Insert statement created for staging table -- {insert_statements}")
    logger.info("***** Connecting with MySQL Server *****")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("There is no file to process")
    raise Exception("***** No Data available with correct files *****")

logger.info("***** Staging Table Updated Successfully *****")

logging.info("***** Fixing Extra Column Coming from source ******")

database_client = DatabaseReader(config.url, config.properties)
final_df = database_client.create_dataframe(spark, "empty_df_create_table")

# To check the correct files, if they have extra columns..., if they have extra columns, adding the columns in the
# new additional columns in comma separated value type.

for data in correct_files:
    data_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns presents are {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional column", concat_ws(", ", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional column")
        logger.info(f"Processed {data} and added additonal column")
    else:
        data_df = data_df.withColumn("additional column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost", "additional column")

    final_df = final_df.union(data_df)

database_client = DatabaseReader(config.url, config.properties)

customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
product_table_df = database_client.create_dataframe(spark, config.product_table)
sales_team_df = database_client.create_dataframe(spark, config.sales_team_table)
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimensions_table_join(final_df, customer_table_df, store_table_df, sales_team_df)
s3_customer_store_sales_df_join.show()

# Customer data mart
logger.info("***** Creating Customer Data Mart *****")
customer_data_mart_df = s3_customer_store_sales_df_join.select("ct.customer_id", "ct.first_name", "ct.last_name",
                                                               "ct.address", "ct.pincode", "phone_number", "sales_date",
                                                               "total_cost")

customer_data_mart_df.show()

parquetWriter = ParquetWriter("overwrite", "parquet")
# This will write the parquet file to local
logger.info("****** Uploading the customer Parquet File to local *****")
parquetWriter.dataframe_writer(customer_data_mart_df, config.customer_data_mart_local_file)

logger.info("***** Successfully uploaded the customer Parquet File to Local ******")
# This will upload the parquet file from local to s3
logger.info("***** Uploading the Parquet File from Local to S3 *****")
s3_uploader = UploadToS3(s3_client)
message = s3_uploader.upload_to_s3(config.s3_customer_datamart_directory, config.bucket_name,
                                   config.customer_data_mart_local_file)
logger.info(f"{message}")

# Sales data mart

sales_data_mart_df = s3_customer_store_sales_df_join.select("store_id", "sales_person_id",
                                                            "sales_person_first_name", "sales_person_last_name",
                                                            "store_manager_name", "manager_id", "is_manager",
                                                            "sales_person_address", "sales_person_pincode",
                                                            "sales_date", "total_cost",
                                                            expr("SUBSTRING(sales_date,1, 7) as sales_month"))

sales_data_mart_df.show()

parquetWriter = ParquetWriter("overwrite", "parquet")
parquetWriter.dataframe_writer(sales_data_mart_df, config.sales_team_data_mart_local_file)

s3_uploader = UploadToS3(s3_client)
message = s3_uploader.upload_to_s3(config.s3_sales_datamart_directory, config.bucket_name,
                                   config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# Writing data to partitons

logger.info("***** Uploading sales partition data mart to local ***** ")
sales_data_mart_df.write.format("parquet").option("header", "true").mode("overwrite")\
    .partitionBy("sales_month", "store_id").option("path", config.sales_team_data_mart_partitioned_local_file).save()

# Moving data to s3
logger.info("***** Uploading sales partition data mart to s3 ***** ")

s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp())

for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)
