import os


#AWS Access And Secret key
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_KEY_ID")
bucket_name = "salesprojectdata"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_partitioned_data_mart/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "sales_project"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": os.getenv("db_password"),
    "driver": "com.mysql.cj.jdbc.Driver"
}
host = "localhost"
user = "root"


# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]

# File Download location
local_directory = "C:\\Users\\ratho\\PycharmProjects\\DataEngineeringProject\\file_from_s3\\"
customer_data_mart_local_file = "C:\\Users\\ratho\\PycharmProjects\\DataEngineeringProject\\customer_data_mart\\"
sales_team_data_mart_local_file = "C:\\Users\\ratho\\PycharmProjects\\DataEngineeringProject\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\ratho\\PycharmProjects\\DataEngineeringProject\\"
error_folder_path_local = "C:\\Users\\ratho\\PycharmProjects\\DataEngineeringProject\\error_files\\"
