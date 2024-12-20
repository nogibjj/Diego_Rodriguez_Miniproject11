# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import requests
import json
import base64

display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

load_dotenv()
server_h = os.getenv("SQL_SERVER_HOST")
access_token = os.getenv("DATABRICKS_API_KEY")
print(server_h)

# COMMAND ----------

headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"
def perform_query(path, headers, data={}):
  session = requests.Session()
  resp = session.request('POST', url + path, data=json.dumps(data), verify=True, headers=headers)
  return resp.json()

def mkdirs(path, headers):
  _data = {}
  _data['path'] = path
  return perform_query('/dbfs/mkdirs', headers=headers, data=_data)

# does its own check to see if the file exists or not 
mkdirs(path="dbfs:/FileStore/nlp", headers=headers)

def create(path, overwrite, headers):
  _data = {}
  _data['path'] = path
  _data['overwrite'] = overwrite
  return perform_query('/dbfs/create', headers=headers, data=_data)

def add_block(handle, data, headers):
  _data = {}
  _data['handle'] = handle
  _data['data'] = data
  return perform_query('/dbfs/add-block', headers=headers, data=_data)

def close(handle, headers):
  _data = {}
  _data['handle'] = handle
  return perform_query('/dbfs/close', headers=headers, data=_data)

def put_file(src_path, dbfs_path, overwrite, headers):
  handle = create(dbfs_path, overwrite, headers=headers)['handle']
  print("Putting file: " + dbfs_path)
  with open(src_path, 'rb') as local_file:
    while True:
      contents = local_file.read(2**20)
      if len(contents) == 0:
        break
      add_block(handle, base64.standard_b64encode(contents).decode(), headers=headers)
    close(handle, headers=headers)

def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, base64.standard_b64encode(content[i:i+2**20]).decode(), headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")

# Usage
create(path="dbfs:/FileStore/nlp/test_times.csv", overwrite= True, headers=headers)

# Usage
serve_times_url = "https://media.githubusercontent.com/media/nickeubank/MIDS_Data/master/World_Development_Indicators/wdi_small_tidy_2015.csv"
serve_times_dbfs_path = "dbfs:/FileStore/nlp/wdi2.csv"
overwrite = True  # Set to False if you don't want to overwrite existing file

put_file_from_url(serve_times_url, serve_times_dbfs_path, overwrite, headers=headers)




# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/nlp/'))


print(os.path.exists('dbfs:/FileStore/nlp'))


# COMMAND ----------

# Initialize SparkSession
spark = SparkSession.builder.appName("Read CSV").getOrCreate()

# Define the file paths
test_times_path = "dbfs:/FileStore/nlp/wdi2.csv"

# Read the CSV files into DataFrames
test_times_df = spark.read.csv(test_times_path, header=True, inferSchema=True)

# Show the DataFrame and count the number of rows
test_times_df.show()
num_rows = test_times_df.count()
print(num_rows)

# Define the table name
table_name = "wdi2_delta"

# Columns to keep and process
df_subset = [
        "Country Name",
        "Adolescent fertility rate (births per 1,000 women ages 15-19)",
        "Antiretroviral therapy coverage for PMTCT (% of pregnant women living with HIV)",
        "Battle-related deaths (number of people)",
        "CPIA building human resources rating (1=low to 6=high)",
        "CPIA business regulatory environment rating (1=low to 6=high)",
        "CPIA debt policy rating (1=low to 6=high)",
    ]
# Select only necessary columns
test_times_df = test_times_df.select(*df_subset)
test_times_df = test_times_df.withColumnRenamed("Country Name", "country") \
       .withColumnRenamed("Adolescent fertility rate (births per 1,000 women ages 15-19)", "fertility_rate") \
       .withColumnRenamed("Antiretroviral therapy coverage for PMTCT (% of pregnant women living with HIV)", "viral") \
       .withColumnRenamed("Battle-related deaths (number of people)", "battle") \
       .withColumnRenamed("CPIA building human resources rating (1=low to 6=high)", "cpia_1") \
       .withColumnRenamed("CPIA business regulatory environment rating (1=low to 6=high)", "cpia_2") \
       .withColumnRenamed("CPIA debt policy rating (1=low to 6=high)", "debt")
       
# Save the DataFrame as a Delta table with schema alignment
# Check if the table exists
if spark._jsparkSession.catalog().tableExists(table_name):
    # Use schema evolution or enforcement when overwriting
    test_times_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
else:
    # Create the Delta table if it doesn't exist
    test_times_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

people_df = spark.read.table("wdi1_delta")
people_df.show()
num_rows = people_df.count()
print(num_rows)
# query once you have both tables 
# merge them
# visuzalize them 
# save them as output 
# figure out how to run databrick 

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/nlp/'))

# COMMAND ----------

"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import requests
from pyspark.sql import SparkSession


FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
headers = {'Authorization': 'Bearer %s' % access_token}


def extract(
    url="""
    https://media.githubusercontent.com/media/nickeubank/MIDS_Data/master/World_Development_Indicators/wdi_small_tidy_2015.csv
    """,
    url2="""
    https://media.githubusercontent.com/media/nickeubank/MIDS_Data/master/World_Development_Indicators/wdi_small_tidy_2015.csv
    """,
    file_path=FILESTORE_PATH+"/wdi1.csv",
    file_path2=FILESTORE_PATH+"/wdi2.csv",
    directory=FILESTORE_PATH,
):
    """Extract a url to a file path"""
    # make the directory, no need to check if it exists or not
    mkdirs(path=directory, headers=headers)
    # add the csv files, no need to check if it exists or not
    put_file_from_url(url, file_path, overwrite, headers=headers)
    put_file_from_url(url2, file_path2, overwrite, headers=headers)

    return file_path, file_path2


# COMMAND ----------

extract()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/mini_project11'))

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

def load_data(spark, data, name):
    """Load data with original headers, handle NaN values, and rename columns for easier handling."""
    df = spark.read.csv(data, header=True, inferSchema=True)

    # Columns to keep and process
    df_subset = [
        "Country Name",
        "Adolescent fertility rate (births per 1,000 women ages 15-19)",
        "Antiretroviral therapy coverage for PMTCT (% of pregnant women living with HIV)",
        "Battle-related deaths (number of people)",
        "CPIA building human resources rating (1=low to 6=high)",
        "CPIA business regulatory environment rating (1=low to 6=high)",
        "CPIA debt policy rating (1=low to 6=high)",
    ]
    # Select only necessary columns
    df = df.select(*df_subset)

    # Rename columns to shorter names for easier handling
    df = (
        df.withColumnRenamed("Country Name", "country") \
       .withColumnRenamed("Adolescent fertility rate (births per 1,000 women ages 15-19)", "fertility_rate") \
       .withColumnRenamed("Antiretroviral therapy coverage for PMTCT (% of pregnant women living with HIV)", "viral") \
       .withColumnRenamed("Battle-related deaths (number of people)", "battle") \
       .withColumnRenamed("CPIA building human resources rating (1=low to 6=high)", "cpia_1") \
       .withColumnRenamed("CPIA business regulatory environment rating (1=low to 6=high)", "cpia_2") \
       .withColumnRenamed("CPIA debt policy rating (1=low to 6=high)", "debt")
    )
    return df

def load(
    dataset="dbfs:/FileStore/nlp/wdi1.csv",
    dataset2="dbfs:/FileStore/nlp/wdi2.csv",
):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()

    # Load and process datasets
    wdi1_df = load_data(spark, dataset, "wdi1")
    wdi2_df = load_data(spark, dataset2, "wdi2")

    # add unique IDs to the DataFrames
    wdi1_df = wdi1_df.withColumn("id", monotonically_increasing_id())
    wdi2_df = wdi2_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it
    wdi2_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("wdi2_delta")
    wdi1_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("wdi1_delta")

    num_rows = wdi2_df.count()
    print(num_rows)
    num_rows = wdi1_df.count()
    print(num_rows)

    return "finished transform and load"


    

# COMMAND ----------

load()

# COMMAND ----------

spark = SparkSession.builder.appName("Query").getOrCreate()

query_result = spark.sql("""
    SELECT 
        * 
    FROM 
        wdi2_delta
    WHERE
        country = 'Chile'
""")
query_result.show()


# COMMAND ----------

def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = (
        "SELECT * FROM wdi1_delta WHERE country = 'Chile'"
    )
    query_result = spark.sql(query)
    return query_result


# COMMAND ----------

query = query_transform()

# COMMAND ----------

!pip install pyspark_dist_explore

# COMMAND ----------

import requests
from dotenv import load_dotenv

load_dotenv()
access_token = os.getenv("DATABRICKS_API_KEY")
job_id = os.getenv("JOB_ID")
server_h = os.getenv("SQL_SERVER_HOST")

url = f'https://{server_h}/api/2.0/jobs/run-now'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}

data = {
    'job_id': job_id
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print('Job run successfully triggered')
else:
    print(f'Error: {response.status_code}, {response.text}')
