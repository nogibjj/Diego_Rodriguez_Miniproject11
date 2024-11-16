"""Query the database from a db connection to Azure Databricks"""

import os
from databricks import sql
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Define a global variable for the log file
LOG_FILE = "query_log.md"


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


if __name__ == "__main__":
    query_transform()
