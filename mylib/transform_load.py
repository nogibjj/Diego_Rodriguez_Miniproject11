"""
Transforms and Loads data into Azure Databricks
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


def load(
    dataset="dbfs:/FileStore/mini_project11/wdi1.csv",
    dataset2="dbfs:/FileStore/mini_project11/wdi2.csv",
):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema
    wdi1_df = spark.read.csv(dataset, header=True, inferSchema=True)
    wdi2_df = spark.read.csv(dataset2, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    wdi1_df = wdi1_df.withColumn("id", monotonically_increasing_id())
    wdi2_df = wdi2_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it
    wdi2_df.write.format("delta").mode("overwrite").saveAsTable("wdi2_delta")
    wdi1_df.write.format("delta").mode("overwrite").saveAsTable("wdi1_delta")

    num_rows = wdi2_df.count()
    print(num_rows)
    num_rows = wdi1_df.count()
    print(num_rows)

    return "finished transform and load"


if __name__ == "__main__":
    load()
