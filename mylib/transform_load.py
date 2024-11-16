"""
Transforms and Loads data into Azure Databricks
"""

from pyspark.sql import SparkSession
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



if __name__ == "__main__":
    load()