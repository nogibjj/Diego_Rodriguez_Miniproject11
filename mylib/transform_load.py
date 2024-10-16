"""
Transforms and Loads data into Azure Databricks
"""

import os
from databricks import sql
import pandas as pd
import numpy as np
from dotenv import load_dotenv


# load the csv file and insert into databricks
def load(dataset="data/wdi1.csv", dataset2="data/wdi2.csv"):
    """Transforms and Loads data into the local databricks database"""
    df1 = pd.read_csv(dataset, delimiter=",", skiprows=1, usecols=range(7))
    df2 = pd.read_csv(dataset2, delimiter=",", skiprows=1, usecols=range(7))
    df1 = df1.replace({np.nan: 0})
    df2 = df2.replace({np.nan: 0})
    # Open and read the CSV file
    load_dotenv()
    server_h = os.getenv("sql_server_host")
    access_token = os.getenv("databricks_api_key")
    http_path = os.getenv("sql_http")
    print(server_h, access_token, http_path)
    with sql.connect(
        server_hostname=server_h, http_path=http_path, access_token=access_token
    ) as connection:
        c = connection.cursor()
        # INSERT TAKES TOO LONG
        # c.execute("DROP TABLE IF EXISTS der41_wdi2")
        c.execute("SHOW TABLES FROM default LIKE 'der41_wdi1'")
        result = c.fetchall()
        print(result)
        # takes too long so not dropping anymore
        # c.execute("DROP TABLE IF EXISTS der41_wdi1")
        if not result:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS der41_wdi1 (
                    id int,
                    country string,
                    fertility_rate int,
                    viral int,
                    battle int,
                    cpia_1 int,
                    cpia_2 int,
                    debt int
                    )
                """
            )
            # Insert the filtered data into the table
            for _, row in df1.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO der41_wdi1 VALUES {convert}")
        c.execute("SHOW TABLES FROM default LIKE 'der41_wdi2'")
        result = c.fetchall()
        print(result)
        # c.execute("DROP TABLE IF EXISTS EventTimesDB")
        if not result:
            c.execute(
                """
                    CREATE TABLE IF NOT EXISTS der41_wdi2 (
                        id int,
                        country string,
                        fertility_rate int,
                        viral int,
                        battle int,
                        cpia_1 int,
                        cpia_2 int,
                        debt int
                    )
                """
            )
            # Insert the filtered data into the table
            for _, row in df2.iterrows():
                convert = (_,) + tuple(row)
                c.execute(f"INSERT INTO der41_wdi2 VALUES {convert}")
        c.close()
    return "success"
