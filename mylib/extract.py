"""
extract a dataset from URL
"""

import requests
import os
import pandas as pd


def extract(
    url="https://media.githubusercontent.com/media/nickeubank/MIDS_Data/master/World_Development_Indicators/wdi_small_tidy_2015.csv",
    file_path="data/wdi1.csv",
    directory="data",
    url2="https://media.githubusercontent.com/media/nickeubank/MIDS_Data/master/World_Development_Indicators/wdi_small_tidy_2015.csv",
    file_path2="data/wdi2.csv",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    with requests.get(url2) as r:
        with open(file_path2, "wb") as f:
            f.write(r.content)
    df = pd.read_csv(file_path2)

    df_subset = df.head(121)

    df_subset.to_csv(file_path2, index=False)
    return file_path, file_path2


if __name__ == "__main__":
    extract()
